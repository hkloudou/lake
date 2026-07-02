package lake

import (
	"context"
	"strings"
	"testing"

	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
)

func TestCompact_ValidatesCatalogBeforeRedis(t *testing.T) {
	c := newDeadClient(t)
	if _, err := c.Compact(context.Background(), "bad|name"); err == nil || !strings.Contains(err.Error(), "invalid catalog") {
		t.Fatalf("expected invalid catalog error, got %v", err)
	}
}

// TestCompactRoundTrip_Redis is the end-to-end contract: compacting after a
// snapshot changes NO read result — it only empties the absorbed part of the
// delta index — and subsequent writes/reads keep working on top of the
// compacted catalog.
func TestCompactRoundTrip_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, provider, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve, WithSnapTarget("mem", "snaps"))

	ctx := context.Background()
	write := func(path string, mt MergeType, body string) {
		t.Helper()
		h, err := c.WriteBegin(ctx, WriteBeginRequest{
			Catalog: "users", Path: path, MergeType: mt, Provider: "mem", Bucket: "data",
		})
		if err != nil {
			t.Fatalf("WriteBegin(%s): %v", path, err)
		}
		if err := store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(body)); err != nil {
			t.Fatalf("upload: %v", err)
		}
		if err := c.WriteNotify(ctx, h); err != nil {
			t.Fatalf("WriteNotify(%s): %v", path, err)
		}
	}

	write("/", MergeTypeReplace, `{"name":"Alice"}`)
	write("/profile", MergeTypeRFC7396, `{"city":"NYC"}`)

	doc1, err := ReadString(ctx, c.List(ctx, "users"))
	if err != nil {
		t.Fatalf("first ReadString: %v", err)
	}
	// The read fired an async snapshot save; compaction has nothing to trim
	// until the snap pointer lands.
	if !waitFor(func() bool { s, _ := c.reader.GetLatestSnap(ctx, "users"); return s != nil }) {
		t.Fatal("snapshot was not indexed within timeout")
	}

	n, err := c.Compact(ctx, "users")
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if n != 2 {
		t.Fatalf("Compact removed %d entries, want 2", n)
	}

	// Snapshot-only read: identical document, zero deltas replayed.
	list := c.List(ctx, "users")
	if list.Err != nil {
		t.Fatalf("List after compact: %v", list.Err)
	}
	if list.LatestSnap == nil || len(list.Entries) != 0 {
		t.Fatalf("after compact: snap=%v entries=%d, want snap + 0 entries", list.LatestSnap, len(list.Entries))
	}
	doc2, err := ReadString(ctx, list)
	if err != nil {
		t.Fatalf("ReadString after compact: %v", err)
	}
	if doc2 != doc1 {
		t.Fatalf("compaction changed the read result:\n before: %s\n after:  %s", doc1, doc2)
	}

	// A write after compaction lands past the snap stop, so a compact run
	// before the next snapshot must not touch it.
	write("/profile", MergeTypeRFC7396, `{"age":31}`)
	list = c.List(ctx, "users")
	if list.Err != nil || len(list.Entries) != 1 {
		t.Fatalf("List after new write: err=%v entries=%d, want 1", list.Err, len(list.Entries))
	}
	if n, err = c.Compact(ctx, "users"); err != nil || n != 0 {
		t.Fatalf("compact before new snap: removed=%d err=%v, want 0/nil", n, err)
	}
	doc3, err := ReadString(ctx, c.List(ctx, "users"))
	if err != nil {
		t.Fatalf("ReadString after new write: %v", err)
	}
	for _, want := range []string{`"name":"Alice"`, `"city":"NYC"`, `"age":31`} {
		if !strings.Contains(doc3, want) {
			t.Fatalf("merged doc missing %s: %s", want, doc3)
		}
	}
	// Drain the async snapshot save triggered by the last read so its Redis
	// write lands before cleanup.
	stop3 := list.Entries[0].TsSeq
	if !waitFor(func() bool {
		s, _ := c.reader.GetLatestSnap(ctx, "users")
		return s != nil && s.StopTsSeq == stop3
	}) {
		t.Fatal("post-write snapshot was not indexed within timeout")
	}
}
