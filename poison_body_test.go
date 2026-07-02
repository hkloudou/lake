package lake

import (
	"context"
	"strings"
	"testing"

	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
)

// TestPoisonBodyFailsLoudly_Redis pins the contract for a body Lake never
// inspects: a client uploads garbage to its presigned location, Notify
// records the delta, and the read must FAIL with the offending delta
// identified — not silently splice the garbage into the merged document and
// then persist that corruption as the catalog's snapshot (which would poison
// every later read).
func TestPoisonBodyFailsLoudly_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, provider, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve, WithSnapTarget("mem", "snaps"))

	ctx := context.Background()
	h, err := c.WriteBegin(ctx, WriteBeginRequest{
		Catalog: "users", Path: "/profile", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
	})
	if err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
	// The "upload": invalid JSON, exactly what a buggy or malicious client
	// can put at the presigned URL.
	if err := store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(`{invalid`)); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := c.WriteNotify(ctx, h); err != nil {
		t.Fatalf("WriteNotify: %v", err)
	}

	list := c.List(ctx, "users")
	if list.Err != nil {
		t.Fatalf("List: %v", list.Err)
	}
	_, err = ReadString(ctx, list)
	if err == nil {
		t.Fatal("read of a poison body must fail, not return a corrupt document")
	}
	// The error must carry what an operator needs to locate the delta.
	for _, want := range []string{list.Entries[0].TsSeq.String(), h.URI} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should identify the offending delta (%s): %v", want, err)
		}
	}

	// The failed read must not have snapshotted anything.
	if snap, _ := c.reader.GetLatestSnap(ctx, "users"); snap != nil {
		t.Fatalf("a failed read must not persist a snapshot, got %+v", snap)
	}

	// Recovery: RemoveDelta with the tsSeq the merge error names clears the
	// poison entry, and the catalog reads again.
	removed, err := c.RemoveDelta(ctx, "users", list.Entries[0].TsSeq.String())
	if err != nil {
		t.Fatalf("RemoveDelta: %v", err)
	}
	if !removed {
		t.Fatal("RemoveDelta reported nothing removed")
	}
	got, err := ReadString(ctx, c.List(ctx, "users"))
	if err != nil {
		t.Fatalf("read after RemoveDelta: %v", err)
	}
	if got != "{}" {
		t.Fatalf("read after RemoveDelta = %q, want empty document", got)
	}
}
