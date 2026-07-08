package lake

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
)

// newMemClient wires a Client against local Redis + in-memory storage.
func newMemClient(t *testing.T, opts ...func(*option)) (*Client, *mem.Store, context.Context) {
	t.Helper()
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, _, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve, opts...)
	t.Cleanup(func() { _ = c.Close() })
	return c, store, context.Background()
}

func writeDelta(t *testing.T, c *Client, store *mem.Store, catalog, path string, mt MergeType, body string) *WriteHandle {
	t.Helper()
	ctx := context.Background()
	h, err := c.WriteBegin(ctx, WriteBeginRequest{
		Catalog: catalog, Path: path, MergeType: mt, Provider: "mem", Bucket: "data",
	})
	if err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
	if err := store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(body)); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := c.WriteNotify(ctx, h); err != nil {
		t.Fatalf("WriteNotify: %v", err)
	}
	return h
}

// TestNotifyMonotonicAcrossClockRegression_Redis pins the allocator floor: a
// Redis clock that steps BACKWARDS (failover to a lagging replica, NTP step)
// must not mint a tsSeq at-or-below anything already issued — duplicates
// would break RemoveDelta targeting and merge order, and a score below the
// snap stop would make an acknowledged write invisible to every read and
// then permanently deleted by Compact.
func TestNotifyMonotonicAcrossClockRegression_Redis(t *testing.T) {
	c, store, ctx := newMemClient(t)

	// Simulate "the clock used to be 1h ahead": plant an allocator value in
	// the future, as if issued before a 1-hour backwards step.
	future := c.reader.NowUnix() + 3600
	if err := c.rdb.Set(ctx, c.reader.Prefix()+":seq:users",
		index.TimeSeqID{Timestamp: future, SeqID: 41}.String(), 0).Err(); err != nil {
		t.Fatal(err)
	}

	writeDelta(t, c, store, "users", "/", MergeTypeReplace, `{"n":1}`)
	list := c.List(ctx, "users")
	if list.Err != nil || len(list.Entries) != 1 {
		t.Fatalf("List: err=%v entries=%d", list.Err, len(list.Entries))
	}
	got := list.Entries[0].TsSeq
	if got.Timestamp != future || got.SeqID != 42 {
		t.Fatalf("tsSeq = %s, want %d_42 (continuation after the planted floor)", got, future)
	}

	// Snap-stop floor: even with the allocator key gone (expired), a write
	// must sort strictly after the snapshot bound or it is unreadable.
	stop := index.TimeSeqID{Timestamp: c.reader.NowUnix() + 7200, SeqID: 7}
	if err := c.writer.AddSnap(ctx, "users2", stop, "mem://snaps/x", "0"); err != nil {
		t.Fatal(err)
	}
	writeDelta(t, c, store, "users2", "/", MergeTypeReplace, `{"n":2}`)
	list2 := c.List(ctx, "users2")
	if list2.Err != nil {
		t.Fatal(list2.Err)
	}
	if len(list2.Entries) != 1 {
		t.Fatalf("the write is invisible: %d entries past the snap stop", len(list2.Entries))
	}
	if s := list2.Entries[0].TsSeq; !(s.Timestamp > stop.Timestamp || (s.Timestamp == stop.Timestamp && s.SeqID > stop.SeqID)) {
		t.Fatalf("tsSeq %s does not sort after snap stop %s", s, stop)
	}
}

// TestSampleEmptyCatalogCachesOnce: the sample of a still-empty catalog is a
// legitimate cacheable value — the loader must run once, not on every call.
func TestSampleEmptyCatalogCachesOnce_Redis(t *testing.T) {
	c, _, ctx := newMemClient(t)

	var runs atomic.Int32
	sampler := NewSampler[string]("empty-ind", func(lr *ListResult) (string, error) {
		runs.Add(1)
		return "empty-result", nil
	})

	for i := 0; i < 3; i++ {
		list := c.List(ctx, "no-such-catalog")
		if list.Err != nil {
			t.Fatal(list.Err)
		}
		v, err := sampler.Sample(ctx, list)
		if err != nil || v != "empty-result" {
			t.Fatalf("Sample #%d: v=%q err=%v", i, v, err)
		}
	}
	if n := runs.Load(); n != 1 {
		t.Fatalf("loader ran %d times for an unchanged empty catalog, want 1", n)
	}
}

// TestClientCloseStopsTickerAndIsIdempotent is a smoke test for the new
// lifecycle hook: Close twice, then keep using the client.
func TestClientClose_Redis(t *testing.T) {
	c, store, ctx := newMemClient(t)
	writeDelta(t, c, store, "users", "/", MergeTypeReplace, `{"a":1}`)

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	// A closed client still serves reads (clock falls back).
	list := c.List(ctx, "users")
	if list.Err != nil {
		t.Fatal(list.Err)
	}
	if got, err := ReadString(ctx, list); err != nil || got != `{"a":1}` {
		t.Fatalf("read after Close: %q err=%v", got, err)
	}
}

// TestReadPrunesDeadDeltaFetches: bodies of entries a later root Replace
// overwrites must not be fetched at all — and a poison body among them must
// not wedge the read.
func TestReadPrunesDeadDeltaFetches_Redis(t *testing.T) {
	c, store, ctx := newMemClient(t)

	// A poison body (invalid JSON) followed by a root Replace that covers it.
	writeDelta(t, c, store, "users", "/profile", MergeTypeReplace, `{invalid-json`)
	writeDelta(t, c, store, "users", "/", MergeTypeReplace, `{"clean":true}`)

	list := c.List(ctx, "users")
	if list.Err != nil {
		t.Fatal(list.Err)
	}
	got, err := ReadString(ctx, list)
	if err != nil {
		t.Fatalf("read wedged by a dead poison delta: %v", err)
	}
	if got != `{"clean":true}` {
		t.Fatalf("read = %q, want {\"clean\":true}", got)
	}
}
