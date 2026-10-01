package lake

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/hkloudou/lake/v3/internal/objkey"
)

// The removal generation is the one barrier between RemoveDelta and derived
// state: every snapshot save and every memo entry carries the generation of
// the ListResult it was computed from, and is refused (AddSnap) or ignored
// (isStale) once the catalog's generation has moved. These tests pin that
// contract end to end against Redis.

// TestSampleStaleListNotServed_Redis: a sample computed from a pre-removal
// ListResult is returned to that caller but never served to a post-removal
// one.
func TestSampleStaleListNotServed_Redis(t *testing.T) {
	c, store, ctx := newMemClient(t)
	writeDelta(t, c, store, "users", "/", MergeTypeReplace, `{"a":1}`)
	writeDelta(t, c, store, "users", "/", MergeTypeReplace, `{"b":2}`)

	staleList := c.List(ctx, "users")
	if staleList.Err != nil || len(staleList.Entries) != 2 {
		t.Fatalf("List: err=%v entries=%d", staleList.Err, len(staleList.Entries))
	}
	if removed, err := c.RemoveDelta(ctx, "users", staleList.Entries[0].TsSeq.String()); err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v", removed, err)
	}
	if staleList.RemoveGen() != "0" || c.List(ctx, "users").RemoveGen() != "1" {
		t.Fatalf("RemoveGen() must move from \"0\" to \"1\" across the removal")
	}

	var runs atomic.Int64
	sampler := NewSampler[int]("views", func(*ListResult) (int, error) { return int(runs.Add(1)), nil })
	if v, err := sampler.Sample(ctx, staleList); err != nil || v != 1 {
		t.Fatalf("stale-list Sample: v=%d err=%v, want 1/nil", v, err)
	}
	// A current list recomputes instead of serving the stale generation.
	if v, err := sampler.Sample(ctx, c.List(ctx, "users")); err != nil || v != 2 {
		t.Fatalf("fresh-list Sample: v=%d err=%v, want 2/nil (recompute)", v, err)
	}
	if v, err := sampler.Sample(ctx, c.List(ctx, "users")); err != nil || v != 2 {
		t.Fatalf("second fresh Sample: v=%d err=%v, want cached 2/nil", v, err)
	}
}

// TestSampleUnsweptStaleEntryRejected_Redis: a planted pre-removal entry
// whose score beats the version floor is still rejected on generation.
func TestSampleUnsweptStaleEntryRejected_Redis(t *testing.T) {
	c, store, ctx := newMemClient(t)
	writeDelta(t, c, store, "users", "/", MergeTypeReplace, `{"n":1}`)
	pre := c.List(ctx, "users")
	if removed, err := c.RemoveDelta(ctx, "users", pre.Entries[0].TsSeq.String()); err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v", removed, err)
	}
	stale, _ := marshalSampleCache(SampleMeta{Score: pre.LastUpdated() + 100, UpdatedAt: 1, RemoveGen: "0"}, 111)
	if err := c.sampleRdb.HSet(ctx, c.idx.SampleKey("views"), "users", stale).Err(); err != nil {
		t.Fatalf("plant stale entry: %v", err)
	}
	var runs atomic.Int64
	sampler := NewSampler[int]("views", func(*ListResult) (int, error) { runs.Add(1); return 222, nil })
	if v, err := sampler.Sample(ctx, c.List(ctx, "users")); err != nil || v != 222 || runs.Load() != 1 {
		t.Fatalf("Sample: v=%d err=%v runs=%d, want recomputed 222", v, err, runs.Load())
	}
}

// TestDeleteCatalogSweepsGlobPrefix_Redis: a prefix with MATCH metacharacters
// must still have its memo hashes swept (unescaped, "p[g]…" would match
// "pg…" instead).
func TestDeleteCatalogSweepsGlobPrefix_Redis(t *testing.T) {
	c, store, ctx := newMemClient(t)
	prefix := c.idx.Prefix() + "[g]*?"
	t.Cleanup(func() {
		c.rdb.Del(context.Background(), prefix+":d:users", prefix+":s", prefix+":m:views", prefix+":seq:users")
	})
	c = New(prefix, c.rdb, c.resolve)
	writeDelta(t, c, store, "users", "/", MergeTypeReplace, `{"n":1}`)

	sampler := NewSampler[int]("views", func(*ListResult) (int, error) { return 7, nil })
	if _, err := sampler.Sample(ctx, c.List(ctx, "users")); err != nil {
		t.Fatalf("prime Sample: %v", err)
	}
	memoKey := c.idx.SampleKey("views")
	if n, err := c.sampleRdb.HExists(ctx, memoKey, "users").Result(); err != nil || !n {
		t.Fatalf("prime not cached (exists=%v err=%v)", n, err)
	}
	if existed, err := c.DeleteCatalog(ctx, "users"); err != nil || !existed {
		t.Fatalf("DeleteCatalog: existed=%v err=%v", existed, err)
	}
	if n, err := c.sampleRdb.HExists(ctx, memoKey, "users").Result(); err != nil || n {
		t.Fatalf("glob-prefix memo hash escaped the sweep (exists=%v err=%v)", n, err)
	}
}

// TestRemoveDeltaBlocksStaleSnapshot_Redis: a snapshot computed from a read
// that listed the since-removed delta must not land; one from a
// post-removal read lands normally.
func TestRemoveDeltaBlocksStaleSnapshot_Redis(t *testing.T) {
	c, store, ctx := newMemClient(t, WithSnapTarget("mem", "snaps"))
	writeDelta(t, c, store, "users", "/", MergeTypeReplace, `{"secret":true}`)

	list := c.List(ctx, "users")
	stop := list.Entries[0].TsSeq
	if removed, err := c.RemoveDelta(ctx, "users", stop.String()); err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v", removed, err)
	}
	if _, err := c.saveSnapshot(ctx, "users", stop, list.removeGen, []byte(`{"secret":true}`)); err != nil {
		t.Fatalf("stale saveSnapshot: %v", err)
	}
	if snap, _ := c.idx.GetLatestSnap(ctx, "users"); snap != nil {
		t.Fatalf("stale snapshot resurrected the removed delta: %+v", snap)
	}

	writeDelta(t, c, store, "users", "/", MergeTypeReplace, `{"clean":true}`)
	list = c.List(ctx, "users")
	if _, err := c.saveSnapshot(ctx, "users", list.Entries[0].TsSeq, list.removeGen, []byte(`{"clean":true}`)); err != nil {
		t.Fatalf("fresh saveSnapshot: %v", err)
	}
	snap, err := c.idx.GetLatestSnap(ctx, "users")
	if err != nil || snap == nil || snap.StopTsSeq != list.Entries[0].TsSeq {
		t.Fatalf("fresh snapshot missing or at wrong stop: snap=%+v err=%v", snap, err)
	}
}

// TestRemoveDeltaSnapshotPathIsolation_Redis: removing a non-latest delta
// leaves the stop unchanged, so stale and fresh generations save snapshots
// for the SAME stop. The stale Put must not overwrite the object the
// published pointer references, even when it finishes last.
func TestRemoveDeltaSnapshotPathIsolation_Redis(t *testing.T) {
	c, store, ctx := newMemClient(t, WithSnapTarget("mem", "snaps"))
	writeDelta(t, c, store, "users", "/", MergeTypeReplace, `{"a":1}`)
	writeDelta(t, c, store, "users", "/", MergeTypeReplace, `{"b":2}`)

	preList := c.List(ctx, "users")
	if removed, err := c.RemoveDelta(ctx, "users", preList.Entries[0].TsSeq.String()); err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v", removed, err)
	}
	postList := c.List(ctx, "users")
	stop := postList.Entries[0].TsSeq
	if preList.Entries[1].TsSeq != stop {
		t.Fatal("test setup: stops must be identical across the removal")
	}

	freshURI, err := c.saveSnapshot(ctx, "users", stop, postList.removeGen, []byte(`{"fresh":true}`))
	if err != nil {
		t.Fatalf("fresh saveSnapshot: %v", err)
	}
	staleURI, err := c.saveSnapshot(ctx, "users", stop, preList.removeGen, []byte(`{"stale":true}`))
	if err != nil {
		t.Fatalf("stale saveSnapshot: %v", err)
	}
	if staleURI == freshURI {
		t.Fatalf("generations share an object path: %s", freshURI)
	}
	snap, err := c.idx.GetLatestSnap(ctx, "users")
	if err != nil || snap == nil || snap.URI != freshURI {
		t.Fatalf("pointer = %+v (err=%v), want the fresh generation's %s", snap, err, freshURI)
	}
	_, _, path, _ := objkey.ParseURI(snap.URI)
	if data, _ := store.Bucket("snaps").Get(ctx, "users", path); string(data) != `{"fresh":true}` {
		t.Fatalf("pointer object overwritten by the stale generation: %s", data)
	}
}
