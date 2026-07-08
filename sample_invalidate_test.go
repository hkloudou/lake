package lake

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/objkey"
	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
)

func TestInvalidateSamples_ValidatesBeforeRedis(t *testing.T) {
	c := newDeadClient(t)
	if _, err := c.InvalidateSamples(context.Background(), "bad|indicator", "users"); err == nil || !strings.Contains(err.Error(), "invalid indicator") {
		t.Fatalf("expected invalid indicator error, got %v", err)
	}
	if _, err := c.InvalidateSamples(context.Background(), "views", "bad|name"); err == nil {
		t.Fatal("expected invalid catalog error, got nil")
	}
	// No catalogs → trivially nothing to do, no Redis call.
	if n, err := c.InvalidateSamples(context.Background(), "views"); err != nil || n != 0 {
		t.Fatalf("empty catalogs: n=%d err=%v, want 0/nil", n, err)
	}
}

// TestInvalidateSamples_ForcesRecompute_Redis pins the full memo lifecycle:
// compute → cached (loader not re-run) → InvalidateSamples → recomputed.
// This is the escape hatch for the two cases staleness policies cannot see —
// a deleted catalog's lingering memo field, and a loader whose code changed
// under an unchanged data version.
func TestInvalidateSamples_ForcesRecompute_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")
	c := New(prefix, rdb, memResolver())

	ctx := context.Background()
	list := &ListResult{client: c, catalog: "users", Entries: []index.DeltaInfo{{Score: 42}}}

	var runs atomic.Int64
	sampler := NewSampler[int]("views", func(*ListResult) (int, error) {
		return int(runs.Add(1)), nil
	})

	if v, err := sampler.Sample(ctx, list); err != nil || v != 1 {
		t.Fatalf("first Sample: v=%d err=%v, want 1/nil", v, err)
	}
	if v, err := sampler.Sample(ctx, list); err != nil || v != 1 {
		t.Fatalf("cached Sample: v=%d err=%v, want 1/nil (loader must not re-run)", v, err)
	}

	n, err := c.InvalidateSamples(ctx, "views", "users")
	if err != nil {
		t.Fatalf("InvalidateSamples: %v", err)
	}
	if n != 1 {
		t.Fatalf("InvalidateSamples removed %d entries, want 1", n)
	}

	if v, err := sampler.Sample(ctx, list); err != nil || v != 2 {
		t.Fatalf("Sample after invalidate: v=%d err=%v, want 2/nil (recompute)", v, err)
	}
}

// TestInvalidateSamples_BarriersInFlightWrite_Redis pins the epoch barrier:
// a compute that captured its epoch BEFORE an invalidation must not be able
// to write its (now stale) value back afterwards — otherwise the write would
// reinstate exactly what was just invalidated, and with the data version
// unchanged nothing would ever evict it again.
func TestInvalidateSamples_BarriersInFlightWrite_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")
	c := New(prefix, rdb, memResolver())

	ctx := context.Background()
	hashKey := c.reader.MakeSampleIndicatorKey("views")
	genKey := c.reader.MakeSampleRemoveGenKey()
	staleValue, _ := marshalSampleCache(SampleMeta{Score: 42, UpdatedAt: 1}, 1)

	// An in-flight compute captured epoch "0" (no invalidation yet)…
	inFlightEpoch := "0"
	// …then the invalidation lands…
	if _, err := c.InvalidateSamples(ctx, "views", "users"); err != nil {
		t.Fatalf("InvalidateSamples: %v", err)
	}
	// …and the in-flight write-back must be discarded.
	if err := sampleWriteScript.Run(ctx, c.sampleRdb, []string{hashKey, genKey}, inFlightEpoch, "0", "users", staleValue).Err(); err != nil {
		t.Fatalf("stale write eval: %v", err)
	}
	if n, err := c.sampleRdb.HExists(ctx, hashKey, "users").Result(); err != nil || n {
		t.Fatalf("stale in-flight write survived the epoch barrier (exists=%v err=%v)", n, err)
	}

	// A write that observed the post-invalidation epoch lands normally.
	epoch, err := c.sampleRdb.HGet(ctx, hashKey, sampleEpochField).Result()
	if err != nil {
		t.Fatalf("read epoch: %v", err)
	}
	if err := sampleWriteScript.Run(ctx, c.sampleRdb, []string{hashKey, genKey}, epoch, "0", "users", staleValue).Err(); err != nil {
		t.Fatalf("fresh write eval: %v", err)
	}
	if n, err := c.sampleRdb.HExists(ctx, hashKey, "users").Result(); err != nil || !n {
		t.Fatalf("current-epoch write was wrongly discarded (exists=%v err=%v)", n, err)
	}
}

// TestRemoveDeltaBlocksUnseenIndicatorWrite_Redis pins the catalog-level
// barrier for an indicator that has NEVER cached anything: its memo hash
// does not exist, so no key sweep can reach it — but a first-ever compute
// that read the pre-removal list must still not land its value after the
// removal. The "<prefix>:mrg" generation exists independently of memo
// hashes and blocks exactly that write.
func TestRemoveDeltaBlocksUnseenIndicatorWrite_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, provider, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve)

	ctx := context.Background()
	h, err := c.WriteBegin(ctx, WriteBeginRequest{
		Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
	})
	if err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
	if err := store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(`{"n":1}`)); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := c.WriteNotify(ctx, h); err != nil {
		t.Fatalf("WriteNotify: %v", err)
	}
	list := c.List(ctx, "users")
	if list.Err != nil || len(list.Entries) != 1 {
		t.Fatalf("List: err=%v entries=%d", list.Err, len(list.Entries))
	}

	// A first-ever compute for indicator "fresh" captured its barriers
	// before the removal: memo hash absent → epoch "0", gen "0".
	hashKey := c.reader.MakeSampleIndicatorKey("fresh")
	genKey := c.reader.MakeSampleRemoveGenKey()
	staleValue, _ := marshalSampleCache(SampleMeta{Score: list.LastUpdated(), UpdatedAt: 1}, 1)

	if removed, err := c.RemoveDelta(ctx, "users", list.Entries[0].TsSeq.String()); err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v", removed, err)
	}

	// The pre-removal compute's write-back must be discarded even though its
	// memo hash never existed for the sweep to find.
	if err := sampleWriteScript.Run(ctx, c.sampleRdb, []string{hashKey, genKey}, "0", "0", "users", staleValue).Err(); err != nil {
		t.Fatalf("stale write eval: %v", err)
	}
	if n, err := c.sampleRdb.HExists(ctx, hashKey, "users").Result(); err != nil || n {
		t.Fatalf("pre-removal write for an unseen indicator landed (exists=%v err=%v)", n, err)
	}

	// A compute that observed the post-removal generation caches normally.
	gen, err := c.sampleRdb.HGet(ctx, genKey, "users").Result()
	if err != nil || gen != "1" {
		t.Fatalf("removal gen = %q err=%v, want \"1\"", gen, err)
	}
	if err := sampleWriteScript.Run(ctx, c.sampleRdb, []string{hashKey, genKey}, "0", gen, "users", staleValue).Err(); err != nil {
		t.Fatalf("fresh write eval: %v", err)
	}
	if n, err := c.sampleRdb.HExists(ctx, hashKey, "users").Result(); err != nil || !n {
		t.Fatalf("current-generation write was wrongly discarded (exists=%v err=%v)", n, err)
	}
}

// TestSampleStaleListNotCached_Redis pins the list-time generation recheck:
// a loader that computed from a ListResult taken BEFORE a RemoveDelta must
// not cache its result AFTER the removal — the probe-time barriers look
// current, but the entries the value was derived from are not. The caller
// still gets the value; only the write-back is dropped.
func TestSampleStaleListNotCached_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, provider, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve)

	ctx := context.Background()
	write := func(body string) {
		t.Helper()
		h, err := c.WriteBegin(ctx, WriteBeginRequest{
			Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
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
	}
	write(`{"a":1}`)
	write(`{"b":2}`)

	staleList := c.List(ctx, "users")
	if staleList.Err != nil || len(staleList.Entries) != 2 {
		t.Fatalf("List: err=%v entries=%d", staleList.Err, len(staleList.Entries))
	}

	if removed, err := c.RemoveDelta(ctx, "users", staleList.Entries[0].TsSeq.String()); err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v", removed, err)
	}

	var runs atomic.Int64
	sampler := NewSampler[int]("views", func(*ListResult) (int, error) {
		return int(runs.Add(1)), nil
	})

	// The exported generation accessor tracks the removal (predicates use it
	// for peer-dependency baselines).
	if g := staleList.RemoveGen(); g != "0" {
		t.Fatalf("pre-removal RemoveGen() = %q, want \"0\"", g)
	}
	if g := c.List(ctx, "users").RemoveGen(); g != "1" {
		t.Fatalf("post-removal RemoveGen() = %q, want \"1\"", g)
	}

	// Sampling the PRE-removal list: value returned, write-back dropped.
	if v, err := sampler.Sample(ctx, staleList); err != nil || v != 1 {
		t.Fatalf("stale-list Sample: v=%d err=%v, want 1/nil", v, err)
	}
	if n, err := c.sampleRdb.HExists(ctx, c.reader.MakeSampleIndicatorKey("views"), "users").Result(); err != nil || n {
		t.Fatalf("stale-list result was cached (exists=%v err=%v)", n, err)
	}

	// A fresh list samples, recomputes, and caches normally.
	if v, err := sampler.Sample(ctx, c.List(ctx, "users")); err != nil || v != 2 {
		t.Fatalf("fresh-list Sample: v=%d err=%v, want 2/nil", v, err)
	}
	if n, err := c.sampleRdb.HExists(ctx, c.reader.MakeSampleIndicatorKey("views"), "users").Result(); err != nil || !n {
		t.Fatalf("fresh-list result missing from cache (exists=%v err=%v)", n, err)
	}
}

// TestRemoveDeltaSnapshotPathIsolation_Redis pins the per-generation object
// path: removing a non-latest delta leaves the stop unchanged, so the stale
// and fresh generations save snapshots FOR THE SAME STOP. The stale save's
// Put must not be able to overwrite the object the published pointer
// references — even when it finishes last.
func TestRemoveDeltaSnapshotPathIsolation_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, provider, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve, WithSnapTarget("mem", "snaps"))

	ctx := context.Background()
	write := func(body string) {
		t.Helper()
		h, err := c.WriteBegin(ctx, WriteBeginRequest{
			Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
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
	}
	write(`{"a":1}`)
	write(`{"b":2}`)

	preList := c.List(ctx, "users")
	if preList.Err != nil || len(preList.Entries) != 2 {
		t.Fatalf("pre List: err=%v entries=%d", preList.Err, len(preList.Entries))
	}
	if removed, err := c.RemoveDelta(ctx, "users", preList.Entries[0].TsSeq.String()); err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v", removed, err)
	}
	postList := c.List(ctx, "users")
	if postList.Err != nil || len(postList.Entries) != 1 {
		t.Fatalf("post List: err=%v entries=%d", postList.Err, len(postList.Entries))
	}
	if preList.NextSnap().StopTsSeq != postList.NextSnap().StopTsSeq {
		t.Fatal("test setup: stops must be identical across the removal")
	}

	// The fresh generation publishes first…
	freshURI, err := c.saveSnapshot(ctx, "users", postList.NextSnap().StopTsSeq, postList.removeGen, []byte(`{"fresh":true}`))
	if err != nil {
		t.Fatalf("fresh saveSnapshot: %v", err)
	}
	// …then the stale generation's save finishes LAST (the overwrite race).
	staleURI, err := c.saveSnapshot(ctx, "users", preList.NextSnap().StopTsSeq, preList.removeGen, []byte(`{"stale":true}`))
	if err != nil {
		t.Fatalf("stale saveSnapshot: %v", err)
	}
	if staleURI == freshURI {
		t.Fatalf("generations share an object path: %s", freshURI)
	}

	snap, err := c.reader.GetLatestSnap(ctx, "users")
	if err != nil || snap == nil {
		t.Fatalf("GetLatestSnap: snap=%+v err=%v", snap, err)
	}
	if snap.URI != freshURI {
		t.Fatalf("pointer = %s, want the fresh generation's %s", snap.URI, freshURI)
	}
	_, _, path, err := objkey.ParseURI(snap.URI)
	if err != nil {
		t.Fatalf("parse pointer URI: %v", err)
	}
	data, err := store.Bucket("snaps").Get(ctx, "users", path)
	if err != nil {
		t.Fatalf("fetch pointer object: %v", err)
	}
	if string(data) != `{"fresh":true}` {
		t.Fatalf("pointer object overwritten by the stale generation: %s", data)
	}
}

// TestRemoveDeltaSweepsGlobPrefix_Redis: a deployment prefix may contain
// Redis MATCH metacharacters; the memo sweep must still reach this
// deployment's literal keys (unescaped, "p[g]…" would match "pg…" instead).
func TestRemoveDeltaSweepsGlobPrefix_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t) + "[g]*?"
	t.Cleanup(func() {
		rdb.Del(context.Background(),
			prefix+":d:users", prefix+":s", prefix+":m:views", prefix+":mrg")
	})

	store := mem.New()
	resolve := func(_ storage.Kind, provider, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve)

	ctx := context.Background()
	h, err := c.WriteBegin(ctx, WriteBeginRequest{
		Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
	})
	if err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
	if err := store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(`{"n":1}`)); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := c.WriteNotify(ctx, h); err != nil {
		t.Fatalf("WriteNotify: %v", err)
	}
	list := c.List(ctx, "users")
	if list.Err != nil || len(list.Entries) != 1 {
		t.Fatalf("List: err=%v entries=%d", list.Err, len(list.Entries))
	}

	sampler := NewSampler[int]("views", func(*ListResult) (int, error) { return 7, nil })
	if _, err := sampler.Sample(ctx, list); err != nil {
		t.Fatalf("prime Sample: %v", err)
	}
	memoKey := c.reader.MakeSampleIndicatorKey("views")
	if n, err := c.sampleRdb.HExists(ctx, memoKey, "users").Result(); err != nil || !n {
		t.Fatalf("prime not cached (exists=%v err=%v)", n, err)
	}

	if removed, err := c.RemoveDelta(ctx, "users", list.Entries[0].TsSeq.String()); err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v", removed, err)
	}
	if n, err := c.sampleRdb.HExists(ctx, memoKey, "users").Result(); err != nil || n {
		t.Fatalf("glob-prefix memo hash escaped the sweep (exists=%v err=%v)", n, err)
	}
}

// TestSampleUnsweptStaleEntryRejected_Redis pins the read-time generation
// check: an entry the post-removal sweep failed to delete (simulated by
// re-planting it) must NOT be served to a post-removal ListResult, even
// though its Score satisfies the version floor.
func TestSampleUnsweptStaleEntryRejected_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, provider, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve)

	ctx := context.Background()
	h, err := c.WriteBegin(ctx, WriteBeginRequest{
		Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
	})
	if err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
	if err := store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(`{"n":1}`)); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := c.WriteNotify(ctx, h); err != nil {
		t.Fatalf("WriteNotify: %v", err)
	}
	pre := c.List(ctx, "users")
	if pre.Err != nil || len(pre.Entries) != 1 {
		t.Fatalf("List: err=%v entries=%d", pre.Err, len(pre.Entries))
	}

	// A pre-removal cached entry, planted as if the sweep had missed it:
	// generation "0", score high enough to satisfy the version floor.
	stale, _ := marshalSampleCache(SampleMeta{Score: pre.LastUpdated() + 100, UpdatedAt: 1, RemoveGen: "0"}, 111)
	memoKey := c.reader.MakeSampleIndicatorKey("views")

	if removed, err := c.RemoveDelta(ctx, "users", pre.Entries[0].TsSeq.String()); err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v", removed, err)
	}
	if err := c.sampleRdb.HSet(ctx, memoKey, "users", stale).Err(); err != nil {
		t.Fatalf("plant stale entry: %v", err)
	}

	var runs atomic.Int64
	sampler := NewSampler[int]("views", func(*ListResult) (int, error) {
		runs.Add(1)
		return 222, nil
	})
	// Post-removal list (generation "1") must reject the generation-"0" hit
	// and recompute, despite the planted score beating the version floor.
	if v, err := sampler.Sample(ctx, c.List(ctx, "users")); err != nil || v != 222 {
		t.Fatalf("Sample: v=%d err=%v, want recomputed 222/nil", v, err)
	}
	if runs.Load() != 1 {
		t.Fatalf("loader ran %d times, want 1 (stale hit must be rejected)", runs.Load())
	}
}

// TestInvalidateSamples_NewFlightAfterInvalidation: a Sample that probes
// AFTER an invalidation must not join a still-running pre-invalidation
// loader (same catalog, indicator, and data version) and share its stale
// result — the barrier values are part of the flight key, so it computes
// its own.
func TestInvalidateSamples_NewFlightAfterInvalidation(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")
	c := New(prefix, rdb, memResolver())

	ctx := context.Background()
	list := &ListResult{client: c, catalog: "users", Entries: []index.DeltaInfo{{Score: 42}}}

	gate := make(chan struct{})
	var runs atomic.Int64
	sampler := NewSampler[int]("views", func(*ListResult) (int, error) {
		n := int(runs.Add(1))
		if n == 1 {
			<-gate // first (pre-invalidation) loader hangs mid-compute
		}
		return n, nil
	})

	first := make(chan int, 1)
	go func() {
		v, _ := sampler.Sample(ctx, list)
		first <- v
	}()
	if !waitFor(func() bool { return runs.Load() == 1 }) {
		t.Fatal("first loader did not start")
	}

	if _, err := c.InvalidateSamples(ctx, "views", "users"); err != nil {
		t.Fatalf("InvalidateSamples: %v", err)
	}

	// Post-invalidation call: must run its own loader (value 2), not join
	// the hung pre-invalidation flight and inherit its value 1.
	done := make(chan struct{})
	var v2 int
	var err2 error
	go func() {
		v2, err2 = sampler.Sample(ctx, list)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("post-invalidation Sample joined the hung pre-invalidation flight")
	}
	if err2 != nil || v2 != 2 {
		t.Fatalf("post-invalidation Sample: v=%d err=%v, want 2/nil", v2, err2)
	}

	close(gate)
	if v1 := <-first; v1 != 1 {
		t.Fatalf("pre-invalidation Sample: v=%d, want 1", v1)
	}
	// And its stale write-back must not have landed.
	if raw, err := c.sampleRdb.HGet(ctx, c.reader.MakeSampleIndicatorKey("views"), "users").Bytes(); err == nil {
		_, cachedVal, derr := unmarshalSampleCache[int](raw)
		if derr == nil && cachedVal == 1 {
			t.Fatal("stale pre-invalidation value was cached")
		}
	}
}

// TestRemoveDeltaInvalidatesSamples_Redis: removing a delta lowers the
// catalog's data version, so the version floor alone would keep serving a
// sample computed WITH the removed write forever. RemoveDelta therefore
// sweeps every indicator's memo entry for the catalog.
func TestRemoveDeltaInvalidatesSamples_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, provider, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve)

	ctx := context.Background()
	h, err := c.WriteBegin(ctx, WriteBeginRequest{
		Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
	})
	if err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
	if err := store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(`{"n":1}`)); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := c.WriteNotify(ctx, h); err != nil {
		t.Fatalf("WriteNotify: %v", err)
	}

	var runs atomic.Int64
	sampler := NewSampler[int]("views", func(*ListResult) (int, error) {
		return int(runs.Add(1)), nil
	})
	list := c.List(ctx, "users")
	if list.Err != nil || len(list.Entries) != 1 {
		t.Fatalf("List: err=%v entries=%d", list.Err, len(list.Entries))
	}
	if v, err := sampler.Sample(ctx, list); err != nil || v != 1 {
		t.Fatalf("prime Sample: v=%d err=%v, want 1/nil", v, err)
	}
	if v, err := sampler.Sample(ctx, list); err != nil || v != 1 {
		t.Fatalf("cached Sample: v=%d err=%v, want 1/nil", v, err)
	}

	removed, err := c.RemoveDelta(ctx, "users", list.Entries[0].TsSeq.String())
	if err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v", removed, err)
	}

	// The memo entry is gone; the next sample recomputes from the
	// post-removal state instead of serving the removed write's value.
	if v, err := sampler.Sample(ctx, c.List(ctx, "users")); err != nil || v != 2 {
		t.Fatalf("Sample after RemoveDelta: v=%d err=%v, want 2/nil (recompute)", v, err)
	}
}

// TestRemoveDeltaBlocksStaleSnapshot_Redis pins the client-level barrier: a
// snapshot save whose data came from a read that listed the (since-removed)
// delta must not land; a save from a post-removal read lands normally.
func TestRemoveDeltaBlocksStaleSnapshot_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, provider, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve, WithSnapTarget("mem", "snaps"))

	ctx := context.Background()
	write := func(body string) {
		t.Helper()
		h, err := c.WriteBegin(ctx, WriteBeginRequest{
			Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
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
	}
	write(`{"secret":true}`)

	// A reader listed the delta (no Read → no auto-snapshot), then the delta
	// is removed. The reader's snapshot publication must be dropped.
	list := c.List(ctx, "users")
	if list.Err != nil || len(list.Entries) != 1 {
		t.Fatalf("List: err=%v entries=%d", list.Err, len(list.Entries))
	}
	stale := list.NextSnap()
	if removed, err := c.RemoveDelta(ctx, "users", list.Entries[0].TsSeq.String()); err != nil || !removed {
		t.Fatalf("RemoveDelta: removed=%v err=%v", removed, err)
	}
	if _, err := c.saveSnapshot(ctx, "users", stale.StopTsSeq, list.removeGen, []byte(`{"secret":true}`)); err != nil {
		t.Fatalf("stale saveSnapshot: %v", err)
	}
	if snap, _ := c.reader.GetLatestSnap(ctx, "users"); snap != nil {
		t.Fatalf("stale snapshot resurrected the removed delta: %+v", snap)
	}

	// A post-removal read snapshots normally (new write, current generation).
	write(`{"clean":true}`)
	list = c.List(ctx, "users")
	if list.Err != nil || len(list.Entries) != 1 {
		t.Fatalf("List after rewrite: err=%v entries=%d", list.Err, len(list.Entries))
	}
	if _, err := c.saveSnapshot(ctx, "users", list.NextSnap().StopTsSeq, list.removeGen, []byte(`{"clean":true}`)); err != nil {
		t.Fatalf("fresh saveSnapshot: %v", err)
	}
	snap, err := c.reader.GetLatestSnap(ctx, "users")
	if err != nil || snap == nil {
		t.Fatalf("fresh snapshot missing: snap=%+v err=%v", snap, err)
	}
	if snap.StopTsSeq != list.Entries[0].TsSeq {
		t.Fatalf("fresh snapshot at wrong stop: %v, want %v", snap.StopTsSeq, list.Entries[0].TsSeq)
	}
}
