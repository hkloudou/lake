package lake

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hkloudou/lake/v3/internal/index"
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
	staleValue, _ := marshalSampleCache(SampleMeta{Score: 42, UpdatedAt: 1}, 1)

	// An in-flight compute captured epoch "0" (no invalidation yet)…
	inFlightEpoch := "0"
	// …then the invalidation lands…
	if _, err := c.InvalidateSamples(ctx, "views", "users"); err != nil {
		t.Fatalf("InvalidateSamples: %v", err)
	}
	// …and the in-flight write-back must be discarded.
	if err := c.sampleRdb.Eval(ctx, sampleWriteScript, []string{hashKey}, inFlightEpoch, "users", staleValue).Err(); err != nil {
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
	if err := c.sampleRdb.Eval(ctx, sampleWriteScript, []string{hashKey}, epoch, "users", staleValue).Err(); err != nil {
		t.Fatalf("fresh write eval: %v", err)
	}
	if n, err := c.sampleRdb.HExists(ctx, hashKey, "users").Result(); err != nil || !n {
		t.Fatalf("current-epoch write was wrongly discarded (exists=%v err=%v)", n, err)
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
