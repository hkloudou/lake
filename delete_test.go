package lake

import (
	"context"
	"testing"

	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
)

// TestDeleteCatalog_Redis pins the delete contract: index state and cached
// samples are gone, a snapshot computed before the delete cannot land after
// it, and the catalog is immediately writable again from empty.
func TestDeleteCatalog_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, _, bucket string) (storage.Storage, error) {
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
			t.Fatal(err)
		}
		_ = store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(body))
		if err := c.WriteNotify(ctx, h); err != nil {
			t.Fatal(err)
		}
	}

	if existed, err := c.DeleteCatalog(ctx, "users"); err != nil || existed {
		t.Fatalf("delete of a missing catalog: existed=%v err=%v, want false/nil", existed, err)
	}

	write(`{"a":1}`)
	if _, err := ReadString(ctx, c.List(ctx, "users")); err != nil {
		t.Fatal(err)
	}
	runs := 0
	sampler := NewSampler[int]("cnt", func(l *ListResult) (int, error) { runs++; return len(l.Entries), nil })
	if v, err := sampler.Sample(ctx, c.List(ctx, "users")); err != nil || v != 1 {
		t.Fatalf("sample = %d/%v, want 1", v, err)
	}
	if !waitFor(func() bool { s, _ := c.idx.GetLatestSnap(ctx, "users"); return s != nil }) {
		t.Fatal("snapshot not persisted")
	}

	// A read listed before the delete; its snapshot save arrives after.
	stale := c.List(ctx, "users")
	write(`{"a":2}`)
	stale2 := c.List(ctx, "users")

	if existed, err := c.DeleteCatalog(ctx, "users"); err != nil || !existed {
		t.Fatalf("DeleteCatalog: existed=%v err=%v, want true/nil", existed, err)
	}
	if _, err := c.saveSnapshot(ctx, "users", stale2.Entries[len(stale2.Entries)-1].TsSeq, stale2.removeGen, []byte(`{"a":2}`)); err != nil {
		t.Fatalf("stale saveSnapshot: %v", err)
	}
	_ = stale

	list := c.List(ctx, "users")
	if list.Err != nil || list.Exist() {
		t.Fatalf("after delete: err=%v exist=%v, want empty", list.Err, list.Exist())
	}
	if got, _ := ReadString(ctx, list); got != "{}" {
		t.Fatalf("read after delete = %q, want {}", got)
	}
	if v, err := sampler.Sample(ctx, list); err != nil || v != 0 || runs != 2 {
		t.Fatalf("sample after delete = %d/%v (runs=%d), want 0 recomputed", v, err, runs)
	}

	// Writable again from empty.
	write(`{"b":3}`)
	if got, err := ReadString(ctx, c.List(ctx, "users")); err != nil || got != `{"b":3}` {
		t.Fatalf("read after re-write = %q/%v", got, err)
	}
}
