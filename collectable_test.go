package lake

import (
	"context"
	"strings"
	"testing"

	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
)

func TestCollectable_ValidatesCatalog(t *testing.T) {
	c := newDeadClient(t)
	if _, err := c.Collectable(context.Background(), "bad|name"); err == nil || !strings.Contains(err.Error(), "invalid catalog") {
		t.Fatalf("err = %v, want invalid catalog", err)
	}
}

// TestCollectable_Redis: no snapshot → 0; after a read persists a snapshot
// the entries at or before its stop count, later writes do not, and the
// index itself is left untouched — Lake only reports.
func TestCollectable_Redis(t *testing.T) {
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
		h, err := beginWrite(c, WriteRequest{
			Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
		})
		if err != nil {
			t.Fatal(err)
		}
		_ = upload(store, h, body)
		if err := c.WriteNotify(ctx, h); err != nil {
			t.Fatal(err)
		}
	}

	write(`{"a":1}`)
	write(`{"a":2}`)
	if n, err := c.Collectable(ctx, "users"); err != nil || n != 0 {
		t.Fatalf("before any snapshot: n=%d err=%v, want 0", n, err)
	}
	if _, err := ReadString(ctx, c.List(ctx, "users")); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { s, _ := c.idx.GetLatestSnap(ctx, "users"); return s != nil }) {
		t.Fatal("snapshot not persisted")
	}
	if n, err := c.Collectable(ctx, "users"); err != nil || n != 2 {
		t.Fatalf("after snapshot: n=%d err=%v, want 2", n, err)
	}
	write(`{"a":3}`)
	if n, err := c.Collectable(ctx, "users"); err != nil || n != 2 {
		t.Fatalf("with a write past the snapshot: n=%d err=%v, want 2", n, err)
	}
	if z, _ := rdb.ZCard(ctx, prefix+":d:users").Result(); z != 3 {
		t.Fatalf("delta log has %d entries, want 3 (Collectable must not trim)", z)
	}
	if got, err := ReadString(ctx, c.List(ctx, "users")); err != nil || got != `{"a":3}` {
		t.Fatalf("read = %q/%v", got, err)
	}
}
