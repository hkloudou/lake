package lake

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
)

type countingStore struct {
	storage.Storage
	puts *atomic.Int32
}

func (s countingStore) Put(ctx context.Context, catalog, path string, data []byte) error {
	s.puts.Add(1)
	return s.Storage.Put(ctx, catalog, path, data)
}

// TestWithSnapMinDeltas_Redis: with a threshold of 3, reads that see 1 or 2
// deltas past the snap upload nothing; the read that sees 3 snapshots.
func TestWithSnapMinDeltas_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	var snapPuts atomic.Int32
	resolve := func(kind storage.Kind, _, bucket string) (storage.Storage, error) {
		if kind == storage.Snap {
			return countingStore{store.Bucket(bucket), &snapPuts}, nil
		}
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve, WithSnapTarget("mem", "snaps"), WithSnapMinDeltas(3))
	ctx := context.Background()

	writeAndRead := func() {
		t.Helper()
		h, err := c.WriteBegin(ctx, WriteBeginRequest{
			Catalog: "doc", Path: "/n", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
		})
		if err != nil {
			t.Fatal(err)
		}
		_ = store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(`1`))
		if err := c.WriteNotify(ctx, h); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadString(ctx, c.List(ctx, "doc")); err != nil {
			t.Fatal(err)
		}
	}

	writeAndRead()
	writeAndRead()
	if waitFor(func() bool { return snapPuts.Load() > 0 }) {
		t.Fatalf("snapshot uploaded below the threshold (%d puts)", snapPuts.Load())
	}
	writeAndRead()
	if !waitFor(func() bool { s, _ := c.idx.GetLatestSnap(ctx, "doc"); return s != nil }) {
		t.Fatal("third delta must trigger a snapshot")
	}
	if snapPuts.Load() != 1 {
		t.Fatalf("snapshot uploads = %d, want 1", snapPuts.Load())
	}
}

func TestWithSnapMinDeltas_PanicsBelowOne(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("WithSnapMinDeltas(0) must panic")
		}
	}()
	WithSnapMinDeltas(0)
}
