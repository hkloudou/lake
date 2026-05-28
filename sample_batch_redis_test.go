package lake

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/storage"
	"github.com/redis/go-redis/v9"
)

// TestBatchSample_HitsAndMisses_Redis exercises BatchSample against a
// real Redis instance:
//   - 3 catalogs share the same indicator
//   - 1 has a pre-seeded fresh cache entry → returned without invoking loader
//   - 2 have no cache → loader runs once each
//   - HLen on the indicator hash must be 3 after the call
//
// Skips when Redis is unreachable.
func TestBatchSample_HitsAndMisses_Redis(t *testing.T) {
	probe := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:6379",
		DB:          14,
		DialTimeout: 200 * time.Millisecond,
	})
	pingCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := probe.Ping(pingCtx).Err(); err != nil {
		_ = probe.Close()
		t.Skipf("redis not reachable, skipping integration test: %v", err)
	}
	if err := probe.FlushDB(pingCtx).Err(); err != nil {
		t.Fatalf("FlushDB: %v", err)
	}
	_ = probe.Close()

	c := NewLake("redis://127.0.0.1:6379/14",
		WithStorage(storage.NewMemoryStorage("test")),
	)

	ctx := context.Background()
	if err := c.ensureInitialized(ctx); err != nil {
		t.Fatalf("ensureInitialized: %v", err)
	}

	mkList := func(cat string, ts index.TimeSeqID) *ListResult {
		return &ListResult{
			client:  c,
			catalog: cat,
			Entries: []index.DeltaInfo{{TsSeq: ts, Score: ts.Score()}},
		}
	}

	stop := index.TimeSeqID{Timestamp: 1700000100, SeqID: 500}

	// Pre-seed a cache hit for "users" at score == lastUpdated.
	primed, err := marshalSampleCache(SampleMeta{Score: stop.Score(), UpdatedAt: time.Now().Unix()}, 7)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	hashKey := c.reader.MakeSampleIndicatorKey("daily")
	if err := c.rdb.HSet(ctx, hashKey, "users", primed).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	lists := map[string]*ListResult{
		"users":    mkList("users", stop),
		"orders":   mkList("orders", stop),
		"products": mkList("products", stop),
	}
	calls := atomic.Int32{}
	loader := func(l *ListResult) (int, error) {
		calls.Add(1)
		switch l.catalog {
		case "orders":
			return 11, nil
		case "products":
			return 22, nil
		}
		t.Errorf("loader unexpectedly invoked for %q", l.catalog)
		return 0, nil
	}
	out := NewSampler[int]("daily", loader).Batch(ctx, lists)

	if got := calls.Load(); got != 2 {
		t.Errorf("loader call count: got %d, want 2 (one per miss)", got)
	}
	for cat, want := range map[string]int{"users": 7, "orders": 11, "products": 22} {
		r := out[cat]
		if r == nil {
			t.Errorf("missing result for %s", cat)
			continue
		}
		if r.Err != nil {
			t.Errorf("%s: unexpected err %v", cat, r.Err)
			continue
		}
		if r.Value != want {
			t.Errorf("%s: got %d, want %d", cat, r.Value, want)
		}
	}

	cnt, err := c.rdb.HLen(ctx, hashKey).Result()
	if err != nil {
		t.Fatalf("HLen: %v", err)
	}
	if cnt != 3 {
		t.Errorf("HLen %s: got %d, want 3", hashKey, cnt)
	}
}
