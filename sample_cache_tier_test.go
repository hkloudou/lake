package lake

import (
	"context"
	"testing"
	"time"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/redis/go-redis/v9"
)

// TestSampleCache_SplitTierRedis verifies WithSampleCacheRedis routes the
// sample memo Hash to a dedicated cache-tier Redis (here the same server on a
// separate logical DB) while the authoritative index stays on its own DB. It
// asserts the memo lands on the cache tier (DB 14) and never leaks onto the
// index tier (DB 13) — the segregation that lets a cache outage degrade
// gracefully without ever touching the source of truth.
//
// Unlike sample_batch_redis_test.go (index and cache share one DB because
// sampleRdb defaults to rdb), this exercises the split-tier wiring directly.
//
// Skips when Redis is unreachable.
func TestSampleCache_SplitTierRedis(t *testing.T) {
	const addr = "127.0.0.1:6379"
	idx := redis.NewClient(&redis.Options{Addr: addr, DB: 13, DialTimeout: 200 * time.Millisecond})
	cache := redis.NewClient(&redis.Options{Addr: addr, DB: 14, DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = idx.Close(); _ = cache.Close() })

	ctx := context.Background()
	pingCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	for _, rdb := range []*redis.Client{idx, cache} {
		if err := rdb.Ping(pingCtx).Err(); err != nil {
			t.Skipf("redis not reachable, skipping integration test: %v", err)
		}
		if err := rdb.FlushDB(pingCtx).Err(); err != nil {
			t.Fatalf("FlushDB: %v", err)
		}
	}

	// index → DB 13 (authoritative), sample cache → DB 14 (same server, separate DB).
	c := New("test", idx, memResolver(), WithSampleCacheRedis(cache))

	stop := index.TimeSeqID{Timestamp: 1700000100, SeqID: 500}
	list := &ListResult{
		client:  c,
		catalog: "users",
		Entries: []index.DeltaInfo{{TsSeq: stop, Score: stop.Score()}},
	}

	calls := 0
	loader := func(*ListResult) (int, error) { calls++; return 42, nil }
	sampler := NewSampler[int]("daily", loader)

	// First call: cache miss → loader runs once → memo written to the cache tier.
	if got, err := sampler.Sample(ctx, list); err != nil {
		t.Fatalf("Sample #1: %v", err)
	} else if got != 42 {
		t.Fatalf("Sample #1 value: got %d, want 42", got)
	}
	if calls != 1 {
		t.Fatalf("loader calls after miss: got %d, want 1", calls)
	}

	key := c.reader.MakeSampleIndicatorKey("daily")

	// The memo MUST be on the cache tier (DB 14)...
	if n, err := cache.Exists(ctx, key).Result(); err != nil || n != 1 {
		t.Fatalf("memo key %q on cache tier (DB14): exists=%d err=%v, want exists=1", key, n, err)
	}
	if v, err := cache.HGet(ctx, key, "users").Result(); err != nil || v == "" {
		t.Fatalf("memo field users on DB14: %q err=%v, want non-empty", v, err)
	}
	// ...and MUST NOT have leaked onto the authoritative index tier (DB 13).
	if n, err := idx.Exists(ctx, key).Result(); err != nil || n != 0 {
		t.Fatalf("memo key %q must be absent on index tier (DB13): exists=%d err=%v, want exists=0", key, n, err)
	}

	// Second call: cache hit served from DB 14 → loader NOT invoked again.
	if got, err := sampler.Sample(ctx, list); err != nil {
		t.Fatalf("Sample #2: %v", err)
	} else if got != 42 {
		t.Fatalf("Sample #2 value: got %d, want 42", got)
	}
	if calls != 1 {
		t.Fatalf("loader calls after cache hit: got %d, want 1 (served from DB14)", calls)
	}
}
