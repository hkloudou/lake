package cached

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisCacheForTest returns a RedisCache on the local test Redis (db 14, the
// cache tier), or skips. Keys are prefixed per test and deleted on cleanup.
func redisCacheForTest(t *testing.T) (*RedisCache, string) {
	t.Helper()
	addr := os.Getenv("LAKE_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 14, DialTimeout: 200 * time.Millisecond, MaxRetries: -1, DialerRetries: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("redis not reachable, skipping integration test: %v", err)
	}
	ns := fmt.Sprintf("cachetest_%d_%s", os.Getpid(), t.Name())
	t.Cleanup(func() {
		c := context.Background()
		keys, _ := rdb.Keys(c, "lake_cache:"+ns+":*").Result()
		if len(keys) > 0 {
			rdb.Del(c, keys...)
		}
		_ = rdb.Close()
	})
	return NewRedisCache(rdb, time.Minute), ns
}

// TestRedisCacheConcurrentTakeCopies: concurrent Take callers of the SAME key
// run the loader once and each receive a private slice — Lake's read path
// lets callers mutate the merged document, which for a fully-snapshotted
// catalog IS the slice Take returned.
func TestRedisCacheConcurrentTakeCopies(t *testing.T) {
	c, ns := redisCacheForTest(t)
	ctx := context.Background()

	const n = 8
	var loads atomic.Int32
	gate := make(chan struct{})
	results := make([][]byte, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Go(func() {
			<-gate
			v, err := c.Take(ctx, ns, "k", func() ([]byte, error) {
				loads.Add(1)
				time.Sleep(30 * time.Millisecond) // widen the flight window
				return []byte(`{"doc":1}`), nil
			})
			if err != nil {
				t.Error(err)
				return
			}
			results[i] = v
		})
	}
	close(gate)
	wg.Wait()

	if loads.Load() != 1 {
		t.Fatalf("loader ran %d times, want 1 (single-flight)", loads.Load())
	}
	for i, v := range results {
		if v == nil {
			t.Fatalf("result %d missing", i)
		}
		for j := i + 1; j < n; j++ {
			if results[j] != nil && &v[0] == &results[j][0] {
				t.Fatalf("results %d and %d share a backing array", i, j)
			}
		}
	}
	results[0][0] = 'X'
	v, err := c.Take(ctx, ns, "k", func() ([]byte, error) { return nil, fmt.Errorf("loader must not run on a hit") })
	if err != nil || !bytes.Equal(v, []byte(`{"doc":1}`)) {
		t.Fatalf("cache content corrupted by caller mutation: %q err=%v", v, err)
	}
}

// TestRedisCacheHitDoesNotSerialize: a hit completes while an unrelated
// key's loader is stuck — hits never enter the single-flight.
func TestRedisCacheHitDoesNotSerialize(t *testing.T) {
	c, ns := redisCacheForTest(t)
	ctx := context.Background()
	if err := c.Set(ctx, ns, "hot", []byte("v")); err != nil {
		t.Fatal(err)
	}
	stuck := make(chan struct{})
	go c.Take(ctx, ns, "cold", func() ([]byte, error) { <-stuck; return []byte("cold"), nil })
	defer close(stuck)

	done := make(chan struct{})
	go func() {
		if v, err := c.Take(ctx, ns, "hot", func() ([]byte, error) { return nil, fmt.Errorf("loader ran for a cached key") }); err != nil || string(v) != "v" {
			t.Errorf("hit = %q, %v", v, err)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("hit blocked behind an unrelated in-flight miss")
	}
}
