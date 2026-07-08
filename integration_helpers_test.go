package lake

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Integration tests talk to a developer's real Redis — 127.0.0.1:6379 unless
// LAKE_TEST_REDIS_ADDR points elsewhere (devcontainers, docker-compose hosts).
// To stay non-destructive they NEVER FlushDB: each test uses a unique key
// prefix (testPrefix) and registers cleanupKeys to delete only its own keys.
// Whatever else lives in that logical DB is left untouched.

var testRedisAddr = testRedisAddrFromEnv()

func testRedisAddrFromEnv() string {
	if a := os.Getenv("LAKE_TEST_REDIS_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:6379"
}

// redisTestDB returns a client to the given logical DB, or skips when Redis is
// unreachable. It registers Close on cleanup but never flushes the DB.
// MaxRetries -1 / DialerRetries 1 disable go-redis's command and dial retry
// loops so the skip probe fails fast when Redis is absent (with defaults,
// every probe would burn its full 500ms context across ~19 call sites).
func redisTestDB(t testing.TB, db int) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr, DB: db, DialTimeout: 200 * time.Millisecond, MaxRetries: -1, DialerRetries: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("redis not reachable, skipping integration test: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// testPrefix is a Redis key prefix unique to this test and process, so parallel
// runs and leftover keys from a crashed run never collide.
func testPrefix(t testing.TB) string {
	return fmt.Sprintf("laketest_%d_%s", os.Getpid(), strings.ReplaceAll(t.Name(), "/", "_"))
}

// waitFor polls cond up to ~2s, returning true once it holds. Use it to drain a
// fire-and-forget goroutine (e.g. the async snapshot save a Read triggers)
// before the test's cleanup runs, so no key is written after cleanup.
func waitFor(cond func() bool) bool {
	for i := 0; i < 100; i++ {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// cleanupKeys registers a t.Cleanup that SCAN+DELs every key matching each
// pattern on rdb — targeted, non-destructive (no FlushDB).
func cleanupKeys(t testing.TB, rdb *redis.Client, patterns ...string) {
	t.Cleanup(func() {
		ctx := context.Background()
		for _, pat := range patterns {
			var cursor uint64
			for {
				keys, next, err := rdb.Scan(ctx, cursor, pat, 256).Result()
				if err != nil {
					break
				}
				if len(keys) > 0 {
					rdb.Del(ctx, keys...)
				}
				if next == 0 {
					break
				}
				cursor = next
			}
		}
	})
}
