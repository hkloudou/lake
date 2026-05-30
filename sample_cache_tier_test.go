package lake

import (
	"context"
	"testing"
	"time"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/storage/cached"
	"github.com/hkloudou/lake/v3/storage/mem"
)

// TestCacheTier_Redis models the intended split deployment and pins the eviction
// characteristics that decide whether an allkeys-lru cache tier can bound itself:
//
//   - index Redis (DB 13): authoritative, must persist.
//   - cache Redis (DB 14): the snap/delta byte cache (storage/cached) AND the
//     sample memo (WithSampleCacheRedis) — both rebuildable.
//
// The two caches behave differently under allkeys-lru, and the test asserts it:
//   - snap byte cache: one STRING per object WITH a TTL → TTL > 0, so it expires
//     and evicts per key (LRU-friendly, self-bounding).
//   - sample memo: one HASH per indicator with NO key TTL → TTL == -1. A hot hash
//     resists LRU and grows one field per catalog forever; bound it another way.
//
// Non-destructive: a unique prefix + targeted cleanup, never FlushDB.
func TestCacheTier_Redis(t *testing.T) {
	idx := redisTestDB(t, 13)   // index tier
	cache := redisTestDB(t, 14) // cache tier
	prefix := testPrefix(t)
	snapNS := prefix + "|snaps" // cached.Wrap namespace → keys are lake_cache:<snapNS>:*
	cleanupKeys(t, idx, prefix+":*", "lake_cache:"+snapNS+":*")
	cleanupKeys(t, cache, prefix+":*", "lake_cache:"+snapNS+":*")

	ctx := context.Background()

	// Snap byte cache → cache tier (DB 14): a snapshot save is a Put through the
	// storage/cached wrapper, whose write-through warms the tier with a TTL'd
	// string keyed by the object path.
	store := mem.New()
	wrapped := cached.Wrap(snapNS, store.Bucket("snaps"), cached.NewRedisCache(cache, time.Hour))
	snapPath := "ab/cd/100.snap"
	if err := wrapped.Put(ctx, "users", snapPath, []byte(`{"a":1}`)); err != nil {
		t.Fatalf("snap Put: %v", err)
	}

	// Sample memo → cache tier (DB 14) via WithSampleCacheRedis.
	c := New(prefix, idx, memResolver(), WithSampleCacheRedis(cache))
	stop := index.TimeSeqID{Timestamp: 1700000100, SeqID: 500}
	list := &ListResult{client: c, catalog: "users", Entries: []index.DeltaInfo{{TsSeq: stop, Score: stop.Score()}}}
	if _, err := NewSampler[int]("daily", func(*ListResult) (int, error) { return 42, nil }).Sample(ctx, list); err != nil {
		t.Fatalf("Sample: %v", err)
	}

	snapKey := "lake_cache:" + snapNS + ":" + snapPath
	memoKey := c.reader.MakeSampleIndicatorKey("daily")

	// Both land on the cache tier (DB 14)...
	if n, _ := cache.Exists(ctx, snapKey).Result(); n != 1 {
		t.Fatalf("snap byte cache missing on cache tier (DB14): %s", snapKey)
	}
	if n, _ := cache.Exists(ctx, memoKey).Result(); n != 1 {
		t.Fatalf("sample memo missing on cache tier (DB14): %s", memoKey)
	}
	// ...and never leak onto the authoritative index tier (DB 13).
	if n, _ := idx.Exists(ctx, snapKey, memoKey).Result(); n != 0 {
		t.Fatalf("cache data leaked onto the index tier (DB13): %d of 2 keys present", n)
	}

	// The eviction contract that makes (or breaks) an allkeys-lru tier:
	// snap bytes carry a per-key TTL; the sample memo hash has none.
	if ttl, _ := cache.TTL(ctx, snapKey).Result(); ttl <= 0 {
		t.Errorf("snap byte cache TTL = %v, want > 0 (per-key expiry, LRU-friendly)", ttl)
	}
	if ttl, _ := cache.TTL(ctx, memoKey).Result(); ttl >= 0 {
		t.Errorf("sample memo TTL = %v, want < 0 (no expiry — resists LRU, grows unbounded)", ttl)
	}
}
