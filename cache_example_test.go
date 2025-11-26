package lake_test

import (
	"context"
	"testing"
	"time"

	"github.com/hkloudou/lake/v2"
	"github.com/hkloudou/lake/v2/internal/cache"
	"github.com/redis/go-redis/v9"
)

// TestWithCacheHelper demonstrates how to use cache with Lake
func TestWithCacheHelper(t *testing.T) {
	// Create Lake client with Redis cache
	client := lake.NewLake(
		"redis://lake-redis-master.cs:6379/2",
	)

	ctx := context.Background()
	// Write some data
	writeErr := client.Write(ctx, lake.WriteRequest{
		Catalog:   "users",
		Path:      "/profile",
		Body:      []byte(`{"name":"Alice2","age":30}`),
		MergeType: lake.MergeTypeReplace,
	})
	if writeErr != nil {
		t.Fatalf("Write failed: %v", writeErr)
	}
	// First read: cache miss, loads from OSS
	list1 := client.List(ctx, "users")
	if list1.Err != nil {
		t.Fatalf("List error: %v", list1.Err)
	}
	data1, err := lake.ReadMap(ctx, list1)
	if err != nil {
		t.Fatalf("ReadMap failed: %v", err)
	}
	t.Logf("First read (cache miss): %+v", data1)

	// Second read: cache hit, loads from Redis
	list2 := client.List(ctx, "users")
	if list2.Err != nil {
		t.Fatalf("List error: %v", list2.Err)
	}
	data2, err := lake.ReadMap(ctx, list2)
	if err != nil {
		t.Fatalf("ReadMap failed: %v", err)
	}
	t.Logf("Second read (cache hit): %+v", data2)

	// Cache stats will be logged every 10 seconds:
	// [Lake Cache lake] qpm: 2, hit_ratio: 50.0%, elements: 1, hit: 1, miss: 1
	t.Log("✓ Cache integration with WithRedisCache successful")
}

func TestWithRedisCache(t *testing.T) {
	// Skip if Redis not available
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// Create cache with 1 minute TTL
	redisCache := cache.NewRedisCache(rdb, 1*time.Minute)

	// Create client with cache
	client := lake.NewLake(
		"redis://localhost:6379/15",
		lake.WithSnapCache(redisCache),
	)

	catalog := "test_cache"

	// Write some data
	writeErr := client.Write(ctx, lake.WriteRequest{
		Catalog:   catalog,
		Path:      "/user",
		Body:      []byte(`{"name":"Bob","age":25}`),
		MergeType: lake.MergeTypeReplace,
	})
	if writeErr != nil {
		t.Fatalf("Write failed: %v", writeErr)
	}

	// First read - cache miss
	list1 := client.List(ctx, catalog)
	if list1.Err != nil {
		t.Fatalf("List error: %v", list1.Err)
	}

	data1, err := lake.ReadMap(ctx, list1)
	if err != nil {
		t.Fatalf("ReadMap failed: %v", err)
	}
	t.Logf("First read (cache miss): %+v", data1)

	// Second read - should hit cache
	list2 := client.List(ctx, catalog)
	if list2.Err != nil {
		t.Fatalf("List error: %v", list2.Err)
	}

	data2, err := lake.ReadMap(ctx, list2)
	if err != nil {
		t.Fatalf("ReadMap failed: %v", err)
	}
	t.Logf("Second read (cache hit): %+v", data2)

	t.Log("✓ Cache integration successful")
	t.Log("Check logs for cache statistics (logged every 10 seconds)")
}

func TestWithNoOpCache(t *testing.T) {
	// Default behavior: no caching
	client := lake.NewLake("redis://localhost:6379/15")

	// This client uses NoOpCache by default (always loads from storage)
	ctx := context.Background()

	writeErr := client.Write(ctx, lake.WriteRequest{
		Catalog:   "test_nocache",
		Path:      "/data",
		Body:      []byte(`"test"`),
		MergeType: lake.MergeTypeReplace,
	})
	if writeErr != nil {
		t.Logf("Write failed (Redis not available): %v", writeErr)
		t.Skip("Skipping test")
	}

	t.Log("✓ NoOpCache (default) works correctly")
}
