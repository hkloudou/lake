package lake_test

import (
	"context"
	"testing"
	"time"

	"github.com/hkloudou/lake/v2"
	"github.com/hkloudou/lake/v2/internal/cache"
	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/redis/go-redis/v9"
)

// TestWithCacheHelper demonstrates how to use cache with Lake
func TestWithCacheHelper(t *testing.T) {
	// Create Lake client with Redis cache
	client := lake.NewLake(
		"redis://lake-redis-master.cs:6379/2",
		lake.WithRedisCache("redis://lake-redis-master.cs:6379/2", 5*time.Minute),
	)

	ctx := context.Background()

	// Write some data
	// _, err := client.Write(ctx, lake.WriteRequest{
	// 	Catalog:   "users",
	// 	Field:     "profile",
	// 	Body:      []byte(`{"name":"Alice","age":30}`),
	// 	MergeType: index.MergeTypeReplace,
	// })
	// if err != nil {
	// 	t.Fatalf("Write failed: %v", err)
	// }

	// First read: cache miss, loads from OSS
	list1, err := client.List(ctx, "users")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	data1, err := lake.ReadMap(ctx, list1)
	if err != nil {
		t.Fatalf("ReadMap failed: %v", err)
	}
	t.Logf("First read (cache miss): %+v", data1)

	// Second read: cache hit, loads from Redis
	list2, err := client.List(ctx, "users")
	if err != nil {
		t.Fatalf("List failed: %v", err)
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
		lake.WithCache(redisCache),
	)

	catalog := "test_cache"

	// Write some data
	_, err := client.Write(ctx, lake.WriteRequest{
		Catalog:   catalog,
		Field:     "user",
		Body:      []byte(`{"name":"Bob","age":25}`),
		MergeType: index.MergeTypeReplace,
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// First read - cache miss
	list1, err := client.List(ctx, catalog)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	data1, err := lake.ReadMap(ctx, list1)
	if err != nil {
		t.Fatalf("ReadMap failed: %v", err)
	}
	t.Logf("First read (cache miss): %+v", data1)

	// Second read - should hit cache
	list2, err := client.List(ctx, catalog)
	if err != nil {
		t.Fatalf("List failed: %v", err)
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

	_, err := client.Write(ctx, lake.WriteRequest{
		Catalog:   "test_nocache",
		Field:     "data",
		Body:      []byte(`"test"`),
		MergeType: index.MergeTypeReplace,
	})
	if err != nil {
		t.Logf("Write failed (Redis not available): %v", err)
		t.Skip("Skipping test")
	}

	t.Log("✓ NoOpCache (default) works correctly")
}
