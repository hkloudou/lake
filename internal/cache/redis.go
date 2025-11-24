package cache

import (
	"context"
	"log"
	"time"

	"github.com/hkloudou/lake/v2/internal/encode"
	"github.com/hkloudou/lake/v2/internal/trace"
	"github.com/hkloudou/lake/v2/internal/xsync"
	"github.com/redis/go-redis/v9"
)

// RedisCache implements Cache interface using Redis  
type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
	stat   *CacheStat
	flight xsync.SingleFlight[[]byte]
}

// NewRedisCache creates a new Redis cache instance
func NewRedisCache(client *redis.Client, ttl time.Duration) *RedisCache {
	return &RedisCache{
		client: client,
		ttl:    ttl,
		flight: xsync.NewSingleFlight[[]byte](),
		stat: NewCacheStat("lake", func() int {
			return countKeys(client, "lake_cache:*")
		}),
	}
}

// NewRedisCacheWithURL creates Redis cache from URL
func NewRedisCacheWithURL(metaUrl string, ttl time.Duration) (*RedisCache, error) {
	redisOpt, err := redis.ParseURL(metaUrl)
	if err != nil {
		return nil, err
	}
	return NewRedisCache(redis.NewClient(redisOpt), ttl), nil
}

// Take implements Cache interface with SingleFlight to prevent cache stampede
func (c *RedisCache) Take(ctx context.Context, namespace, key string, loader func() ([]byte, error)) ([]byte, error) {
	tr := trace.FromContext(ctx)
	cacheKey := "lake_cache:" + encode.EncodeRedisCatalogName(namespace+":"+key)

	// Use SingleFlight to prevent multiple concurrent requests for same key
	return c.flight.Do(cacheKey, func() ([]byte, error) {

		// Try to get from Redis
		cachedData, err := c.client.GetEx(ctx, cacheKey, c.ttl).Result()
		if err == redis.Nil {
			// Cache miss
			c.stat.IncrementMiss()
			tr.RecordSpan("RedisCache.Miss")

			// Call loader function to get []byte
			data, err := loader()
			if err != nil {
				tr.RecordSpan("RedisCache.LoaderFailed", map[string]any{
					"error": err.Error(),
				})
				return nil, err
			}

			tr.RecordSpan("RedisCache.Loaded", map[string]any{
				"size": len(data),
			})

			// Write to Redis with TTL (data is already []byte, no need to marshal)
			err = c.client.Set(ctx, cacheKey, data, c.ttl).Err()
			if err != nil {
			} else {
			}

			return data, nil
		} else if err != nil {
			// Redis error, fallback to loader
			return loader()
		}

		// Cache hit
		c.stat.IncrementHit()
		tr.RecordSpan("RedisCache.Hit", map[string]any{
			"key":  cacheKey,
			"size": len(cachedData),
		})

		// Return cached data as []byte
		return []byte(cachedData), nil
	})
}

// countKeys counts keys matching a pattern
func countKeys(client *redis.Client, pattern string) int {
	ctx := context.Background()
	var cursor uint64
	var count int

	for {
		keys, newCursor, err := client.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return 0
		}
		count += len(keys)
		cursor = newCursor
		if cursor == 0 {
			break
		}
	}

	return count
}
