package cache

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache implements Cache interface using Redis
type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
	stat   *CacheStat
}

// NewRedisCache creates a new Redis cache instance
func NewRedisCache(client *redis.Client, ttl time.Duration) *RedisCache {
	return &RedisCache{
		client: client,
		ttl:    ttl,
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

// Take implements Cache interface
func (c *RedisCache) Take(key string, loader func() ([]byte, error)) ([]byte, error) {
	// Only cache .snap files
	if !strings.HasSuffix(key, ".snap") {
		return loader()
	}

	ctx := context.Background()
	cacheKey := "lake_cache:" + key

	// Try to get from Redis
	cachedData, err := c.client.GetEx(ctx, cacheKey, c.ttl).Result()
	if err == redis.Nil {
		// Cache miss
		c.stat.IncrementMiss()

		// Call loader function to get []byte
		data, err := loader()
		if err != nil {
			return nil, err
		}

		// Write to Redis with TTL (data is already []byte, no need to marshal)
		err = c.client.Set(ctx, cacheKey, data, c.ttl).Err()
		if err != nil {
			// Log error but don't fail (cache is optional)
			// Continue with loaded data
		}

		return data, nil
	} else if err != nil {
		// Redis error, fallback to loader
		return loader()
	}

	// Cache hit
	c.stat.IncrementHit()
	
	// Return cached data as []byte
	return []byte(cachedData), nil
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
