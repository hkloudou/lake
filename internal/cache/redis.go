package cache

import (
	"context"
	"encoding/json"
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
func (c *RedisCache) Take(key string, loader func() (any, error)) (any, error) {
	// Only cache .snap files
	if !strings.HasSuffix(key, ".snap") {
		return loader()
	}

	ctx := context.Background()
	cacheKey := "lake_cache:" + key

	// Try to get from Redis
	data, err := c.client.GetEx(ctx, cacheKey, c.ttl).Result()
	if err == redis.Nil {
		// Cache miss
		c.stat.IncrementMiss()

		// Call loader function
		obj, err := loader()
		if err != nil {
			return nil, err
		}

		// Serialize to JSON
		jsonData, err := json.Marshal(obj)
		if err != nil {
			return nil, err
		}

		// Write to Redis with TTL
		err = c.client.Set(ctx, cacheKey, jsonData, c.ttl).Err()
		if err != nil {
			// Log error but don't fail (cache is optional)
			// Continue with loaded data
		}

		return obj, nil
	} else if err != nil {
		// Redis error, fallback to loader
		return loader()
	}

	// Cache hit
	c.stat.IncrementHit()

	// Deserialize JSON
	var obj any
	err = json.Unmarshal([]byte(data), &obj)
	if err != nil {
		// Deserialization error, fallback to loader
		c.stat.IncrementMiss()
		return loader()
	}

	return obj, nil
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
