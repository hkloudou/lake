package cache

import (
	"context"
	"encoding/base64"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache implements Cache interface using Redis
type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
	debug  bool
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
func (c *RedisCache) Take(namespace, key string, loader func() ([]byte, error)) ([]byte, error) {
	ctx := context.Background()
	// Build cache key with namespace to avoid conflicts
	// Format: lake_cache:{namespace}:{key}
	// Example: lake_cache:oss:mylake:users/snap/1700000000_1~1700000100_500.snap
	cacheKey := "lake_cache:" + base64.URLEncoding.EncodeToString([]byte(namespace+":"+key))

	if c.debug {
		log.Printf("[Cache] Take: namespace=%s, key=%s, cacheKey=%s", namespace, key, cacheKey)
	}

	// Try to get from Redis
	cachedData, err := c.client.GetEx(ctx, cacheKey, c.ttl).Result()
	if err == redis.Nil {
		// Cache miss
		c.stat.IncrementMiss()
		if c.debug {
			log.Printf("[Cache] MISS: %s (loading from storage)", cacheKey)
		}

		// Call loader function to get []byte
		data, err := loader()
		if err != nil {
			if c.debug {
				log.Printf("[Cache] Loader failed for %s: %v", cacheKey, err)
			}
			return nil, err
		}

		if c.debug {
			log.Printf("[Cache] Loaded %d bytes from storage for %s", len(data), cacheKey)
		}

		// Write to Redis with TTL (data is already []byte, no need to marshal)
		err = c.client.Set(ctx, cacheKey, data, c.ttl).Err()
		if err != nil {
			if c.debug {
				log.Printf("[Cache] Failed to cache %s: %v (continuing with data)", cacheKey, err)
			}
		} else {
			if c.debug {
				log.Printf("[Cache] Cached %d bytes for %s (TTL: %v)", len(data), cacheKey, c.ttl)
			}
		}

		return data, nil
	} else if err != nil {
		// Redis error, fallback to loader
		if c.debug {
			log.Printf("[Cache] Redis error for %s: %v (fallback to loader)", cacheKey, err)
		}
		return loader()
	}

	// Cache hit
	c.stat.IncrementHit()
	if c.debug {
		log.Printf("[Cache] HIT: %s (%d bytes)", cacheKey, len(cachedData))
	}

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
