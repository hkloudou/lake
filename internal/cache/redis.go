package cache

import (
	"context"
	"time"

	"github.com/hkloudou/lake/v3/internal/encode"
	"github.com/hkloudou/lake/v3/internal/encrypt"
	"github.com/hkloudou/lake/v3/internal/xsync"
	"github.com/redis/go-redis/v9"
)

// RedisCache implements Cache interface using Redis. Implements Close for
// graceful shutdown of its stat logger and (when applicable) its owned
// redis.Client.
type RedisCache struct {
	client     *redis.Client
	ttl        time.Duration
	stat       *CacheStat
	flight     xsync.SingleFlight[[]byte]
	ownsClient bool // true when the redis.Client was created internally and Close should release it
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

// NewRedisCacheWithURL creates Redis cache from URL.
//
// Note: the redis.Client created here is owned by the cache and will be
// closed by Close.
func NewRedisCacheWithURL(metaUrl string, ttl time.Duration) (*RedisCache, error) {
	redisOpt, err := redis.ParseURL(metaUrl)
	if err != nil {
		return nil, err
	}
	c := NewRedisCache(redis.NewClient(redisOpt), ttl)
	c.ownsClient = true
	return c, nil
}

// Close stops the stat logger. If this RedisCache owns its redis.Client
// (i.e., it was created via NewRedisCacheWithURL), the client is also
// closed. Idempotent.
func (c *RedisCache) Close() error {
	if c.stat != nil {
		c.stat.Close()
	}
	if c.ownsClient && c.client != nil {
		return c.client.Close()
	}
	return nil
}

// Take implements Cache interface with SingleFlight to prevent cache stampede
func (c *RedisCache) Take(ctx context.Context, namespace, key string, loader func() ([]byte, error)) ([]byte, error) {
	cacheKey := "lake_cache:" + encode.EncodeRedisCatalogName(namespace+":"+key)

	// Use SingleFlight to prevent multiple concurrent requests for same key
	return c.flight.Do(cacheKey, func() ([]byte, error) {
		// time.Sleep(1 * time.Second) // for flight test to see if it works
		// Try to get from Redis
		cachedData, err := c.client.GetEx(ctx, cacheKey, c.ttl).Bytes()
		if err == redis.Nil {
			// Cache miss
			c.stat.IncrementMiss()

			// Call loader function to get []byte
			data, err := loader()
			if err != nil {
				return nil, err
			}

			// Write to Redis with TTL (data is already []byte, no need to marshal)
			encryptedData, err := encrypt.Encrypt(data, []byte("lake"))
			if err != nil {
				return nil, err
			}
			err = c.client.Set(ctx, cacheKey, encryptedData, c.ttl).Err()
			if err != nil {
			} else {
			}

			return data, nil
		} else if err != nil {
			// Redis error, fallback to loader
			data, err := loader()
			if err != nil {
				return nil, err
			}
			return data, nil
		}

		// Cache hit
		c.stat.IncrementHit()
		decryptedData, err := encrypt.Decrypt([]byte(cachedData), []byte("lake"))
		if err != nil {
			return nil, err
		}

		// Return cached data as []byte
		return decryptedData, nil
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
