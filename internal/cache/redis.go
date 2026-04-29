package cache

import (
	"context"
	"log"
	"time"

	"github.com/hkloudou/lake/v3/internal/encode"
	"github.com/hkloudou/lake/v3/internal/encrypt"
	"github.com/hkloudou/lake/v3/internal/xsync"
	"github.com/redis/go-redis/v9"
)

// RedisCache is a Redis-backed Cache. Implements Close to stop its
// stat logger and (when applicable) release its owned redis.Client.
type RedisCache struct {
	client     *redis.Client
	ttl        time.Duration
	stat       *CacheStat
	flight     xsync.SingleFlight[[]byte]
	ownsClient bool
}

func NewRedisCache(client *redis.Client, ttl time.Duration) *RedisCache {
	return &RedisCache{
		client: client,
		ttl:    ttl,
		flight: xsync.NewSingleFlight[[]byte](),
		stat:   NewCacheStat("lake", func() int { return countKeys(client, "lake_cache:*") }),
	}
}

// NewRedisCacheWithURL builds a RedisCache from a URL; the returned
// cache owns the underlying redis.Client and closes it on Close.
func NewRedisCacheWithURL(metaUrl string, ttl time.Duration) (*RedisCache, error) {
	opt, err := redis.ParseURL(metaUrl)
	if err != nil {
		return nil, err
	}
	c := NewRedisCache(redis.NewClient(opt), ttl)
	c.ownsClient = true
	return c, nil
}

// Close stops the stat logger; if the redis.Client is owned, closes it too.
func (c *RedisCache) Close() error {
	if c.stat != nil {
		c.stat.Close()
	}
	if c.ownsClient {
		return c.client.Close()
	}
	return nil
}

func (c *RedisCache) Take(ctx context.Context, namespace, key string, loader func() ([]byte, error)) ([]byte, error) {
	cacheKey := "lake_cache:" + encode.EncodeRedisCatalogName(namespace+":"+key)
	return c.flight.Do(cacheKey, func() ([]byte, error) {
		raw, err := c.client.GetEx(ctx, cacheKey, c.ttl).Bytes()
		switch err {
		case nil:
			c.stat.IncrementHit()
			return encrypt.Decrypt(raw, []byte("lake"))
		case redis.Nil:
			c.stat.IncrementMiss()
			data, err := loader()
			if err != nil {
				return nil, err
			}
			enc, err := encrypt.Encrypt(data, []byte("lake"))
			if err != nil {
				return nil, err
			}
			if err := c.client.Set(ctx, cacheKey, enc, c.ttl).Err(); err != nil {
				log.Printf("[Lake Cache] set %s: %v", cacheKey, err)
			}
			return data, nil
		default:
			// Redis error → fall back to loader without caching.
			return loader()
		}
	})
}

// countKeys SCANs Redis for keys matching pattern (best-effort; returns 0 on error).
func countKeys(client *redis.Client, pattern string) int {
	ctx := context.Background()
	var (
		cursor uint64
		count  int
	)
	for {
		keys, next, err := client.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return 0
		}
		count += len(keys)
		cursor = next
		if cursor == 0 {
			return count
		}
	}
}
