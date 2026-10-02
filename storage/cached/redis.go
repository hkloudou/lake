package cached

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// RedisCache is a Redis-backed Cache: one TTL'd string per object, so an
// allkeys-lru cache tier evicts per key. It holds only rebuildable bytes.
type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
	flight singleflight.Group
}

func NewRedisCache(client *redis.Client, ttl time.Duration) *RedisCache {
	return &RedisCache{client: client, ttl: ttl}
}

func (c *RedisCache) cacheKey(namespace, key string) string {
	return "lake_cache:" + namespace + ":" + key
}

// Take is read-through. A hit is served directly (GetEx also slides the
// TTL); a miss — or a cache-Redis error — runs the loader once per key across
// concurrent callers and stores the result best-effort. Every caller gets a
// private slice: Lake lets callers mutate the documents built from it.
func (c *RedisCache) Take(ctx context.Context, namespace, key string, loader func() ([]byte, error)) ([]byte, error) {
	cacheKey := c.cacheKey(namespace, key)
	if data, err := c.client.GetEx(ctx, cacheKey, c.ttl).Bytes(); err == nil {
		return data, nil
	}
	v, err, _ := c.flight.Do(cacheKey, func() (any, error) {
		data, err := loader()
		if err == nil {
			c.write(ctx, cacheKey, data)
		}
		return data, err
	})
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), v.([]byte)...), nil
}

// Set writes data through to the cache (write-through warming).
func (c *RedisCache) Set(ctx context.Context, namespace, key string, data []byte) error {
	c.write(ctx, c.cacheKey(namespace, key), data)
	return nil
}

// write stores best-effort: a cache-write failure is logged, never surfaced.
func (c *RedisCache) write(ctx context.Context, cacheKey string, data []byte) {
	if err := c.client.Set(ctx, cacheKey, data, c.ttl).Err(); err != nil {
		log.Printf("[lake cache] set %s: %v", cacheKey, err)
	}
}
