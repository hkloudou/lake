package cached

import (
	"context"
	"log"
	"time"

	"github.com/hkloudou/lake/v3/internal/encode"
	"github.com/hkloudou/lake/v3/internal/xsync"
	"github.com/redis/go-redis/v9"
)

// RedisCache is a Redis-backed Cache. Values are gzip-compressed (a space
// optimization, not encryption — the cache holds only rebuildable data).
type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
	flight xsync.SingleFlight[[]byte]
}

func NewRedisCache(client *redis.Client, ttl time.Duration) *RedisCache {
	return &RedisCache{
		client: client,
		ttl:    ttl,
		flight: xsync.NewSingleFlight[[]byte](),
	}
}

// NewRedisCacheWithURL builds a RedisCache from a Redis URL.
func NewRedisCacheWithURL(url string, ttl time.Duration) (*RedisCache, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return NewRedisCache(redis.NewClient(opt), ttl), nil
}

func (c *RedisCache) cacheKey(namespace, key string) string {
	return "lake_cache:" + encode.EncodeRedisCatalogName(namespace+":"+key)
}

// Take is read-through. Hits are served OUTSIDE the single-flight —
// concurrent readers of a hot key each pay their own GetEx (parallel, no
// head-of-line blocking) and gunzip already hands each of them a private
// buffer. Everything that must call the loader — a miss, an undecodable
// value, and a cache-Redis ERROR alike — funnels through the flight, so a
// cold or cache-degraded hot object hits the backend once per cohort, not
// once per caller (an outage must not turn into a backend stampede).
func (c *RedisCache) Take(ctx context.Context, namespace, key string, loader func() ([]byte, error)) ([]byte, error) {
	cacheKey := c.cacheKey(namespace, key)
	raw, err := c.client.GetEx(ctx, cacheKey, c.ttl).Bytes()
	if err == nil {
		// A value that fails to decompress (foreign / legacy format) is
		// treated as a miss and recomputed below.
		if data, derr := gunzip(raw); derr == nil {
			return data, nil
		}
	}
	cacheDown := err != nil && err != redis.Nil

	return takeWithRetry(ctx, c.flight, cacheKey, func(retry bool) ([]byte, error) {
		// Re-check only on the retry pass: this caller just observed the
		// miss itself, but a retry follows someone else's flight and the
		// value may have landed meanwhile. Skipped when the cache Redis is
		// erroring — the loader result is then served WITHOUT caching.
		if retry && !cacheDown {
			if raw, gerr := c.client.GetEx(ctx, cacheKey, c.ttl).Bytes(); gerr == nil {
				if data, derr := gunzip(raw); derr == nil {
					return data, nil
				}
			}
		}
		data, lerr := loader()
		if lerr != nil {
			return nil, lerr
		}
		if !cacheDown {
			c.write(ctx, cacheKey, data)
		}
		return data, nil
	})
}

// Set writes data through to the cache (write-through warming).
func (c *RedisCache) Set(ctx context.Context, namespace, key string, data []byte) error {
	c.write(ctx, c.cacheKey(namespace, key), data)
	return nil
}

// write gzips and stores best-effort: a cache-write failure is logged, never
// surfaced — the cache holds only rebuildable data.
func (c *RedisCache) write(ctx context.Context, cacheKey string, data []byte) {
	enc, gerr := gzipCompress(data)
	if gerr != nil {
		log.Printf("[lake cache] gzip %s: %v", cacheKey, gerr)
		return
	}
	if err := c.client.Set(ctx, cacheKey, enc, c.ttl).Err(); err != nil {
		log.Printf("[lake cache] set %s: %v", cacheKey, err)
	}
}
