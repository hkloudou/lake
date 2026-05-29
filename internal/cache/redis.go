package cache

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

// NewRedisCacheWithURL builds a RedisCache from a URL.
func NewRedisCacheWithURL(metaUrl string, ttl time.Duration) (*RedisCache, error) {
	opt, err := redis.ParseURL(metaUrl)
	if err != nil {
		return nil, err
	}
	return NewRedisCache(redis.NewClient(opt), ttl), nil
}

func (c *RedisCache) Take(ctx context.Context, namespace, key string, loader func() ([]byte, error)) ([]byte, error) {
	cacheKey := "lake_cache:" + encode.EncodeRedisCatalogName(namespace+":"+key)
	return c.flight.Do(cacheKey, func() ([]byte, error) {
		raw, err := c.client.GetEx(ctx, cacheKey, c.ttl).Bytes()
		switch err {
		case nil:
			// A value that fails to decompress (foreign / legacy format) is
			// treated as a miss and recomputed below.
			if data, derr := gunzip(raw); derr == nil {
				return data, nil
			}
		case redis.Nil:
			// miss → fall through to loader
		default:
			// Redis error → serve from loader without caching.
			return loader()
		}

		data, err := loader()
		if err != nil {
			return nil, err
		}
		if enc, gerr := gzipCompress(data); gerr == nil {
			if err := c.client.Set(ctx, cacheKey, enc, c.ttl).Err(); err != nil {
				log.Printf("[lake cache] set %s: %v", cacheKey, err)
			}
		}
		return data, nil
	})
}
