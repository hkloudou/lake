package lake

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache 是一个通用的缓存接口，包含 Take 方法
type Cache interface {
	// Take 尝试从缓存中获取 key 对应的数据。如果缓存中不存在，则调用 loader 函数加载数据，
	// 将其缓存，并返回数据。
	Take(key string, loader func() (any, error)) (any, error)
}

// RedisCache 是 Redis 缓存的实现
type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
}

// NewRedisCache 创建一个新的 RedisCache 实例
func NewRedisCache(client *redis.Client, ttl time.Duration) *RedisCache {

	return &RedisCache{
		client: client,
		ttl:    ttl,
	}
}

// Take 实现 Cache 接口的 Take 方法
func (c *RedisCache) Take(key string, loader func() (any, error)) (any, error) {
	ctx := context.Background()

	key = "lake_cache:" + key

	// 尝试从 Redis 获取数据
	data, err := c.client.GetEx(ctx, key, c.ttl).Result()
	if err == redis.Nil {
		// 缓存未命中，调用 loader 函数加载数据
		obj, err := loader()
		if err != nil {
			return nil, err
		}

		// 序列化数据为 JSON
		jsonData, err := json.Marshal(obj)
		if err != nil {
			return nil, err
		}

		// 将数据写入 Redis，设置 TTL
		err = c.client.Set(ctx, key, jsonData, c.ttl).Err()
		if err != nil {
			return nil, err
		}

		return obj, nil
	} else if err != nil {
		// 其他 Redis 错误
		return nil, err
	}

	// 缓存命中，反序列化 JSON 数据
	var obj any
	err = json.Unmarshal([]byte(data), &obj)
	if err != nil {
		return nil, err
	}

	return obj, nil
}
