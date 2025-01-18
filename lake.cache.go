package lake

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hkloudou/xlib/xlog"
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
	state  *cacheStat
}

// NewRedisCache 创建一个新的 RedisCache 实例
func NewRedisCache(client *redis.Client, ttl time.Duration) *RedisCache {
	// collection.NewCache()
	return &RedisCache{
		client: client,
		ttl:    ttl,
		state: newCacheStat("lake", func() int {
			prefix := "lake_cache"
			ctx := context.Background()
			var cursor uint64
			var count int
			for {
				// 使用 SCAN 命令遍历键
				keys, newCursor, err := client.Scan(ctx, cursor, prefix+"*", 1000).Result()
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

		}),
	}
}

// Take 实现 Cache 接口的 Take 方法
func (c *RedisCache) Take(key string, loader func() (any, error)) (any, error) {
	if !strings.HasSuffix(key, ".snap") {
		return loader()
	}
	ctx := context.Background()

	key = "lake_cache:" + key

	// 尝试从 Redis 获取数据
	data, err := c.client.GetEx(ctx, key, c.ttl).Result()
	if err == redis.Nil {
		c.state.IncrementMiss()
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
	} else {
		c.state.IncrementHit()
	}

	// 缓存命中，反序列化 JSON 数据
	var obj any
	err = json.Unmarshal([]byte(data), &obj)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

type cacheStat struct {
	name         string
	hit          uint64
	miss         uint64
	sizeCallback func() int
}

func newCacheStat(name string, sizeCallback func() int) *cacheStat {
	st := &cacheStat{
		name:         name,
		sizeCallback: sizeCallback,
	}
	go st.statLoop()
	return st
}

func (cs *cacheStat) IncrementHit() {
	atomic.AddUint64(&cs.hit, 1)
}

func (cs *cacheStat) IncrementMiss() {
	atomic.AddUint64(&cs.miss, 1)
}

func (cs *cacheStat) statLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		hit := atomic.SwapUint64(&cs.hit, 0)
		miss := atomic.SwapUint64(&cs.miss, 0)
		total := hit + miss
		if total == 0 {
			continue
		}
		percent := 100 * float32(hit) / float32(total)
		xlog.Statf("cache(%s) - qpm: %d, hit_ratio: %.1f%%, elements: %d, hit: %d, miss: %d",
			cs.name, total, percent, cs.sizeCallback(), hit, miss)
	}
}
