package cache

import (
	"context"
	"sync"
	"time"

	"github.com/hkloudou/lake/v3/internal/xsync"
)

// MemoryCache is a process-local TTL cache. The background cleanup
// loop runs for the process lifetime.
type MemoryCache struct {
	mu     sync.RWMutex
	data   map[string]cacheEntry
	ttl    time.Duration
	flight xsync.SingleFlight[[]byte]
}

type cacheEntry struct {
	value      []byte
	expireTime time.Time
}

func NewMemoryCache(ttl time.Duration) *MemoryCache {
	c := &MemoryCache{
		data:   make(map[string]cacheEntry),
		ttl:    ttl,
		flight: xsync.NewSingleFlight[[]byte](),
	}
	go c.cleanupLoop()
	return c
}

func (c *MemoryCache) Take(ctx context.Context, namespace, key string, loader func() ([]byte, error)) ([]byte, error) {
	cacheKey := namespace + ":" + key
	return c.flight.Do(cacheKey, func() ([]byte, error) {
		c.mu.RLock()
		e, ok := c.data[cacheKey]
		c.mu.RUnlock()
		if ok && time.Now().Before(e.expireTime) {
			return e.value, nil
		}
		data, err := loader()
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		c.data[cacheKey] = cacheEntry{value: data, expireTime: time.Now().Add(c.ttl)}
		c.mu.Unlock()
		return data, nil
	})
}

// cleanupLoop sweeps expired entries every minute, forever.
func (c *MemoryCache) cleanupLoop() {
	t := time.NewTicker(1 * time.Minute)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		c.mu.Lock()
		for k, e := range c.data {
			if now.After(e.expireTime) {
				delete(c.data, k)
			}
		}
		c.mu.Unlock()
	}
}
