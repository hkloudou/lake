package cache

import (
	"context"
	"sync"
	"time"

	"github.com/hkloudou/lake/v2/internal/trace"
	"github.com/hkloudou/lake/v2/internal/xsync"
)

// MemoryCache implements Cache interface using in-memory map with TTL
type MemoryCache struct {
	mu     sync.RWMutex
	data   map[string]*cacheEntry
	ttl    time.Duration
	flight xsync.SingleFlight[[]byte]
}

type cacheEntry struct {
	value      []byte
	expireTime time.Time
}

// NewMemoryCache creates a new memory cache with TTL support
func NewMemoryCache(ttl time.Duration) *MemoryCache {
	c := &MemoryCache{
		data:   make(map[string]*cacheEntry),
		ttl:    ttl,
		flight: xsync.NewSingleFlight[[]byte](),
	}

	// Start cleanup goroutine
	go c.cleanupLoop()

	return c
}

// Take implements Cache interface with SingleFlight
func (c *MemoryCache) Take(ctx context.Context, namespace, key string, loader func() ([]byte, error)) ([]byte, error) {
	tr := trace.FromContext(ctx)
	cacheKey := namespace + ":" + key
	tr.RecordSpan("MemoryCache.Take", map[string]any{
		"namespace": namespace,
		"key":       key,
	})
	// Use SingleFlight to prevent cache stampede
	return c.flight.Do(cacheKey, func() ([]byte, error) {
		// Check cache first
		c.mu.RLock()
		if entry, ok := c.data[cacheKey]; ok {
			if time.Now().Before(entry.expireTime) {
				// Cache hit
				c.mu.RUnlock()
				tr.RecordSpan("MemoryCache.Hit", map[string]any{
					"key":  cacheKey,
					"size": len(entry.value),
				})
				return entry.value, nil
			}
		}
		c.mu.RUnlock()

		tr.RecordSpan("MemoryCache.Miss")

		// Cache miss, call loader
		data, err := loader()
		if err != nil {
			tr.RecordSpan("MemoryCache.LoaderFailed", map[string]any{
				"error": err.Error(),
			})
			return nil, err
		}

		// Store in cache with TTL
		c.mu.Lock()
		c.data[cacheKey] = &cacheEntry{
			value:      data,
			expireTime: time.Now().Add(c.ttl),
		}
		c.mu.Unlock()

		tr.RecordSpan("MemoryCache.Loaded", map[string]any{
			"size": len(data),
		})

		return data, nil
	})
}

// cleanupLoop periodically removes expired entries
func (c *MemoryCache) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.cleanup()
	}
}

// cleanup removes expired entries
func (c *MemoryCache) cleanup() {
	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	for key, entry := range c.data {
		if now.After(entry.expireTime) {
			delete(c.data, key)
		}
	}
}
