package cached

import (
	"context"
	"sync"
	"time"

	"github.com/hkloudou/lake/v3/internal/xsync"
)

// MemoryCache is a process-local TTL cache. The background cleanup
// loop runs until Close (or process exit).
type MemoryCache struct {
	mu        sync.RWMutex
	data      map[string]cacheEntry
	ttl       time.Duration
	flight    xsync.SingleFlight[[]byte]
	done      chan struct{}
	closeOnce sync.Once
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
		done:   make(chan struct{}),
	}
	go c.cleanupLoop()
	return c
}

// Close stops the background cleanup loop. Idempotent. The cache keeps
// working after Close; expired entries are then reclaimed lazily on access.
func (c *MemoryCache) Close() { c.closeOnce.Do(func() { close(c.done) }) }

// Take is read-through with two properties callers rely on:
//
//   - hits never enter the single-flight — concurrent readers of a hot key
//     proceed in parallel instead of queueing behind one leader;
//   - every caller gets a PRIVATE copy (hit, miss leader, and every flight
//     waiter alike). Waiters sharing the leader's slice would alias the
//     buffers Lake hands to callers who are explicitly allowed to mutate
//     their merged document.
func (c *MemoryCache) Take(ctx context.Context, namespace, key string, loader func() ([]byte, error)) ([]byte, error) {
	cacheKey := namespace + ":" + key
	if v, ok := c.lookup(cacheKey); ok {
		return append([]byte(nil), v...), nil
	}
	return takeWithRetry(ctx, c.flight, cacheKey, func(bool) ([]byte, error) {
		// Re-check inside the flight (a map lookup — free, unlike a network
		// probe): a just-finished leader may have filled the entry while
		// this caller was queueing.
		if v, ok := c.lookup(cacheKey); ok {
			return v, nil
		}
		data, err := loader()
		if err != nil {
			return nil, err
		}
		c.store(cacheKey, data)
		return data, nil
	})
}

// lookup returns the stored (shared) slice — callers must copy before
// handing it out.
func (c *MemoryCache) lookup(cacheKey string) ([]byte, bool) {
	c.mu.RLock()
	e, ok := c.data[cacheKey]
	c.mu.RUnlock()
	if ok && time.Now().Before(e.expireTime) {
		return e.value, true
	}
	return nil, false
}

// Set writes data through to the cache (write-through warming).
func (c *MemoryCache) Set(_ context.Context, namespace, key string, data []byte) error {
	c.store(namespace+":"+key, data)
	return nil
}

func (c *MemoryCache) store(cacheKey string, data []byte) {
	cp := append([]byte(nil), data...)
	c.mu.Lock()
	c.data[cacheKey] = cacheEntry{value: cp, expireTime: time.Now().Add(c.ttl)}
	c.mu.Unlock()
}

// cleanupLoop sweeps expired entries every minute until Close.
func (c *MemoryCache) cleanupLoop() {
	t := time.NewTicker(1 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
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
}
