package cache

import "context"

// Cache is a cache interface for []byte data
type Cache interface {
	// Take tries to get value from cache by key.
	// ctx: context for cancellation and tracing
	// namespace: cache namespace to avoid key conflicts (e.g., storage prefix like "oss:mylake")
	// key: cache key (e.g., "users/snap/1700000000_1~1700000100_500.snap")
	// If cache miss, calls loader function to load data, cache it, and return.
	// Uses SingleFlight internally to prevent cache stampede.
	Take(ctx context.Context, namespace, key string, loader func() ([]byte, error)) ([]byte, error)
}

// NoOpCache is a cache implementation that always calls the loader (no caching)
type NoOpCache struct{}

// NewNoOpCache creates a new no-op cache
func NewNoOpCache() *NoOpCache {
	return &NoOpCache{}
}

// Take always calls the loader (no caching)
func (c *NoOpCache) Take(ctx context.Context, namespace, key string, loader func() ([]byte, error)) ([]byte, error) {
	return loader()
}
