package cache

// Cache is a cache interface for []byte data
type Cache interface {
	// Take tries to get value from cache by key.
	// namespace: cache namespace to avoid key conflicts (e.g., storage prefix like "oss:mylake")
	// key: cache key (e.g., "users/snap/1700000000_1~1700000100_500.snap")
	// If cache miss, calls loader function to load data, cache it, and return.
	// Uses SingleFlight internally to prevent cache stampede.
	Take(namespace, key string, loader func() ([]byte, error)) ([]byte, error)
}

// NoOpCache is a cache implementation that always calls the loader (no caching)
type NoOpCache struct{}

// NewNoOpCache creates a new no-op cache
func NewNoOpCache() *NoOpCache {
	return &NoOpCache{}
}

// Take always calls the loader (no caching)
func (c *NoOpCache) Take(namespace, key string, loader func() ([]byte, error)) ([]byte, error) {
	return loader()
}
