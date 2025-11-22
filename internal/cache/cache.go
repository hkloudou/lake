package cache

// Cache is a cache interface for []byte data
type Cache interface {
	// Take tries to get value from cache by key.
	// If cache miss, calls loader function to load data, cache it, and return.
	Take(key string, loader func() ([]byte, error)) ([]byte, error)
}

// NoOpCache is a cache implementation that always calls the loader (no caching)
type NoOpCache struct{}

// NewNoOpCache creates a new no-op cache
func NewNoOpCache() *NoOpCache {
	return &NoOpCache{}
}

// Take always calls the loader (no caching)
func (c *NoOpCache) Take(key string, loader func() ([]byte, error)) ([]byte, error) {
	return loader()
}

