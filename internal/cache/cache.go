package cache

import "context"

// Cache is a []byte cache keyed by (namespace, key) with a single-flight
// loader on miss.
type Cache interface {
	Take(ctx context.Context, namespace, key string, loader func() ([]byte, error)) ([]byte, error)
}

// NoOpCache always invokes the loader (no caching).
type NoOpCache struct{}

func NewNoOpCache() *NoOpCache { return &NoOpCache{} }

func (NoOpCache) Take(_ context.Context, _, _ string, loader func() ([]byte, error)) ([]byte, error) {
	return loader()
}
