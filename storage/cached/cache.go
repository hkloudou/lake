// Package cached is a ready-made caching decorator for any storage.Storage,
// composed in a storage.Resolver (alongside storage/oss, storage/file,
// storage/mem). Lake core is cache-agnostic: it only ever calls Get/Put on the
// Storage the Resolver returns, so caching is entirely an embedder concern wired
// here.
//
// Wrap adds read-through caching on Get and write-through warming on Put — the
// latter is why a freshly saved snapshot is served from cache on the next read
// instead of a cold object-store round-trip. Presign capability is passed
// through unchanged and never cached.
package cached

import "context"

// Cache is a []byte cache keyed by (namespace, key). Take is read-through with a
// single-flight loader on miss; Set is the write-through path used to warm the
// cache with bytes the caller already holds.
type Cache interface {
	Take(ctx context.Context, namespace, key string, loader func() ([]byte, error)) ([]byte, error)
	Set(ctx context.Context, namespace, key string, data []byte) error
}

// NoOpCache always invokes the loader and stores nothing. Use it to leave a
// backend explicitly uncached.
type NoOpCache struct{}

func NewNoOpCache() *NoOpCache { return &NoOpCache{} }

func (NoOpCache) Take(_ context.Context, _, _ string, loader func() ([]byte, error)) ([]byte, error) {
	return loader()
}

func (NoOpCache) Set(_ context.Context, _, _ string, _ []byte) error { return nil }
