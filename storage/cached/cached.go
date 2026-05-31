package cached

import (
	"context"

	"github.com/hkloudou/lake/v3/storage"
)

// cachedStorage decorates a storage.Storage with read-through Get and
// write-through Put. namespace isolates this backend's keys in a shared cache.
type cachedStorage struct {
	namespace string
	base      storage.Storage
	cache     Cache
}

// Get is read-through: a cache hit skips the backend; a miss loads from the
// backend and stores the result.
func (s cachedStorage) Get(ctx context.Context, catalog, path string) ([]byte, error) {
	return s.cache.Take(ctx, s.namespace, path, func() ([]byte, error) {
		return s.base.Get(ctx, catalog, path)
	})
}

// Put writes to the backend, then warms the cache with the same bytes
// (write-through). This is what lets the next reader of a just-saved snapshot
// hit the cache instead of paying a cold backend round-trip. The warm is
// best-effort: a cache-write failure never fails the Put.
func (s cachedStorage) Put(ctx context.Context, catalog, path string, data []byte) error {
	if err := s.base.Put(ctx, catalog, path, data); err != nil {
		return err
	}
	_ = s.cache.Set(ctx, s.namespace, path, data)
	return nil
}

// cachedPresignStorage additionally exposes Presigner when the wrapped backend
// supports it, so a caching wrapper never hides an object store's presign
// capability from WriteBegin. Presign mints a URL; it is never cached.
type cachedPresignStorage struct {
	cachedStorage
	presigner storage.Presigner
}

func (s cachedPresignStorage) PresignPut(ctx context.Context, catalog, path string, opts storage.PresignOptions) (storage.PresignedUpload, error) {
	return s.presigner.PresignPut(ctx, catalog, path, opts)
}

// Wrap decorates base with read-through (Get) and write-through (Put) caching
// under the given namespace. If base implements storage.Presigner the returned
// Storage does too (delegating, uncached), so `st.(storage.Presigner)` keeps
// working for object-store backends.
func Wrap(namespace string, base storage.Storage, cache Cache) storage.Storage {
	cs := cachedStorage{namespace: namespace, base: base, cache: cache}
	if p, ok := base.(storage.Presigner); ok {
		return cachedPresignStorage{cachedStorage: cs, presigner: p}
	}
	return cs
}

// Resolver wraps every Storage that inner returns with the cache chosen by
// policy(kind, provider, bucket); a nil policy result leaves that backend
// uncached. Keys are namespaced by "provider|bucket". Because Lake passes the
// object Kind, one bucket can cache snapshots and skip deltas with no path
// inspection and no bucket split:
//
//	resolve := cached.Resolver(backends, func(kind storage.Kind, provider, bucket string) cached.Cache {
//	    if kind == storage.Snap {
//	        return cached.NewRedisCache(cacheRDB, 2*time.Hour) // cache snapshots
//	    }
//	    return nil // deltas: read once before a snapshot absorbs them — uncached
//	})
func Resolver(inner storage.Resolver, policy func(kind storage.Kind, provider, bucket string) Cache) storage.Resolver {
	return func(kind storage.Kind, provider, bucket string) (storage.Storage, error) {
		base, err := inner(kind, provider, bucket)
		if err != nil {
			return nil, err
		}
		c := policy(kind, provider, bucket)
		if c == nil {
			return base, nil
		}
		return Wrap(provider+"|"+bucket, base, c), nil
	}
}
