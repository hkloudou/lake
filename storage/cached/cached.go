package cached

import (
	"context"
	"strings"

	"github.com/hkloudou/lake/v3/storage"
)

// cachedStorage decorates a storage.Storage with read-through Get and
// write-through Put. namespace isolates this backend's keys in a shared cache.
// cacheable, when non-nil, gates caching per object path (see WrapIf).
type cachedStorage struct {
	namespace string
	base      storage.Storage
	cache     Cache
	cacheable func(path string) bool // nil = cache every object
}

// Get is read-through: a cache hit skips the backend; a miss loads from the
// backend and stores the result. An object the predicate excludes bypasses the
// cache entirely (straight to the backend).
func (s cachedStorage) Get(ctx context.Context, catalog, path string) ([]byte, error) {
	if s.cacheable != nil && !s.cacheable(path) {
		return s.base.Get(ctx, catalog, path)
	}
	return s.cache.Take(ctx, s.namespace, path, func() ([]byte, error) {
		return s.base.Get(ctx, catalog, path)
	})
}

// Put writes to the backend, then warms the cache with the same bytes
// (write-through). This is what lets the next reader of a just-saved snapshot
// hit the cache instead of paying a cold backend round-trip. The warm is
// best-effort: a cache-write failure never fails the Put. An object the
// predicate excludes is written to the backend but not cached.
func (s cachedStorage) Put(ctx context.Context, catalog, path string, data []byte) error {
	if err := s.base.Put(ctx, catalog, path, data); err != nil {
		return err
	}
	if s.cacheable == nil || s.cacheable(path) {
		_ = s.cache.Set(ctx, s.namespace, path, data)
	}
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
	return WrapIf(namespace, base, cache, nil)
}

// WrapIf is Wrap that caches an object only when cacheable(path) returns true;
// every other object passes straight through to base, with no cache Get or Set.
// A nil predicate caches everything, exactly like Wrap.
//
// Use it when one bucket holds more than one class of object and only some are
// worth caching — e.g. Lake snapshots and deltas sharing a bucket: cache the
// snapshots (read on every read) and skip the deltas (read once, then absorbed
// by the next snapshot):
//
//	cached.WrapIf(provider+"|"+bucket, base, snapCache, cached.BySuffix(".snap"))
func WrapIf(namespace string, base storage.Storage, cache Cache, cacheable func(path string) bool) storage.Storage {
	cs := cachedStorage{namespace: namespace, base: base, cache: cache, cacheable: cacheable}
	if p, ok := base.(storage.Presigner); ok {
		return cachedPresignStorage{cachedStorage: cs, presigner: p}
	}
	return cs
}

// Resolver wraps every Storage that inner returns with the cache chosen by
// policy(provider, bucket); a nil policy result leaves that backend uncached.
// Keys are namespaced by "provider|bucket", so one shared cache never collides
// across buckets. Typical use routes snaps and deltas (usually different
// buckets) to different cache tiers:
//
//	resolve := cached.Resolver(inner, func(provider, bucket string) cached.Cache {
//	    switch bucket {
//	    case snapBucket:  return cached.NewRedisCache(cacheRDB, 2*time.Hour) // shared, long TTL
//	    case deltaBucket: return cached.NewMemoryCache(time.Minute)         // process-local, short TTL
//	    default:          return nil                                        // uncached
//	    }
//	})
//
// When snaps and deltas share ONE bucket, policy can't tell them apart (it never
// sees the object path); wrap that bucket with WrapIf + BySuffix(".snap") instead.
func Resolver(inner storage.Resolver, policy func(provider, bucket string) Cache) storage.Resolver {
	return func(provider, bucket string) (storage.Storage, error) {
		base, err := inner(provider, bucket)
		if err != nil {
			return nil, err
		}
		c := policy(provider, bucket)
		if c == nil {
			return base, nil
		}
		return Wrap(provider+"|"+bucket, base, c), nil
	}
}

// BySuffix returns a WrapIf predicate that caches only objects whose path ends
// in one of suffixes. With Lake's object naming this selects an object class:
// cached.BySuffix(".snap") caches snapshots only (delta bodies end in ".dat"),
// so snapshots and deltas can share a bucket yet only the snapshot is cached.
func BySuffix(suffixes ...string) func(path string) bool {
	return func(path string) bool {
		for _, sfx := range suffixes {
			if strings.HasSuffix(path, sfx) {
				return true
			}
		}
		return false
	}
}
