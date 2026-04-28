package storage

import (
	"context"

	"github.com/hkloudou/lake/v3/internal/index"
)

// Storage is the backend interface for object storage (OSS / local file /
// in-memory).
//
// Per-backend key shape is intentionally NOT uniform across implementations.
// Each backend exposes the structure that fits its own performance and
// addressability constraints:
//
//   - OSS:    flat keys with MD5 sharding (`md5[0:4]/encoded/...`) — object
//             stores have no per-prefix object-count penalty, so depth is
//             optimised for shorter keys / lower request size.
//   - File:   deeper tree (`md5[0:2]/encoded/h1/h2/h3/...`) to keep per-
//             directory file counts under ext4/NTFS-friendly bounds.
//   - Memory: trivial layout (no sharding) — purely for tests; never
//             round-trips a real filesystem.
//
// The trade-off: switching the storage backend on a live deployment requires
// data migration. This is the explicit contract.
type Storage interface {
	// Put stores data under the given key.
	Put(ctx context.Context, key string, data []byte) error

	// Get retrieves data by key.
	Get(ctx context.Context, key string) ([]byte, error)

	// Delete removes data by key. Idempotent: deleting a missing key is
	// not an error.
	Delete(ctx context.Context, key string) error

	// Exists reports whether the key is present.
	Exists(ctx context.Context, key string) (bool, error)

	// List returns all keys under the given storage-side prefix.
	List(ctx context.Context, prefix string) ([]string, error)

	// RedisPrefix returns the per-deployment Redis key prefix used by Lake's
	// index and cache (typically the Name configured in lake.setting). It
	// is the tenancy axis: all Lake clients sharing the same RedisPrefix
	// share index, snapshot/delta caches, sample caches, and seqid space.
	RedisPrefix() string

	MakeDeltaKey(catalog string, tsSeqID index.TimeSeqID, mergeType int) string
	MakeSnapKey(catalog string, startTsSeq, stopTsSeq index.TimeSeqID) string
}
