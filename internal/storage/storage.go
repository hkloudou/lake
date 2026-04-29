package storage

import (
	"context"

	"github.com/hkloudou/lake/v3/internal/index"
)

// Storage is the backend interface for object storage.
//
// Per-backend key shape is intentionally NOT uniform: OSS uses flat keys
// with MD5 sharding (object stores have no prefix penalty); File uses a
// deeper tree (filesystems do); Memory is trivial (test only).
// Switching backends therefore requires data migration — explicit contract.
type Storage interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error // idempotent: missing key is not an error
	Exists(ctx context.Context, key string) (bool, error)
	List(ctx context.Context, prefix string) ([]string, error)

	// RedisPrefix is the deployment-level Redis namespace (lake.setting Name).
	// All Lake clients sharing the same RedisPrefix share index, caches,
	// sample state, and seqid space.
	RedisPrefix() string

	MakeDeltaKey(catalog string, tsSeqID index.TimeSeqID, mergeType int) string
	MakeSnapKey(catalog string, stopTsSeq index.TimeSeqID) string
}
