package storage

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/hkloudou/lake/v2/internal/index"
)

// Storage is the interface for object storage (OSS/S3/Local)
type Storage interface {
	// Put stores data with the given key
	Put(ctx context.Context, key string, data []byte) error

	// Get retrieves data by key
	Get(ctx context.Context, key string) ([]byte, error)

	// Delete removes data by key
	Delete(ctx context.Context, key string) error

	// Exists checks if key exists
	Exists(ctx context.Context, key string) (bool, error)

	// List lists all keys with the given prefix
	List(ctx context.Context, prefix string) ([]string, error)

	RedisPrefix() string
}

// StreamStorage extends Storage with streaming support
// type StreamStorage interface {
// 	Storage

// 	// PutStream stores data from a reader
// 	PutStream(ctx context.Context, key string, reader io.Reader, size int64) error

// 	// GetStream retrieves data as a reader
// 	GetStream(ctx context.Context, key string) (io.ReadCloser, error)
// }

// MakeKey generates storage key for catalog and file identifier
// For data files: catalog/{ts}_{seqid}_{mergeTypeInt}.json
// For snap files: catalog/{uuid}.json (legacy format)
// func MakeKey(catalog, identifier string) string {
// 	return catalog + "/" + identifier + ".json"
// }

// encodeCatalogPath encodes catalog name following OSS best practices
// Uses hex encoding (lowercase only) to avoid case-sensitivity issues
// shardSize: number of prefix chars for sharding (typically 4 or 6)
// Format: hash[0:shardSize] + "." + hash[shardSize:]
// Examples (shardSize=4):
//
//	"Users"    -> "5573657273" -> "5573.657273"
//	"users"    -> "7573657273" -> "7573.657273"
//	"a"        -> "61"         -> "61" (short, no sharding)
//	"products" -> "70726f6475637473" -> "7072.6f6475637473"
//
// Shard Size Analysis:
//   - size=4: 16^4 = 65,536 directories (recommended) ✅
//   - size=6: 16^6 = 16,777,216 directories (overkill for most cases)
func encodeCatalogPath(catalog string, shardSize int) string {
	// Hex encode the catalog name (lowercase only, OSS case-insensitive safe)
	// Hex uses: 0-9, a-f (no uppercase, no conflicts)
	// This preserves case sensitivity while being OSS-safe
	encoded := hex.EncodeToString([]byte(catalog))

	// OSS best practice: prefix directory for sharding
	// Use first shardSize chars as directory prefix
	if len(encoded) <= shardSize {
		// Short catalog, no sharding needed
		return encoded
	}

	// Format: hash[0:shardSize] + "." + hash[shardSize:]
	// Using "." as separator (filesystem-safe)
	// Example: "5573.657273" (shardSize=4)
	prefix := encoded[0:shardSize]
	suffix := encoded[shardSize:]
	return prefix + "." + suffix
}

// MakeDeltaKey generates storage key for data files with sharded path
// Format: {hash[0:4]}.{hash[4:]}/delta/{ts}_{seqid}_{mergeTypeInt}.json
// Example: 5573.657273/delta/1700000000_123_1.json (for catalog "Users")
func MakeDeltaKey(catalog string, tsSeqID index.TimeSeqID, mergeType int) string {
	shardedPath := encodeCatalogPath(catalog, 4) // Default: 4-char shard (65,536 dirs)
	return fmt.Sprintf("%s/delta/%s_%d.json", shardedPath, tsSeqID.String(), mergeType)
}

// MakeSnapKey generates storage key for snapshot files with sharded path
// Format: {hash[0:4]}.{hash[4:]}/snap/{startTsSeq}~{stopTsSeq}.snap
// Example: 5573.657273/snap/1700000000_1~1700000100_500.snap (for catalog "Users")
func MakeSnapKey(catalog string, startTsSeq, stopTsSeq index.TimeSeqID) string {
	shardedPath := encodeCatalogPath(catalog, 4) // Default: 4-char shard (65,536 dirs)
	return fmt.Sprintf("%s/snap/%s~%s.snap", shardedPath, startTsSeq.String(), stopTsSeq.String())
}
