package storage

import (
	"context"
	"crypto/md5"
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
// Uses MD5 for sharding + hex for identification
// shardSize: number of MD5 prefix chars for sharding (typically 4)
// Format: md5(catalog)[0:shardSize]/hex(catalog)
// Examples (shardSize=4):
//
//	"Users"    -> MD5="f9aa..." hex="5573657273" -> "f9aa/5573657273"
//	"users"    -> MD5="9bc6..." hex="7573657273" -> "9bc6/7573657273"
//	"products" -> MD5="8602..." hex="70726f6475637473" -> "8602/70726f6475637473"
//
// Benefits:
//   - MD5 prefix: uniform distribution across shards
//   - Hex suffix: preserves original catalog (no confusion)
//   - No collisions: hex(catalog) is unique identifier
//
// Shard Size Analysis:
//   - size=4: 16^4 = 65,536 directories (recommended) ✅
//   - size=6: 16^6 = 16,777,216 directories (overkill)
func encodeCatalogPath(catalog string, shardSize int) string {
	// MD5 hash for uniform shard distribution
	hash := md5.Sum([]byte(catalog))
	md5Hex := hex.EncodeToString(hash[:])

	// Hex encode catalog for unique identification
	catalogHex := hex.EncodeToString([]byte(catalog))

	// Format: md5[0:shardSize]/catalogHex
	// This ensures uniform sharding while preserving original catalog info
	prefix := md5Hex[0:shardSize]
	return prefix + "/" + catalogHex
}

// MakeDeltaKey generates storage key for data files with MD5-sharded path
// Format: {md5[0:4]}/{hex(catalog)}/delta/{ts}_{seqid}_{mergeTypeInt}.json
// Example: f9aa/5573657273/delta/1700000000_123_1.json (for catalog "Users")
func MakeDeltaKey(catalog string, tsSeqID index.TimeSeqID, mergeType int) string {
	shardedPath := encodeCatalogPath(catalog, 4) // Default: 4-char MD5 prefix (65,536 dirs)
	return fmt.Sprintf("%s/delta/%s_%d.json", shardedPath, tsSeqID.String(), mergeType)
}

// MakeSnapKey generates storage key for snapshot files with MD5-sharded path
// Format: {md5[0:4]}/{hex(catalog)}/snap/{startTsSeq}~{stopTsSeq}.snap
// Example: f9aa/5573657273/snap/1700000000_1~1700000100_500.snap (for catalog "Users")
func MakeSnapKey(catalog string, startTsSeq, stopTsSeq index.TimeSeqID) string {
	shardedPath := encodeCatalogPath(catalog, 4) // Default: 4-char MD5 prefix (65,536 dirs)
	return fmt.Sprintf("%s/snap/%s~%s.snap", shardedPath, startTsSeq.String(), stopTsSeq.String())
}
