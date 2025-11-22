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
// Format: hash[0:4]/hash (OSS best practice for sharding)
// Examples:
//
//	"Users"    -> "5573657273" -> "5573/5573657273"
//	"users"    -> "7573657273" -> "7573/7573657273"
//	"a"        -> "61"         -> "61" (short catalog, no sharding)
//	"products" -> "70726f6475637473" -> "7072/70726f6475637473"
func encodeCatalogPath(catalog string) string {
	// Hex encode the catalog name (lowercase only, OSS case-insensitive safe)
	// Hex uses: 0-9, a-f (no uppercase, no conflicts)
	// This preserves case sensitivity while being OSS-safe
	encoded := hex.EncodeToString([]byte(catalog))

	// OSS best practice: prefix directory for sharding
	// Use first 4 chars as directory, full hash as identifier
	// This creates max 16^4 = 65,536 directories
	if len(encoded) <= 4 {
		// Short catalog, no sharding needed
		return encoded
	}

	// Format: hash[0:4]/hash (OSS best practice)
	// Example: "5573/5573657273"
	prefix := encoded[0:4]
	return prefix + "/" + encoded
}

// MakeDeltaKey generates storage key for data files with sharded path
// Format: {hash[0:4]}/{hash}/delta/{ts}_{seqid}_{mergeTypeInt}.json
// Example: 5573/5573657273/delta/1700000000_123_1.json (for catalog "Users")
func MakeDeltaKey(catalog string, tsSeqID index.TimeSeqID, mergeType int) string {
	shardedPath := encodeCatalogPath(catalog)
	return fmt.Sprintf("%s/delta/%s_%d.json", shardedPath, tsSeqID.String(), mergeType)
}

// MakeSnapKey generates storage key for snapshot files with sharded path
// Format: {hash[0:4]}/{hash}/snap/{startTsSeq}~{stopTsSeq}.snap
// Example: 5573/5573657273/snap/1700000000_1~1700000100_500.snap (for catalog "Users")
func MakeSnapKey(catalog string, startTsSeq, stopTsSeq index.TimeSeqID) string {
	shardedPath := encodeCatalogPath(catalog)
	return fmt.Sprintf("%s/snap/%s~%s.snap", shardedPath, startTsSeq.String(), stopTsSeq.String())
}
