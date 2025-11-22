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

// encodeCatalogPath encodes catalog name to sharded path
// Uses hex encoding (lowercase only) to avoid case-sensitivity issues on OSS
// shardSize: number of characters per shard (e.g., 3)
// Examples (shardSize=3):
//
//	"Users" -> "5573657273" -> "557/365/727/3"
//	"users" -> "7573657273" -> "757/365/727/3"
//	"a"     -> "61"         -> "61"
func encodeCatalogPath(catalog string, shardSize int) string {
	// Hex encode the catalog name (lowercase only, OSS case-insensitive safe)
	// Hex uses: 0-9, a-f (no uppercase, no conflicts)
	encoded := hex.EncodeToString([]byte(catalog))

	// Split into shardSize-character chunks for path sharding
	// This creates a balanced directory tree structure
	var parts []string
	for i := 0; i < len(encoded); i += shardSize {
		end := i + shardSize
		if end > len(encoded) {
			end = len(encoded)
		}
		parts = append(parts, encoded[i:end])
	}

	// Build path based on number of parts
	if len(parts) == 0 {
		return ""
	} else if len(parts) == 1 {
		return parts[0]
	} else if len(parts) == 2 {
		return parts[0] + "/" + parts[1]
	} else {
		// 3+ parts: use first, second, and last for sharding
		// Example: "557/365/3" (if parts = [557, 365, 727, 3] and shardSize=3)
		return parts[0] + "/" + parts[1] + "/" + parts[len(parts)-1]
	}
}

// MakeDeltaKey generates storage key for data files with sharded path
// Format: {hex1}/{hex2}/{hexN}/delta/{ts}_{seqid}_{mergeTypeInt}.json
// Example: 557/365/3/delta/1700000000_123_1.json (for catalog "Users" with shardSize=3)
func MakeDeltaKey(catalog string, tsSeqID index.TimeSeqID, mergeType int) string {
	shardedPath := encodeCatalogPath(catalog, 3) // Default shard size: 3 chars
	return fmt.Sprintf("%s/delta/%s_%d.json", shardedPath, tsSeqID.String(), mergeType)
}

// MakeSnapKey generates storage key for snapshot files with sharded path
// Format: {hex1}/{hex2}/{hexN}/snap/{startTsSeq}~{stopTsSeq}.snap
// Example: 557/365/3/snap/1700000000_1~1700000100_500.snap (for catalog "Users" with shardSize=3)
func MakeSnapKey(catalog string, startTsSeq, stopTsSeq index.TimeSeqID) string {
	shardedPath := encodeCatalogPath(catalog, 3) // Default shard size: 3 chars
	return fmt.Sprintf("%s/snap/%s~%s.snap", shardedPath, startTsSeq.String(), stopTsSeq.String())
}
