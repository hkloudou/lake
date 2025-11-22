package storage

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v2/internal/encode"
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
// Delegates to encode.EncodeOssCatalogPath
func encodeCatalogPath(catalog string, shardSize int) string {
	return encode.EncodeOssCatalogPath(catalog, shardSize)
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
