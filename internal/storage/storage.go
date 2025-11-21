package storage

import (
	"context"
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
func MakeKey(catalog, identifier string) string {
	return catalog + "/" + identifier + ".json"
}

// MakeDataKey generates storage key for data files
// Format: catalog/{ts}_{seqid}_{mergeTypeInt}.json
func MakeDataKey(catalog string, tsSeqID index.TimeSeqID, mergeType int) string {
	return fmt.Sprintf("%s/%s_%d.json", catalog, tsSeqID.String(), mergeType)
}

// MakeSnapKey generates storage key for snapshot files
// Format: catalog/snap/{startTsSeq}~{stopTsSeq}.snap
// Example: users/snap/1700000000_1~1700000100_500.snap
func MakeSnapKey(catalog string, startTsSeq, stopTsSeq index.TimeSeqID) string {
	return fmt.Sprintf("%s/snap/%s~%s.snap", catalog, startTsSeq.String(), stopTsSeq.String())
}
