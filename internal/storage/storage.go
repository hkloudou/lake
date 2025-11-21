package storage

import (
	"context"
	"io"
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
type StreamStorage interface {
	Storage

	// PutStream stores data from a reader
	PutStream(ctx context.Context, key string, reader io.Reader, size int64) error

	// GetStream retrieves data as a reader
	GetStream(ctx context.Context, key string) (io.ReadCloser, error)
}

// MakeKey generates storage key for catalog and uuid
func MakeKey(catalog, uuid string) string {
	return catalog + "/" + uuid + ".json"
}
