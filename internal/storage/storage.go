package storage

import (
	"context"
	"errors"
	"time"

	"github.com/hkloudou/lake/v3/internal/index"
)

// Storage is the backend interface for object storage.
//
// Per-backend key shape is intentionally NOT uniform: OSS uses flat
// keys with MD5 sharding (object stores have no prefix penalty); File
// uses a deeper tree (filesystems do); Memory is trivial (test only).
// Switching backends therefore requires data migration — explicit
// contract.
//
// Bodies are stored RAW. Lake no longer compresses or encrypts bodies
// in the storage layer (clients direct-upload deltas; encryption, if
// needed, is delegated to OSS SSE or client-side encoding).
type Storage interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	List(ctx context.Context, prefix string) ([]string, error)

	// RedisPrefix is the deployment-level Redis namespace (lake.setting Name).
	RedisPrefix() string

	// MakeDeltaKey builds the storage key for a delta object. Deltas
	// are content-keyed by uuid (allocated at WriteBegin) — independent
	// of the tsSeq, which is allocated only at WriteNotify.
	MakeDeltaKey(catalog, uuid string) string

	// MakeSnapKey builds the storage key for a snapshot stamped at
	// stopTsSeq. Snapshots are server-generated, so they keep their
	// tsSeq-based naming.
	MakeSnapKey(catalog string, stopTsSeq index.TimeSeqID) string
}

// Presigner is an optional capability: a Storage backend that can mint
// HTTP-signed URLs for direct client uploads. Only OSS-class backends
// (S3 / Aliyun OSS / etc.) implement this; File / Memory return
// ErrPresignNotSupported.
type Presigner interface {
	PresignPut(ctx context.Context, key string, opts PresignOptions) (PresignedUpload, error)
}

// PresignOptions tunes the signed PUT.
type PresignOptions struct {
	TTL              time.Duration     // signature validity
	MaxContentLength int64             // 0 = unlimited; otherwise enforced by signed Conditions
	UserMetadata     map[string]string // mapped to "x-oss-meta-*" / "x-amz-meta-*" headers
	ContentType      string            // optional; if set, signed and required
}

// PresignedUpload is the JSON-serializable response of WriteBegin.
type PresignedUpload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"` // typically "PUT"
	Headers map[string]string `json:"headers"`
}

// ErrPresignNotSupported is returned by File / Memory backends.
var ErrPresignNotSupported = errors.New("storage: presigned uploads not supported by this backend")
