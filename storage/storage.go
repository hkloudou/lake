// Package storage defines Lake's object-storage contract. Lake itself is
// storage-agnostic — it never imports a cloud SDK. The embedding program
// supplies a Resolver that maps a (provider, bucket) pair to a bucket-scoped
// Storage; ready-made backends live in optional subpackages (storage/oss,
// storage/file, storage/mem).
package storage

import (
	"context"
	"errors"
	"time"
)

// Storage is a bucket-scoped object store. A Resolver has already bound it to
// one (provider, bucket), so methods take only (catalog, path):
//
//   - path    the object key within the bucket. It fully locates the object
//     and is exactly what Lake records in each delta's URI
//     (provider://bucket/path), so the URI is a portable locator.
//   - catalog the owning catalog, passed as context (per-catalog lifecycle
//     rules, metrics, object tagging). A backend need NOT use it to
//     locate — path already does that — and may ignore it.
type Storage interface {
	Get(ctx context.Context, catalog, path string) ([]byte, error)
	Put(ctx context.Context, catalog, path string, data []byte) error
}

// Presigner is an optional capability: a Storage that can mint an HTTP-signed
// URL for a direct client upload. Object stores (OSS / S3 / COS) implement it;
// file / memory backends do not, and WriteBegin returns ErrPresignNotSupported
// for them.
type Presigner interface {
	PresignPut(ctx context.Context, catalog, path string, opts PresignOptions) (PresignedUpload, error)
}

// Resolver maps a (provider, bucket) pair to a bucket-scoped Storage. It is the
// single storage-injection point of a Lake client (passed to lake.New). The
// implementation owns all credential / endpoint / pooling / multi-account
// routing; Lake only ever calls the returned Storage. Lake memoises the result
// per (provider, bucket), so a Resolver is called at most once per distinct
// pair for the life of the client.
type Resolver func(provider, bucket string) (Storage, error)

// PresignOptions tunes the signed PUT.
type PresignOptions struct {
	TTL              time.Duration     // signature validity
	MaxContentLength int64             // 0 = unlimited
	UserMetadata     map[string]string // mapped to x-oss-meta-* / x-amz-meta-*
	ContentType      string            // optional; if set, signed and required
}

// PresignedUpload is the JSON-serialisable result handed back to a client.
type PresignedUpload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
}

// ErrPresignNotSupported is returned by backends without presign capability.
var ErrPresignNotSupported = errors.New("storage: presigned uploads not supported by this backend")
