// Package storage defines Lake's object-storage contract. Lake itself is
// storage-agnostic — it never imports a cloud SDK. The embedding program
// supplies a Resolver that maps a (kind, provider, bucket) to a bucket-scoped
// Storage; ready-made backends live in optional subpackages (storage/oss,
// storage/mem).
package storage

import (
	"context"
	"errors"
	"strconv"
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
// the memory backend does not, and NewWriteHandle returns
// ErrPresignNotSupported for it.
type Presigner interface {
	PresignPut(ctx context.Context, catalog, path string, opts PresignOptions) (PresignedUpload, error)
}

// Kind tells a Resolver which class of object Lake is about to access, so it can
// route by class — cache snapshots, pick a storage tier, tag metrics — without
// inspecting the path or relying on bucket naming. Object storage only ever
// holds these two.
type Kind uint8

const (
	Delta Kind = iota // a write's body: client-uploaded, read only until a snapshot absorbs it
	Snap              // a packed checkpoint: read on every read of the catalog
)

// String must stay exhaustive: a future Kind must not be mislabeled as an
// existing one in logs, error messages, or metrics.
func (k Kind) String() string {
	switch k {
	case Delta:
		return "delta"
	case Snap:
		return "snap"
	default:
		return "kind(" + strconv.Itoa(int(k)) + ")"
	}
}

// Resolver maps a (kind, provider, bucket) to a bucket-scoped Storage. It is the
// single storage-injection point of a Lake client (passed to lake.New): kind
// lets it route snapshots and deltas differently (e.g. cache only snapshots)
// even when they share a bucket. The implementation owns all credential /
// endpoint / pooling / multi-account routing; Lake only ever calls the returned
// Storage. A Client memoises the result per (kind, provider, bucket) on the
// read path, but NewWriteHandle calls the Resolver for every handle it mints,
// possibly concurrently — so a Resolver must be cheap and safe for concurrent
// use: build SDK clients once outside the closure and only look them up inside
// (the oss / mem backends and cached.Resolver all behave this way).
type Resolver func(kind Kind, provider, bucket string) (Storage, error)

// PresignOptions tunes the signed PUT.
type PresignOptions struct {
	TTL          time.Duration     // signature validity
	UserMetadata map[string]string // mapped to x-oss-meta-* / x-amz-meta-*
	ContentType  string            // optional; if set, signed and required
}

// PresignedUpload is the JSON-serialisable result handed back to a client.
type PresignedUpload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
}

// ErrPresignNotSupported is returned by backends without presign capability.
var ErrPresignNotSupported = errors.New("storage: presigned uploads not supported by this backend")
