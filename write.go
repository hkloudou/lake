package lake

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/hkloudou/lake/v3/internal/objkey"
	"github.com/hkloudou/lake/v3/internal/utils"
	"github.com/hkloudou/lake/v3/storage"
)

// ErrPresignNotSupported is returned by WriteBegin when the resolved storage
// backend cannot mint presigned URLs (e.g. file / memory).
var ErrPresignNotSupported = storage.ErrPresignNotSupported

// Default presign tunings; override via WriteBeginOption.
const (
	defaultUploadTTL    = 15 * time.Minute
	defaultMaxBodyBytes = 100 * 1024 * 1024 // 100 MB
)

// WriteBeginRequest describes a write that is about to happen. The caller picks
// where the body lands per-write via Provider + Bucket; that location is
// recorded in the delta (as provider://bucket/path), so a catalog's deltas may
// live across different buckets or clouds.
type WriteBeginRequest struct {
	Catalog   string    `json:"catalog"`
	Path      string    `json:"path"`      // JSON path; "/" means root
	MergeType MergeType `json:"mergeType"` // Replace or RFC7396
	Provider  string    `json:"provider"`  // storage provider, e.g. "oss", "cos"
	Bucket    string    `json:"bucket"`    // target bucket
}

// WriteHandle is what WriteBegin returns and WriteNotify consumes. It carries
// enough state for a stateless HTTP transport: serialise to JSON, ship to a
// non-Go client, that client uploads to UploadURL, then ships the handle back
// to Lake's notify endpoint.
type WriteHandle struct {
	Catalog       string            `json:"catalog"`
	Path          string            `json:"path"`
	MergeType     MergeType         `json:"mergeType"`
	UUID          string            `json:"uuid"`
	Provider      string            `json:"provider"`
	Bucket        string            `json:"bucket"`
	Key           string            `json:"key"` // object path within the bucket
	URI           string            `json:"uri"` // provider://bucket/key — recorded in the delta
	UploadURL     string            `json:"uploadURL"`
	UploadMethod  string            `json:"uploadMethod"`
	UploadHeaders map[string]string `json:"uploadHeaders"`
	ExpiresAt     int64             `json:"expiresAt"` // unix seconds
}

// WriteBeginOption tunes the presign call.
type WriteBeginOption func(*writeBeginOpts)

type writeBeginOpts struct {
	ttl              time.Duration
	maxContentLength int64
	contentType      string
}

// WithUploadTTL overrides the signed URL validity (default 15 min).
func WithUploadTTL(d time.Duration) WriteBeginOption {
	return func(o *writeBeginOpts) { o.ttl = d }
}

// WithMaxBodyBytes overrides the signed URL's max content length (default 100 MB).
func WithMaxBodyBytes(n int64) WriteBeginOption {
	return func(o *writeBeginOpts) { o.maxContentLength = n }
}

// WithUploadContentType pins Content-Type into the signed URL.
func WithUploadContentType(ct string) WriteBeginOption {
	return func(o *writeBeginOpts) { o.contentType = ct }
}

// WriteBegin reserves a UUID, derives the object path, and signs a PUT URL
// against the requested (Provider, Bucket) for direct client upload. The
// resulting URI (provider://bucket/path) is returned in the handle and
// recorded by WriteNotify.
func (c *Client) WriteBegin(ctx context.Context, req WriteBeginRequest, opts ...WriteBeginOption) (*WriteHandle, error) {
	c.emitEvent(req.Catalog, "WriteBegin", map[string]any{
		"path": req.Path, "mergeType": int(req.MergeType), "provider": req.Provider, "bucket": req.Bucket,
	})

	if err := utils.ValidateCatalog(req.Catalog); err != nil {
		return nil, err
	}
	if err := utils.ValidateFieldPath(req.Path); err != nil {
		return nil, err
	}
	if req.MergeType < MergeTypeReplace || req.MergeType > MergeTypeRFC7396 {
		return nil, fmt.Errorf("invalid mergeType: %d", req.MergeType)
	}
	if req.Provider == "" || req.Bucket == "" {
		return nil, errors.New("WriteBegin requires Provider and Bucket")
	}

	st, err := c.storageFor(storage.Delta, req.Provider, req.Bucket)
	if err != nil {
		return nil, err
	}
	presigner, ok := st.(storage.Presigner)
	if !ok {
		return nil, ErrPresignNotSupported
	}

	o := &writeBeginOpts{ttl: defaultUploadTTL, maxContentLength: defaultMaxBodyBytes}
	for _, opt := range opts {
		opt(o)
	}

	uuid, err := newUUID()
	if err != nil {
		return nil, fmt.Errorf("generate uuid: %w", err)
	}
	key := objkey.DeltaPath(req.Catalog, uuid)
	uri := objkey.BuildURI(req.Provider, req.Bucket, key)

	upload, err := presigner.PresignPut(ctx, req.Catalog, key, storage.PresignOptions{
		TTL:              o.ttl,
		MaxContentLength: o.maxContentLength,
		ContentType:      o.contentType,
		UserMetadata: map[string]string{
			"catalog":    req.Catalog,
			"path":       req.Path,
			"merge-type": strconv.Itoa(int(req.MergeType)),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("presign put: %w", err)
	}
	return &WriteHandle{
		Catalog:       req.Catalog,
		Path:          req.Path,
		MergeType:     req.MergeType,
		UUID:          uuid,
		Provider:      req.Provider,
		Bucket:        req.Bucket,
		Key:           key,
		URI:           uri,
		UploadURL:     upload.URL,
		UploadMethod:  upload.Method,
		UploadHeaders: upload.Headers,
		ExpiresAt:     time.Now().Add(o.ttl).Unix(),
	}, nil
}

// WriteNotify finalises a write: allocates a tsSeq and atomically records the
// delta (carrying handle.URI) in Redis. It does NOT touch storage — the body
// is already at handle.URI from the direct upload.
//
// Notify is NOT idempotent — duplicate calls produce duplicate deltas (each
// with its own tsSeq, all referencing the same URI). For Replace / RFC7396,
// applying the same body twice is benign; nevertheless, callers should retry
// only after the previous Notify definitively errored.
func (c *Client) WriteNotify(ctx context.Context, h *WriteHandle) error {
	if h == nil {
		return errors.New("nil WriteHandle")
	}
	c.emitEvent(h.Catalog, "WriteNotify", map[string]any{"path": h.Path, "uri": h.URI})

	if err := utils.ValidateCatalog(h.Catalog); err != nil {
		return err
	}
	if err := utils.ValidateFieldPath(h.Path); err != nil {
		return err
	}
	if h.URI == "" {
		return errors.New("empty URI in handle")
	}
	_, _, err := c.writer.Notify(ctx, h.Catalog, h.Path, h.MergeType, h.URI)
	return err
}

// newUUID returns a UUID v4 string (32 hex chars, no hyphens).
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return hex.EncodeToString(b[:]), nil
}
