package lake

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/hkloudou/lake/v3/internal/storage"
	"github.com/hkloudou/lake/v3/internal/utils"
)

// ErrPresignNotSupported is returned by WriteBegin when the configured
// storage backend cannot mint presigned URLs (file / memory).
var ErrPresignNotSupported = storage.ErrPresignNotSupported

// Default presign tunings; override via WriteBeginOption.
const (
	defaultUploadTTL    = 15 * time.Minute
	defaultMaxBodyBytes = 100 * 1024 * 1024 // 100 MB
)

// WriteBeginRequest describes a write that is about to happen.
type WriteBeginRequest struct {
	Catalog   string    `json:"catalog"`
	Path      string    `json:"path"`      // JSON path; "/" means root
	MergeType MergeType `json:"mergeType"` // Replace, RFC7396, or RFC6902
}

// WriteHandle is what WriteBegin returns and WriteNotify consumes. It
// carries enough state for a stateless HTTP transport: serialise to
// JSON, ship to a non-Go client, that client uploads to UploadURL,
// then ships the same handle back to Lake's notify endpoint.
//
// Fields are exported so SDKs in other languages can construct it.
// ExpiresAt is a Unix timestamp in seconds (not RFC3339) so non-Go
// clients can compare it without parsing dates.
type WriteHandle struct {
	Catalog       string            `json:"catalog"`
	Path          string            `json:"path"`
	MergeType     MergeType         `json:"mergeType"`
	UUID          string            `json:"uuid"`
	StorageKey    string            `json:"storageKey"` // for debugging / traceability
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

// WithMaxBodyBytes overrides the signed URL's max content length
// (default 100 MB).
func WithMaxBodyBytes(n int64) WriteBeginOption {
	return func(o *writeBeginOpts) { o.maxContentLength = n }
}

// WithUploadContentType pins Content-Type into the signed URL.
func WithUploadContentType(ct string) WriteBeginOption {
	return func(o *writeBeginOpts) { o.contentType = ct }
}

// WriteBegin reserves a UUID and signs an OSS PUT URL for direct client
// upload. NO Redis write happens here — Begin is a pure function of
// (catalog, path, mergeType, OSS credentials, current time). It can be
// served from FaaS / edge with no Redis dependency, and the returned
// WriteHandle is JSON-serialisable for non-Go clients.
//
// The signed URL forces the client to attach OSS user metadata
// (catalog / path / mergeType) so the OSS object is self-describing —
// this is what makes "OSS is source of truth" provable: an LIST + head
// of the bucket can rebuild Lake's Redis index.
func (c *Client) WriteBegin(ctx context.Context, req WriteBeginRequest, opts ...WriteBeginOption) (*WriteHandle, error) {
	c.emitEvent(req.Catalog, "WriteBegin", map[string]any{"path": req.Path, "mergeType": int(req.MergeType)})

	if err := utils.ValidateCatalog(req.Catalog); err != nil {
		return nil, err
	}
	if err := utils.ValidateFieldPath(req.Path); err != nil {
		return nil, err
	}
	if req.MergeType < MergeTypeReplace || req.MergeType > MergeTypeRFC6902 {
		return nil, fmt.Errorf("invalid mergeType: %d", req.MergeType)
	}
	if err := c.ensureInitialized(ctx); err != nil {
		return nil, err
	}

	presigner, ok := c.storage.(storage.Presigner)
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
	key := c.storage.MakeDeltaKey(req.Catalog, uuid)

	upload, err := presigner.PresignPut(ctx, key, storage.PresignOptions{
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
		StorageKey:    key,
		UploadURL:     upload.URL,
		UploadMethod:  upload.Method,
		UploadHeaders: upload.Headers,
		ExpiresAt:     time.Now().Add(o.ttl).Unix(),
	}, nil
}

// WriteNotify finalises a write: allocates a tsSeq, atomically adds
// the committed delta member to Redis. The OSS object the client
// uploaded to handle.UploadURL is referenced by handle.UUID inside the
// member, so reads can resolve it.
//
// Notify is NOT idempotent — duplicate calls produce duplicate deltas
// (each with its own tsSeq, all referencing the same OSS uuid). For
// Replace / RFC7396 / RFC6902, applying the same body twice is benign;
// nevertheless, callers should retry only after the previous Notify
// definitively errored.
func (c *Client) WriteNotify(ctx context.Context, h *WriteHandle) error {
	if h == nil {
		return errors.New("nil WriteHandle")
	}
	c.emitEvent(h.Catalog, "WriteNotify", map[string]any{"path": h.Path, "uuid": h.UUID})

	if err := utils.ValidateCatalog(h.Catalog); err != nil {
		return err
	}
	if err := utils.ValidateFieldPath(h.Path); err != nil {
		return err
	}
	if h.UUID == "" {
		return errors.New("empty UUID in handle")
	}
	if err := c.ensureInitialized(ctx); err != nil {
		return err
	}
	_, _, err := c.writer.Notify(ctx, h.Catalog, h.Path, h.MergeType, h.UUID)
	return err
}

// newUUID returns a UUID v4 string (32 hex chars, no hyphens). Hyphens
// are dropped so the value can be embedded in OSS keys / Redis members
// without escaping.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return hex.EncodeToString(b[:]), nil
}
