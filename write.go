package lake

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

// defaultUploadTTL is the signed-URL validity; override via WithUploadTTL.
const defaultUploadTTL = 15 * time.Minute

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
	// Signature authenticates the handle's identity fields when the Client
	// was built WithHandleSecret; empty otherwise. Clients must echo it back
	// unchanged.
	Signature string `json:"signature,omitempty"`
}

// WriteBeginOption tunes the presign call.
type WriteBeginOption func(*writeBeginOpts)

type writeBeginOpts struct {
	ttl         time.Duration
	contentType string
}

// WithUploadTTL overrides the signed URL validity (default 15 min).
func WithUploadTTL(d time.Duration) WriteBeginOption {
	return func(o *writeBeginOpts) { o.ttl = d }
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
	// Provider/Bucket are embedded in the delta URI (provider://bucket/path);
	// an ambiguous character ("/", ":") would make ParseURI resolve the
	// recorded locator to a different object than the one presigned here.
	if err := utils.ValidateStorageProvider(req.Provider); err != nil {
		return nil, err
	}
	if err := utils.ValidateStorageBucket(req.Bucket); err != nil {
		return nil, err
	}

	st, err := c.storageFor(storage.Delta, req.Provider, req.Bucket)
	if err != nil {
		return nil, err
	}
	presigner, ok := st.(storage.Presigner)
	if !ok {
		return nil, ErrPresignNotSupported
	}

	o := &writeBeginOpts{ttl: defaultUploadTTL}
	for _, opt := range opts {
		opt(o)
	}
	if o.ttl <= 0 {
		o.ttl = defaultUploadTTL
	}

	uuid, err := newUUID()
	if err != nil {
		return nil, fmt.Errorf("generate uuid: %w", err)
	}
	key := objkey.DeltaPath(req.Catalog, uuid)
	uri := objkey.BuildURI(req.Provider, req.Bucket, key)

	upload, err := presigner.PresignPut(ctx, req.Catalog, key, storage.PresignOptions{
		TTL:         o.ttl,
		ContentType: o.contentType,
		UserMetadata: map[string]string{
			"catalog":    req.Catalog,
			"path":       req.Path,
			"merge-type": strconv.Itoa(int(req.MergeType)),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("presign put: %w", err)
	}
	h := &WriteHandle{
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
	}
	if len(c.handleSecret) > 0 {
		h.Signature = c.signHandle(h)
	}
	return h, nil
}

// signHandle computes the HMAC-SHA256 over the handle's identity fields —
// exactly the ones WriteNotify acts on plus ExpiresAt. The payload is a JSON
// string array, so no field value can forge a boundary into a neighbour.
func (c *Client) signHandle(h *WriteHandle) string {
	payload, _ := json.Marshal([6]string{
		h.Catalog, h.Path, strconv.Itoa(int(h.MergeType)),
		h.UUID, h.URI, strconv.FormatInt(h.ExpiresAt, 10),
	})
	mac := hmac.New(sha256.New, c.handleSecret)
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// WriteNotify finalises a write: allocates a tsSeq and atomically records the
// delta (carrying handle.URI) in Redis. It does NOT touch storage — the body
// is already at handle.URI from the direct upload.
//
// Handles round-trip through clients Lake does not trust, so Notify rebinds
// the handle to its own catalog: the URI's object path must be exactly the
// delta path WriteBegin derived for (Catalog, UUID). A tampered handle can
// therefore never point a catalog's index at another catalog's objects.
// With WithHandleSecret configured, Notify additionally requires a valid
// HMAC signature over the identity fields, pinning Path / MergeType /
// ExpiresAt to what WriteBegin issued.
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
	if h.MergeType < MergeTypeReplace || h.MergeType > MergeTypeRFC7396 {
		return fmt.Errorf("invalid mergeType: %d", h.MergeType)
	}
	if !isUUIDHex(h.UUID) {
		return fmt.Errorf("invalid uuid in handle: %q", h.UUID)
	}
	if h.URI == "" {
		return errors.New("empty URI in handle")
	}
	_, _, path, err := objkey.ParseURI(h.URI)
	if err != nil {
		return err
	}
	if want := objkey.DeltaPath(h.Catalog, h.UUID); path != want {
		return fmt.Errorf("handle URI path %q does not match catalog/uuid (want %q)", path, want)
	}
	if len(c.handleSecret) > 0 {
		if h.Signature == "" {
			return errors.New("handle signature required")
		}
		if !hmac.Equal([]byte(c.signHandle(h)), []byte(h.Signature)) {
			return errors.New("invalid handle signature")
		}
		// The signature authenticates ExpiresAt, so enforce it too: a leaked
		// signed handle must not be replayable indefinitely. (Without a
		// secret the field is client-editable, so checking it there would
		// only be theater.)
		if now := time.Now().Unix(); now > h.ExpiresAt {
			return fmt.Errorf("handle expired at %d (now %d)", h.ExpiresAt, now)
		}
	}
	_, _, err = c.writer.Notify(ctx, h.Catalog, h.Path, h.MergeType, h.URI)
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

// isUUIDHex reports whether s is exactly the form newUUID emits: 32 lowercase
// hex chars. WriteNotify uses it to keep a client-supplied UUID from smuggling
// path segments into the recomputed delta path.
func isUUIDHex(s string) bool {
	if len(s) != 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
