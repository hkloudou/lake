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

// ErrPresignNotSupported is returned by WriteBegin when the resolved backend
// cannot mint presigned URLs (file / memory).
var ErrPresignNotSupported = storage.ErrPresignNotSupported

const defaultUploadTTL = 15 * time.Minute

// WriteBeginRequest describes a write about to happen. Provider + Bucket pick
// where the body lands, per write; the delta records it as provider://bucket/path.
type WriteBeginRequest struct {
	Catalog   string    `json:"catalog"`
	Path      string    `json:"path"`      // JSON path; "/" means root
	MergeType MergeType `json:"mergeType"` // Replace or RFC7396
	Provider  string    `json:"provider"`
	Bucket    string    `json:"bucket"`
}

// WriteHandle is what WriteBegin returns and WriteNotify consumes. It is
// JSON-serialisable so a non-Go client can upload to UploadURL and ship the
// handle back to a notify endpoint.
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
	ExpiresAt     int64             `json:"expiresAt"`           // unix seconds
	Signature     string            `json:"signature,omitempty"` // set iff WithHandleSecret; echo back unchanged
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

// WriteBegin reserves a UUID, derives the object path and signs a PUT URL
// against (Provider, Bucket) for direct client upload. No Redis op.
func (c *Client) WriteBegin(ctx context.Context, req WriteBeginRequest, opts ...WriteBeginOption) (*WriteHandle, error) {
	if c.hasHandlers() {
		c.emitEvent(req.Catalog, "WriteBegin", map[string]any{
			"path": req.Path, "mergeType": int(req.MergeType), "provider": req.Provider, "bucket": req.Bucket,
		})
	}
	if err := validateWrite(req.Catalog, req.Path, req.MergeType, req.Provider, req.Bucket); err != nil {
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
	} else if o.ttl < time.Second {
		o.ttl = time.Second // presign APIs and ExpiresAt work in whole seconds
	}

	uuid, err := newUUID()
	if err != nil {
		return nil, fmt.Errorf("generate uuid: %w", err)
	}
	key := objkey.DeltaPath(req.Catalog, uuid)
	upload, err := presigner.PresignPut(ctx, req.Catalog, key, storage.PresignOptions{
		TTL:         o.ttl,
		ContentType: o.contentType,
		UserMetadata: map[string]string{
			"catalog": req.Catalog, "path": req.Path, "merge-type": strconv.Itoa(int(req.MergeType)),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("presign put: %w", err)
	}
	h := &WriteHandle{
		Catalog: req.Catalog, Path: req.Path, MergeType: req.MergeType, UUID: uuid,
		Provider: req.Provider, Bucket: req.Bucket, Key: key,
		URI:       objkey.BuildURI(req.Provider, req.Bucket, key),
		UploadURL: upload.URL, UploadMethod: upload.Method, UploadHeaders: upload.Headers,
		ExpiresAt: time.Now().Unix() + int64(o.ttl/time.Second),
	}
	if len(c.handleSecret) > 0 {
		h.Signature = c.signHandle(h)
	}
	return h, nil
}

// WriteNotify commits a write: allocates a tsSeq and records the delta
// (carrying handle.URI) in Redis. No storage op — the body is already at
// handle.URI. Idempotent per handle for an hour: a retry after a lost
// response returns success without appending a second delta.
//
// Handles round-trip through clients Lake does not trust, so the URI must be
// exactly the delta path WriteBegin derived for (Catalog, UUID) — a tampered
// handle can never point one catalog's index at another's objects. With
// WithHandleSecret, Path / MergeType / ExpiresAt are pinned by the signature.
func (c *Client) WriteNotify(ctx context.Context, h *WriteHandle) error {
	if h == nil {
		return errors.New("nil WriteHandle")
	}
	if c.hasHandlers() {
		c.emitEvent(h.Catalog, "WriteNotify", map[string]any{"path": h.Path, "uri": h.URI})
	}
	provider, bucket, path, err := objkey.ParseURI(h.URI)
	if err != nil {
		return err
	}
	if err := validateWrite(h.Catalog, h.Path, h.MergeType, provider, bucket); err != nil {
		return err
	}
	if !isUUIDHex(h.UUID) {
		return fmt.Errorf("invalid uuid in handle: %q", h.UUID)
	}
	if want := objkey.DeltaPath(h.Catalog, h.UUID); path != want {
		return fmt.Errorf("handle URI path %q does not match catalog/uuid (want %q)", path, want)
	}
	if len(c.handleSecret) > 0 {
		if h.Signature == "" || !hmac.Equal([]byte(c.signHandle(h)), []byte(h.Signature)) {
			return errors.New("invalid handle signature")
		}
		if now := time.Now().Unix(); now > h.ExpiresAt {
			return fmt.Errorf("handle expired at %d (now %d)", h.ExpiresAt, now)
		}
	}
	_, err = c.idx.Notify(ctx, h.Catalog, h.Path, h.MergeType, h.URI)
	return err
}

// validateWrite is the single check both WriteBegin and WriteNotify apply
// to a write's identity (the handle is untrusted input).
func validateWrite(catalog, path string, mt MergeType, provider, bucket string) error {
	if err := utils.ValidateCatalog(catalog); err != nil {
		return err
	}
	if err := utils.ValidateFieldPath(path); err != nil {
		return err
	}
	if mt < MergeTypeReplace || mt > MergeTypeRFC7396 {
		return fmt.Errorf("invalid mergeType: %d", mt)
	}
	if err := utils.ValidateStorageProvider(provider); err != nil {
		return err
	}
	return utils.ValidateStorageBucket(bucket)
}

// signHandle is the HMAC-SHA256 over the handle's identity fields, encoded as
// a JSON string array so no field can forge a boundary into a neighbour.
func (c *Client) signHandle(h *WriteHandle) string {
	payload, _ := json.Marshal([6]string{
		h.Catalog, h.Path, strconv.Itoa(int(h.MergeType)), h.UUID, h.URI, strconv.FormatInt(h.ExpiresAt, 10),
	})
	mac := hmac.New(sha256.New, c.handleSecret)
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// newUUID returns a UUID v4 as 32 lowercase hex chars.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b[:]), nil
}

func isUUIDHex(s string) bool {
	if len(s) != 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
