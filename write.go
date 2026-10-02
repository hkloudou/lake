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

// ErrPresignNotSupported is returned by NewWriteHandle when the resolved backend
// cannot mint presigned URLs (the memory backend).
var ErrPresignNotSupported = storage.ErrPresignNotSupported

const defaultUploadTTL = 15 * time.Minute

// WriteRequest describes a write about to happen. Provider + Bucket pick
// where the body lands, per write; the delta records it as provider://bucket/path.
type WriteRequest struct {
	Catalog   string    `json:"catalog"`
	Path      string    `json:"path"`      // JSON path; "/" means root
	MergeType MergeType `json:"mergeType"` // Replace or RFC7396
	Provider  string    `json:"provider"`
	Bucket    string    `json:"bucket"`
}

// WriteHandle is what NewWriteHandle returns and WriteNotify consumes. The
// identity fields (everything but Upload) are exactly what the index records
// — Catalog keys the delta log, the rest is the member [mergeType, path, …, uri];
// Upload is for the caller's PUT and is ignored by WriteNotify, so a client
// may drop it when it notifies. It is JSON-serialisable so a non-Go client can
// do the upload and ship the handle back to a notify endpoint. The object's
// provider, bucket and key are all in URI (provider://bucket/key). Who may
// mint or notify is not Lake's concern: authenticate those endpoints at the
// HTTP layer.
type WriteHandle struct {
	Catalog   string    `json:"catalog"`
	Path      string    `json:"path"`
	MergeType MergeType `json:"mergeType"`
	URI       string    `json:"uri"` // provider://bucket/key — recorded in the delta

	// Upload is the presigned PUT the caller performs itself: send the body to
	// URL with Method and every header in Headers, verbatim.
	Upload storage.PresignedUpload `json:"upload,omitzero"`
}

// WriteOption tunes the presign call.
type WriteOption func(*writeOpts)

type writeOpts struct {
	ttl         time.Duration
	contentType string
}

// WithUploadTTL overrides the signed URL validity (default 15 min).
func WithUploadTTL(d time.Duration) WriteOption {
	return func(o *writeOpts) { o.ttl = d }
}

// WithUploadContentType pins Content-Type into the signed URL.
func WithUploadContentType(ct string) WriteOption {
	return func(o *writeOpts) { o.contentType = ct }
}

// NewWriteHandle starts a write: it reserves a UUID, derives the object path
// and signs a PUT URL against the storage that resolve maps req.Provider /
// req.Bucket to — the same Resolver reads use, so the URI recorded in the
// index and the URL the client uploads to can never name different places.
// It needs no Client and no Redis — pure local computation plus one presign
// call — so anything that holds the object store's credentials (an API
// server, a gateway, a batch job pre-minting uploads) can produce handles and
// hand them to WriteNotify. resolve is called once per handle, so it must be
// cheap and safe for concurrent use (see storage.Resolver).
func NewWriteHandle(ctx context.Context, req WriteRequest, resolve storage.Resolver, opts ...WriteOption) (*WriteHandle, error) {
	if err := validateWrite(req.Catalog, req.Path, req.MergeType, req.Provider, req.Bucket); err != nil {
		return nil, err
	}
	if resolve == nil {
		return nil, errors.New("lake: NewWriteHandle requires a storage.Resolver")
	}
	st, err := resolve(storage.Delta, req.Provider, req.Bucket)
	if err != nil {
		return nil, fmt.Errorf("resolve %s://%s: %w", req.Provider, req.Bucket, err)
	}
	presigner, ok := st.(storage.Presigner)
	if !ok {
		return nil, ErrPresignNotSupported
	}
	o := &writeOpts{ttl: defaultUploadTTL}
	for _, opt := range opts {
		opt(o)
	}
	if o.ttl <= 0 {
		o.ttl = defaultUploadTTL
	} else if o.ttl < time.Second {
		o.ttl = time.Second // presign APIs work in whole seconds
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
	return &WriteHandle{
		Catalog: req.Catalog, Path: req.Path, MergeType: req.MergeType,
		URI:    objkey.BuildURI(req.Provider, req.Bucket, key),
		Upload: upload,
	}, nil
}

// WriteNotify commits a write: allocates a tsSeq and records the delta
// (carrying handle.URI) in Redis. No storage op — the body is already at
// handle.URI. Idempotent per handle for an hour: a retry after a lost
// response returns success without appending a second delta.
//
// Handles round-trip through clients Lake does not trust, so the URI's object
// path must be a delta path of this handle's own Catalog (its prefix, a
// well-formed UUID, ".dat") — a tampered handle can never point one catalog's
// index at another's objects, or at anything that is not a delta. Whether
// the caller may commit this Catalog / Path / MergeType is the HTTP layer's
// decision, made at notify time on the handle it receives: Lake carries no
// approval token from the begin step, so a client can edit those fields in
// between and WriteNotify commits what it is given.
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
	if !objkey.IsDeltaPath(h.Catalog, path) {
		return fmt.Errorf("handle URI %q is not a delta object of catalog %q", h.URI, h.Catalog)
	}
	_, err = c.idx.Notify(ctx, h.Catalog, h.Path, h.MergeType, h.URI)
	return err
}

// validateWrite is the single check both NewWriteHandle and WriteNotify apply
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
