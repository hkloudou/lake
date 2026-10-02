package lake

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hkloudou/lake/v3/internal/objkey"
	"github.com/hkloudou/lake/v3/storage"
)

func TestWriteNotify_RejectsInvalidMergeTypeBeforeRedis(t *testing.T) {
	c := newDeadClient(t)
	err := c.WriteNotify(context.Background(), &WriteHandle{
		Catalog:   "users",
		Path:      "/",
		MergeType: MergeTypeUnknown,
		URI:       "mem://data/object.dat",
	})
	if err == nil {
		t.Fatal("expected invalid mergeType error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid mergeType") {
		t.Fatalf("expected invalid mergeType error, got %v", err)
	}
}

// testUUID is a well-formed (32 lowercase hex) UUID segment for delta paths
// that must pass the URI binding check and reach a later validation step.
const testUUID = "0123456789abcdef0123456789abcdef"

func TestWriteNotify_RejectsMalformedURIBeforeRedis(t *testing.T) {
	c := newDeadClient(t)
	err := c.WriteNotify(context.Background(), &WriteHandle{
		Catalog:   "users",
		Path:      "/",
		MergeType: MergeTypeReplace,
		URI:       "oops",
	})
	if err == nil {
		t.Fatal("expected invalid storage URI error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid storage URI") {
		t.Fatalf("expected invalid storage URI error, got %v", err)
	}
}

// TestWriteNotify_RejectsForeignURI: handles round-trip through untrusted
// clients, so Notify must refuse a URI whose object path is not a delta path
// of this handle's own catalog — another catalog's object, a free-form path,
// a snapshot, or a delta path whose UUID segment is malformed (the segment
// is the only free text in the path, so it must be exactly 32 lowercase hex).
func TestWriteNotify_RejectsForeignURI(t *testing.T) {
	c := newDeadClient(t)
	for _, uri := range []string{
		"mem://data/" + objkey.DeltaPath("other-catalog", testUUID), // another catalog's object
		"mem://data/arbitrary/object.dat",                           // free-form path
		"mem://data/" + objkey.SnapPath("users", "1700000000_1"),    // a snap, not a delta
		"mem://data/" + objkey.DeltaPath("users", "short"),
		"mem://data/" + objkey.DeltaPath("users", strings.Repeat("g", 32)),
		"mem://data/" + objkey.DeltaPath("users", testUUID+"ff"),
		"mem://data/" + objkey.DeltaPath("users", "../"+testUUID[3:]),
	} {
		err := c.WriteNotify(context.Background(), &WriteHandle{
			Catalog:   "users",
			Path:      "/",
			MergeType: MergeTypeReplace,
			URI:       uri,
		})
		if err == nil || !strings.Contains(err.Error(), "is not a delta object") {
			t.Fatalf("uri %q: expected not-a-delta error, got %v", uri, err)
		}
	}
	// The exact shape NewWriteHandle mints passes this check (and then fails
	// only on the unreachable Redis).
	err := c.WriteNotify(context.Background(), &WriteHandle{
		Catalog: "users", Path: "/", MergeType: MergeTypeReplace,
		URI: "mem://data/" + objkey.DeltaPath("users", testUUID),
	})
	if err == nil || strings.Contains(err.Error(), "is not a delta object") {
		t.Fatalf("well-formed delta URI must pass the binding check, got %v", err)
	}
}

// TestNewWriteHandle_RejectsAmbiguousProviderBucket: provider and bucket are
// embedded in the delta URI "provider://bucket/path", which ParseURI splits
// on the first "://" and the first "/" — a "/" or ":" inside either part
// would make the recorded locator resolve to a different object than the one
// presigned. NewWriteHandle must reject such names before presigning anything.
func TestNewWriteHandle_RejectsAmbiguousProviderBucket(t *testing.T) {
	for _, tc := range []struct{ provider, bucket string }{
		{"oss/x", "data"},   // "/" in provider
		{"oss:x", "data"},   // ":" in provider (would nest into "://")
		{"oss", "data/sub"}, // "/" in bucket → ParseURI eats it as path
		{"oss", "data:1"},   // ":" in bucket
		{"oss", "da|ta"},    // delta-member delimiter
		{".oss", "data"},    // leading dot
		{"oss", "-data"},    // leading dash
	} {
		_, err := NewWriteHandle(context.Background(), WriteRequest{
			Catalog: "users", Path: "/", MergeType: MergeTypeReplace,
			Provider: tc.provider, Bucket: tc.bucket,
		}, failingResolver)
		if err == nil || !strings.Contains(err.Error(), "invalid storage") {
			t.Fatalf("provider=%q bucket=%q: expected invalid storage error, got %v", tc.provider, tc.bucket, err)
		}
	}
}

// TestWriteNotify_RejectsAmbiguousURIParts: the handle URI is untrusted
// input recorded verbatim into the index, where reads feed its parsed
// provider/bucket to the resolver — so notify holds both to NewWriteHandle's
// charset even when the path component binds correctly.
func TestWriteNotify_RejectsAmbiguousURIParts(t *testing.T) {
	c := newDeadClient(t)
	for _, uri := range []string{
		"me:m://data/" + objkey.DeltaPath("users", testUUID), // ":" in provider
		"mem://da|ta/" + objkey.DeltaPath("users", testUUID), // "|" in bucket
	} {
		err := c.WriteNotify(context.Background(), &WriteHandle{
			Catalog:   "users",
			Path:      "/",
			MergeType: MergeTypeReplace,
			URI:       uri,
		})
		if err == nil || !strings.Contains(err.Error(), "invalid storage") {
			t.Fatalf("uri %q: expected invalid storage error, got %v", uri, err)
		}
	}
}

// TestWithSnapTarget_PanicsOnAmbiguousTarget: an invalid snap target is a
// construction-time programmer error (package policy: panic). Catching it at
// New matters because a snap URI that parses back to a different bucket
// would wedge every read of a snapshotted catalog at runtime. Both-empty is
// the documented "disabled" spelling and must NOT panic; one-empty must.
func TestWithSnapTarget_PanicsOnAmbiguousTarget(t *testing.T) {
	for _, tc := range []struct{ provider, bucket string }{
		{"oss", "bucket/sub"},
		{"os:s", "bucket"},
		{"", "bucket"},
		{"oss", ""},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("WithSnapTarget(%q, %q) must panic", tc.provider, tc.bucket)
				}
			}()
			WithSnapTarget(tc.provider, tc.bucket)
		}()
	}

	// Both-empty = "auto-snapshotting disabled": stays valid for callers that
	// pass unset config through; the option must be a no-op, not a panic.
	opt := &option{}
	WithSnapTarget("", "")(opt)
	if opt.snapProvider != "" || opt.snapBucket != "" {
		t.Fatalf("WithSnapTarget(\"\", \"\") must leave the target unset, got %q/%q", opt.snapProvider, opt.snapBucket)
	}
}

func TestNewWriteHandle_ZeroTTLUsesDefaultTTL(t *testing.T) {
	var got time.Duration
	rec := func(_ storage.Kind, _, _ string) (storage.Storage, error) {
		return ttlRecorder{&got}, nil
	}
	if _, err := NewWriteHandle(context.Background(), WriteRequest{
		Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
	}, rec, WithUploadTTL(0)); err != nil {
		t.Fatalf("NewWriteHandle: %v", err)
	}
	if got != 15*time.Minute {
		t.Fatalf("presign TTL = %v, want the 15m default", got)
	}
}

// ttlRecorder is a presign-capable Storage that records the TTL it is asked for.
type ttlRecorder struct{ ttl *time.Duration }

func (ttlRecorder) Get(context.Context, string, string) ([]byte, error) { return nil, nil }
func (ttlRecorder) Put(context.Context, string, string, []byte) error   { return nil }
func (r ttlRecorder) PresignPut(_ context.Context, _, _ string, o storage.PresignOptions) (storage.PresignedUpload, error) {
	*r.ttl = o.TTL
	return storage.PresignedUpload{URL: "x://upload", Method: "PUT"}, nil
}
