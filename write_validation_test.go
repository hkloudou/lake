package lake

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hkloudou/lake/v3/internal/objkey"
	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
	"github.com/redis/go-redis/v9"
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

// testUUID is a well-formed (32 lowercase hex) handle UUID for tests that
// must get past the UUID check to reach a later validation step.
const testUUID = "0123456789abcdef0123456789abcdef"

func TestWriteNotify_RejectsMalformedURIBeforeRedis(t *testing.T) {
	c := newDeadClient(t)
	err := c.WriteNotify(context.Background(), &WriteHandle{
		Catalog:   "users",
		Path:      "/",
		MergeType: MergeTypeReplace,
		UUID:      testUUID,
		URI:       "oops",
	})
	if err == nil {
		t.Fatal("expected invalid storage URI error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid storage URI") {
		t.Fatalf("expected invalid storage URI error, got %v", err)
	}
}

// TestWriteNotify_RejectsTamperedUUID: a handle whose UUID is not the exact
// 32-hex form WriteBegin mints is rejected before any Redis call — the UUID
// participates in the recomputed delta path, so it must not carry path
// metacharacters.
func TestWriteNotify_RejectsTamperedUUID(t *testing.T) {
	c := newDeadClient(t)
	for _, uuid := range []string{"", "short", strings.Repeat("g", 32), testUUID + "ff", "../" + testUUID[3:]} {
		err := c.WriteNotify(context.Background(), &WriteHandle{
			Catalog:   "users",
			Path:      "/",
			MergeType: MergeTypeReplace,
			UUID:      uuid,
			URI:       "mem://data/whatever.dat",
		})
		if err == nil || !strings.Contains(err.Error(), "invalid uuid") {
			t.Fatalf("uuid %q: expected invalid uuid error, got %v", uuid, err)
		}
	}
}

// TestWriteNotify_RejectsForeignURI: handles round-trip through untrusted
// clients, so Notify must refuse a URI whose object path is not the delta
// path derived from this handle's own (catalog, uuid) — otherwise a tampered
// handle could point catalog A's index at catalog B's objects (or anywhere).
func TestWriteNotify_RejectsForeignURI(t *testing.T) {
	c := newDeadClient(t)
	for _, uri := range []string{
		"mem://data/" + objkey.DeltaPath("other-catalog", testUUID), // another catalog's object
		"mem://data/arbitrary/object.dat",                           // free-form path
		"mem://data/" + objkey.SnapPath("users", "1700000000_1"),    // a snap, not a delta
	} {
		err := c.WriteNotify(context.Background(), &WriteHandle{
			Catalog:   "users",
			Path:      "/",
			MergeType: MergeTypeReplace,
			UUID:      testUUID,
			URI:       uri,
		})
		if err == nil || !strings.Contains(err.Error(), "does not match catalog/uuid") {
			t.Fatalf("uri %q: expected catalog/uuid mismatch error, got %v", uri, err)
		}
	}
}

// TestWriteBegin_RejectsAmbiguousProviderBucket: provider and bucket are
// embedded in the delta URI "provider://bucket/path", which ParseURI splits
// on the first "://" and the first "/" — a "/" or ":" inside either part
// would make the recorded locator resolve to a different object than the one
// presigned. WriteBegin must reject such names before presigning anything.
func TestWriteBegin_RejectsAmbiguousProviderBucket(t *testing.T) {
	c := newDeadClient(t)
	for _, tc := range []struct{ provider, bucket string }{
		{"oss/x", "data"},   // "/" in provider
		{"oss:x", "data"},   // ":" in provider (would nest into "://")
		{"oss", "data/sub"}, // "/" in bucket → ParseURI eats it as path
		{"oss", "data:1"},   // ":" in bucket
		{"oss", "da|ta"},    // delta-member delimiter
		{".oss", "data"},    // leading dot
		{"oss", "-data"},    // leading dash
	} {
		_, err := c.WriteBegin(context.Background(), WriteBeginRequest{
			Catalog: "users", Path: "/", MergeType: MergeTypeReplace,
			Provider: tc.provider, Bucket: tc.bucket,
		})
		if err == nil || !strings.Contains(err.Error(), "invalid storage") {
			t.Fatalf("provider=%q bucket=%q: expected invalid storage error, got %v", tc.provider, tc.bucket, err)
		}
	}
}

// TestWriteNotify_RejectsAmbiguousURIParts: the handle URI is untrusted
// input recorded verbatim into the index, where reads feed its parsed
// provider/bucket to the resolver — so notify holds both to WriteBegin's
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
			UUID:      testUUID,
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

func TestWriteBegin_ZeroTTLUsesDefaultTTL(t *testing.T) {
	store := mem.New()
	resolve := func(_ storage.Kind, _, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = rdb.Close() })
	c := New("ttltest", rdb, resolve)

	h, err := c.WriteBegin(context.Background(), WriteBeginRequest{
		Catalog:   "users",
		Path:      "/",
		MergeType: MergeTypeReplace,
		Provider:  "mem",
		Bucket:    "data",
	}, WithUploadTTL(0))
	if err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}

	ttl := h.ExpiresAt - time.Now().Unix()
	if ttl < 14*60 || ttl > 16*60 {
		t.Fatalf("ExpiresAt delta = %ds, want about 15m", ttl)
	}
}
