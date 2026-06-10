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
