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
	"github.com/tidwall/gjson"
)

// newSignedDeadClient is a Client with handle signing on and an unreachable
// index Redis — WriteBegin never touches Redis and signature rejection in
// WriteNotify happens before the Redis call, so both are testable offline.
func newSignedDeadClient(t *testing.T, secret string) *Client {
	t.Helper()
	store := mem.New()
	resolve := func(_ storage.Kind, _, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	rdb := redis.NewClient(&redis.Options{Addr: unreachableRedis, DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = rdb.Close() })
	return New("sigtest", rdb, resolve, WithHandleSecret([]byte(secret)))
}

func beginSigned(t *testing.T, c *Client) *WriteHandle {
	t.Helper()
	h, err := c.WriteBegin(context.Background(), WriteBeginRequest{
		Catalog: "users", Path: "/profile", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
	})
	if err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
	if h.Signature == "" {
		t.Fatal("WithHandleSecret client must sign the handle")
	}
	return h
}

func TestHandleSignature_RejectsTampering(t *testing.T) {
	c := newSignedDeadClient(t, "s3cret")
	ctx := context.Background()

	// Each mutation of a signed identity field must be rejected before Redis.
	tampers := map[string]func(*WriteHandle){
		"path":      func(h *WriteHandle) { h.Path = "/other" },
		"mergeType": func(h *WriteHandle) { h.MergeType = MergeTypeRFC7396 },
		"expiresAt": func(h *WriteHandle) { h.ExpiresAt += 3600 },
		"signature": func(h *WriteHandle) { h.Signature = strings.Repeat("0", len(h.Signature)) },
	}
	for name, tamper := range tampers {
		h := beginSigned(t, c)
		tamper(h)
		if err := c.WriteNotify(ctx, h); err == nil || !strings.Contains(err.Error(), "signature") {
			t.Errorf("tampered %s: expected signature rejection, got %v", name, err)
		}
	}

	// A stripped signature is rejected too.
	h := beginSigned(t, c)
	h.Signature = ""
	if err := c.WriteNotify(ctx, h); err == nil || !strings.Contains(err.Error(), "signature required") {
		t.Errorf("stripped signature: expected 'signature required', got %v", err)
	}

	// A different deployment secret must not validate this handle.
	other := newSignedDeadClient(t, "other-secret")
	h = beginSigned(t, c)
	if err := other.WriteNotify(ctx, h); err == nil || !strings.Contains(err.Error(), "invalid handle signature") {
		t.Errorf("cross-secret: expected invalid signature, got %v", err)
	}
}

// TestHandleSignature_RejectsExpiredHandle: the signature makes ExpiresAt
// trustworthy, so Notify must also enforce it — a leaked signed handle is
// not replayable indefinitely. (The handle here is validly signed OVER the
// past expiry, isolating the expiry check from the tamper check.)
func TestHandleSignature_RejectsExpiredHandle(t *testing.T) {
	c := newSignedDeadClient(t, "s3cret")
	h := beginSigned(t, c)
	h.ExpiresAt = time.Now().Add(-time.Hour).Unix()
	h.Signature = c.signHandle(h)
	if err := c.WriteNotify(context.Background(), h); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expiry rejection, got %v", err)
	}
}

// TestHandleSignature_IgnoredWithoutSecret: verification is opt-in — a
// secretless client must not reject a handle for carrying (or lacking) a
// signature. The structurally valid handle passes every pre-Redis check and
// fails only at the unreachable Redis, proving the signature field played no
// part.
func TestHandleSignature_IgnoredWithoutSecret(t *testing.T) {
	c := newDeadClient(t)
	h := &WriteHandle{
		Catalog: "users", Path: "/", MergeType: MergeTypeReplace, UUID: testUUID,
		URI:       "mem://data/" + objkey.DeltaPath("users", testUUID),
		Signature: "bogus-but-ignored",
	}
	err := c.WriteNotify(context.Background(), h)
	if err == nil {
		t.Fatal("expected an error from the unreachable Redis")
	}
	if strings.Contains(err.Error(), "signature") {
		t.Fatalf("secretless client must ignore the signature, got %v", err)
	}
}

// TestHandleSignature_SignedRoundTrip_Redis: the happy path against live
// Redis — an untampered signed handle notifies and reads back normally.
func TestHandleSignature_SignedRoundTrip_Redis(t *testing.T) {
	rdb := redisTestDB(t, 13)
	prefix := testPrefix(t)
	cleanupKeys(t, rdb, prefix+":*")

	store := mem.New()
	resolve := func(_ storage.Kind, _, bucket string) (storage.Storage, error) {
		return presignBucket{store.Bucket(bucket)}, nil
	}
	c := New(prefix, rdb, resolve, WithHandleSecret([]byte("s3cret")))

	ctx := context.Background()
	h, err := c.WriteBegin(ctx, WriteBeginRequest{
		Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data",
	})
	if err != nil {
		t.Fatalf("WriteBegin: %v", err)
	}
	if err := store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(`{"name":"Alice"}`)); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := c.WriteNotify(ctx, h); err != nil {
		t.Fatalf("WriteNotify (signed, untampered): %v", err)
	}
	got, err := ReadString(ctx, c.List(ctx, "users"))
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	if gjson.Parse(got).Get("name").String() != "Alice" {
		t.Fatalf("doc = %s, want name=Alice", got)
	}
}
