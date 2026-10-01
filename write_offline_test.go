package lake

import (
	"context"
	"testing"

	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
)

// TestNewWriteHandle_OfflineMintAccepted_Redis: a handle minted without a
// Client (only a presigner and, if the notifying side signs, the same secret)
// is accepted by WriteNotify exactly like one from WriteBegin.
func TestNewWriteHandle_OfflineMintAccepted_Redis(t *testing.T) {
	secret := []byte("shared-secret")
	c, store, ctx := newMemClient(t, WithHandleSecret(secret))
	presigner := presignBucket{store.Bucket("data")}
	req := WriteBeginRequest{Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data"}

	h, err := NewWriteHandle(ctx, req, presigner, secret)
	if err != nil {
		t.Fatalf("NewWriteHandle: %v", err)
	}
	if err := store.Bucket(h.Bucket).Put(ctx, h.Catalog, h.Key, []byte(`{"offline":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteNotify(ctx, h); err != nil {
		t.Fatalf("WriteNotify of an offline-minted handle: %v", err)
	}
	if got, err := ReadString(ctx, c.List(ctx, "users")); err != nil || got != `{"offline":true}` {
		t.Fatalf("read = %q, %v", got, err)
	}

	// A handle minted with the wrong secret is rejected like any tampered one.
	bad, _ := NewWriteHandle(ctx, req, presigner, []byte("other"))
	if err := c.WriteNotify(ctx, bad); err == nil {
		t.Fatal("handle signed with a different secret must be rejected")
	}
	// Validation runs offline too: no presigner call for an invalid request.
	if _, err := NewWriteHandle(ctx, WriteBeginRequest{Catalog: "a|b", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data"}, failingPresigner{}, nil); err == nil {
		t.Fatal("invalid catalog must fail before presigning")
	}
}

type failingPresigner struct{}

func (failingPresigner) PresignPut(context.Context, string, string, storage.PresignOptions) (storage.PresignedUpload, error) {
	panic("presigner must not be called for an invalid request")
}

var _ storage.Presigner = presignBucket{mem.New().Bucket("x")}
