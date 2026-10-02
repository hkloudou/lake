package lake

import (
	"context"
	"testing"

	"github.com/hkloudou/lake/v3/storage"
	"github.com/hkloudou/lake/v3/storage/mem"
)

// TestNewWriteHandle_OfflineMintAccepted_Redis: a handle minted with only a
// presigner and, if the notifying side signs, the same secret
// is accepted by WriteNotify.
func TestNewWriteHandle_OfflineMintAccepted_Redis(t *testing.T) {
	secret := []byte("shared-secret")
	c, store, ctx := newMemClient(t, WithHandleSecret(secret))
	presigner := presignBucket{store.Bucket("data")}
	req := WriteRequest{Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data"}

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
	if _, err := NewWriteHandle(ctx, WriteRequest{Catalog: "a|b", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data"}, failingPresigner{}, nil); err == nil {
		t.Fatal("invalid catalog must fail before presigning")
	}
}

// failingPresigner is a Storage whose presign must never be reached.
type failingPresigner struct{ storage.Storage }

func (failingPresigner) PresignPut(context.Context, string, string, storage.PresignOptions) (storage.PresignedUpload, error) {
	panic("presigner must not be called for an invalid request")
}

// TestNewWriteHandle_RequiresPresigner: a backend without presign capability
// (file / memory) cannot start a write.
func TestNewWriteHandle_RequiresPresigner(t *testing.T) {
	req := WriteRequest{Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data"}
	if _, err := NewWriteHandle(context.Background(), req, mem.New().Bucket("data"), nil); err != ErrPresignNotSupported {
		t.Fatalf("err = %v, want ErrPresignNotSupported", err)
	}
}
