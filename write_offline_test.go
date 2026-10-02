package lake

import (
	"context"
	"testing"
)

// TestNewWriteHandle_OfflineMintAccepted_Redis: a handle minted with only a
// Resolver and, if the notifying side signs, the same secret
// is accepted by WriteNotify.
func TestNewWriteHandle_OfflineMintAccepted_Redis(t *testing.T) {
	secret := []byte("shared-secret")
	c, store, ctx := newMemClient(t, WithHandleSecret(secret))
	req := WriteRequest{Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data"}

	h, err := NewWriteHandle(ctx, req, c.resolve, secret)
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
	bad, _ := NewWriteHandle(ctx, req, c.resolve, []byte("other"))
	if err := c.WriteNotify(ctx, bad); err == nil {
		t.Fatal("handle signed with a different secret must be rejected")
	}
	// Validation runs first: the resolver is never called for an invalid request.
	if _, err := NewWriteHandle(ctx, WriteRequest{Catalog: "a|b", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data"}, failingResolver, nil); err == nil {
		t.Fatal("invalid catalog must fail before presigning")
	}
}

func TestNewWriteHandle_NilResolverIsAnError(t *testing.T) {
	req := WriteRequest{Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data"}
	if _, err := NewWriteHandle(context.Background(), req, nil, nil); err == nil {
		t.Fatal("nil resolver must be an error, not a panic")
	}
}

// TestNewWriteHandle_RequiresPresigner: a backend without presign capability
// (file / memory) cannot start a write.
func TestNewWriteHandle_RequiresPresigner(t *testing.T) {
	req := WriteRequest{Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data"}
	if _, err := NewWriteHandle(context.Background(), req, memResolver(), nil); err != ErrPresignNotSupported {
		t.Fatalf("err = %v, want ErrPresignNotSupported", err)
	}
}
