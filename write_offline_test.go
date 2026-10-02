package lake

import (
	"context"
	"testing"
)

// TestNewWriteHandle_OfflineMintAccepted_Redis: a handle minted with only a
// Resolver — no Client, no Redis — is accepted by WriteNotify.
func TestNewWriteHandle_OfflineMintAccepted_Redis(t *testing.T) {
	c, store, ctx := newMemClient(t)
	req := WriteRequest{Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data"}

	h, err := NewWriteHandle(ctx, req, c.resolve)
	if err != nil {
		t.Fatalf("NewWriteHandle: %v", err)
	}
	if err := upload(store, h, `{"offline":true}`); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteNotify(ctx, h); err != nil {
		t.Fatalf("WriteNotify of an offline-minted handle: %v", err)
	}
	if got, err := ReadString(ctx, c.List(ctx, "users")); err != nil || got != `{"offline":true}` {
		t.Fatalf("read = %q, %v", got, err)
	}

	// Validation runs first: the resolver is never called for an invalid request.
	if _, err := NewWriteHandle(ctx, WriteRequest{Catalog: "a|b", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data"}, failingResolver); err == nil {
		t.Fatal("invalid catalog must fail before presigning")
	}
}

func TestNewWriteHandle_NilResolverIsAnError(t *testing.T) {
	req := WriteRequest{Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data"}
	if _, err := NewWriteHandle(context.Background(), req, nil); err == nil {
		t.Fatal("nil resolver must be an error, not a panic")
	}
}

// TestNewWriteHandle_RequiresPresigner: a backend without presign capability
// (file / memory) cannot start a write.
func TestNewWriteHandle_RequiresPresigner(t *testing.T) {
	req := WriteRequest{Catalog: "users", Path: "/", MergeType: MergeTypeReplace, Provider: "mem", Bucket: "data"}
	if _, err := NewWriteHandle(context.Background(), req, memResolver()); err != ErrPresignNotSupported {
		t.Fatalf("err = %v, want ErrPresignNotSupported", err)
	}
}
