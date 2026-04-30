package lake

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/hkloudou/lake/v3/internal/storage"
)

// initedClient builds a Client whose ensureInitialized succeeds without
// reaching Redis (memory storage injected). Tests that don't need a
// real Redis use this to focus on BatchSample's branching logic.
func initedClient(t *testing.T) *Client {
	t.Helper()
	c := NewLake("127.0.0.1:1", WithStorage(storage.NewMemoryStorage("test")))
	t.Cleanup(func() { _ = c.Close() })
	if err := c.ensureInitialized(context.Background()); err != nil {
		t.Fatalf("ensureInitialized: %v", err)
	}
	return c
}

// TestBatchSample_EmptyInput: zero catalogs in → empty map out, no panics.
func TestBatchSample_EmptyInput(t *testing.T) {
	out := BatchSample[int](context.Background(), nil, "x", func(*ListResult) (int, error) { return 0, nil })
	if len(out) != 0 {
		t.Fatalf("expected empty result, got %v", out)
	}
}

// TestBatchSample_PreservesPerCatalogErrors: list-level errors and
// HasPending must surface on their own catalog only, never invoke
// loader, and not be overridden by post-init paths.
func TestBatchSample_PreservesPerCatalogErrors(t *testing.T) {
	c := initedClient(t)

	listErr := errors.New("list-failed")
	lists := map[string]*ListResult{
		"with-err": {client: c, catalog: "with-err", Err: listErr},
	}
	loaderCalls := atomic.Int32{}
	out := BatchSample[int](context.Background(), lists, "daily", func(*ListResult) (int, error) {
		loaderCalls.Add(1)
		return 1, nil
	})

	if !errors.Is(out["with-err"].Err, listErr) {
		t.Errorf("expected list err to surface, got %v", out["with-err"].Err)
	}
	if loaderCalls.Load() != 0 {
		t.Errorf("loader must not run for already-erroring lists; ran %d times", loaderCalls.Load())
	}
}

// TestBatchSample_NilListEntry: a nil ListResult value yields a per-catalog
// error rather than a panic.
func TestBatchSample_NilListEntry(t *testing.T) {
	c := initedClient(t)
	lists := map[string]*ListResult{
		"valid": {client: c, catalog: "valid", Err: errors.New("dummy")},
		"nil":   nil,
	}
	out := BatchSample[int](context.Background(), lists, "x", func(*ListResult) (int, error) { return 0, nil })
	if out["nil"].Err == nil {
		t.Fatal("expected error for nil list entry")
	}
}

// TestBatchSample_NoClientFallback: a map full of plain ListResults with
// nil clients (degenerate input) returns errors for all entries without
// panicking.
func TestBatchSample_NoClientFallback(t *testing.T) {
	lists := map[string]*ListResult{
		"a": {catalog: "a"},
		"b": {catalog: "b"},
	}
	out := BatchSample[int](context.Background(), lists, "x", func(*ListResult) (int, error) { return 0, nil })
	if len(out) != 2 || out["a"].Err == nil || out["b"].Err == nil {
		t.Fatalf("expected per-catalog errors when no client is available, got %+v", out)
	}
}
