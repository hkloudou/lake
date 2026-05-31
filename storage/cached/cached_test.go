package cached

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hkloudou/lake/v3/storage"
)

// countingStore is a minimal in-memory storage.Storage that counts Get/Put so a
// test can assert exactly when the backend is (not) touched.
type countingStore struct {
	mu   sync.Mutex
	data map[string][]byte
	gets atomic.Int32
	puts atomic.Int32
}

func newCountingStore() *countingStore { return &countingStore{data: map[string][]byte{}} }

func (s *countingStore) Get(_ context.Context, _, path string) ([]byte, error) {
	s.gets.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[path]
	if !ok {
		return nil, fmt.Errorf("not found: %s", path)
	}
	return append([]byte(nil), v...), nil
}

func (s *countingStore) Put(_ context.Context, _, path string, data []byte) error {
	s.puts.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[path] = append([]byte(nil), data...)
	return nil
}

// presignStore adds presign capability to countingStore.
type presignStore struct{ *countingStore }

func (presignStore) PresignPut(context.Context, string, string, storage.PresignOptions) (storage.PresignedUpload, error) {
	return storage.PresignedUpload{URL: "x://upload", Method: "PUT"}, nil
}

// TestWrap_WriteThroughWarmsCache is the core property: a Put warms the cache,
// so the next Get of the same key is served WITHOUT a backend round-trip. This
// is exactly what spares a freshly saved snapshot its cold object-store GET.
func TestWrap_WriteThroughWarmsCache(t *testing.T) {
	base := newCountingStore()
	w := Wrap("oss|snaps", base, NewMemoryCache(time.Minute))
	ctx := context.Background()

	if err := w.Put(ctx, "users", "ab/cd/100.snap", []byte(`{"a":1}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := base.puts.Load(); got != 1 {
		t.Fatalf("backend puts = %d, want 1", got)
	}

	got, err := w.Get(ctx, "users", "ab/cd/100.snap")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != `{"a":1}` {
		t.Fatalf("Get = %s, want {\"a\":1}", got)
	}
	if n := base.gets.Load(); n != 0 {
		t.Fatalf("backend gets = %d, want 0 (served from write-through warm)", n)
	}
}

// TestWrap_ReadThroughCachesMiss covers the Get path: a miss hits the backend
// once and caches the result; subsequent Gets are served from cache.
func TestWrap_ReadThroughCachesMiss(t *testing.T) {
	base := newCountingStore()
	ctx := context.Background()
	// Seed the backend out-of-band (e.g. a delta uploaded via a presigned URL,
	// which never goes through this wrapper's Put).
	if err := base.Put(ctx, "users", "ab/cd/d.dat", []byte(`{"b":2}`)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	w := Wrap("oss|data", base, NewMemoryCache(time.Minute))

	for i := 0; i < 3; i++ {
		got, err := w.Get(ctx, "users", "ab/cd/d.dat")
		if err != nil {
			t.Fatalf("Get #%d: %v", i, err)
		}
		if string(got) != `{"b":2}` {
			t.Fatalf("Get #%d = %s, want {\"b\":2}", i, got)
		}
	}
	if n := base.gets.Load(); n != 1 {
		t.Fatalf("backend gets = %d, want 1 (miss once, then cached)", n)
	}
}

// TestWrap_PresignPassthrough pins that a caching wrapper never hides (nor
// fabricates) presign capability: the wrapper is a storage.Presigner iff the
// wrapped backend is. WriteBegin relies on this type assertion.
func TestWrap_PresignPassthrough(t *testing.T) {
	withPresign := Wrap("p|b", presignStore{newCountingStore()}, NewNoOpCache())
	if _, ok := withPresign.(storage.Presigner); !ok {
		t.Fatal("wrapped presign-capable backend must expose storage.Presigner")
	}

	noPresign := Wrap("p|b", newCountingStore(), NewNoOpCache())
	if _, ok := noPresign.(storage.Presigner); ok {
		t.Fatal("wrapped non-presign backend must NOT expose storage.Presigner")
	}
}

// TestResolver_PolicyRoutesCache verifies the combinator wraps only when policy
// returns a non-nil cache, leaving other backends untouched.
func TestResolver_PolicyRoutesCache(t *testing.T) {
	inner := func(_, _ string) (storage.Storage, error) { return newCountingStore(), nil }
	resolve := Resolver(inner, func(_, bucket string) Cache {
		if bucket == "cached" {
			return NewMemoryCache(time.Minute)
		}
		return nil
	})

	raw, err := resolve("oss", "raw")
	if err != nil {
		t.Fatalf("resolve raw: %v", err)
	}
	if _, ok := raw.(*countingStore); !ok {
		t.Fatalf("uncached bucket should return the base unwrapped, got %T", raw)
	}

	wrapped, err := resolve("oss", "cached")
	if err != nil {
		t.Fatalf("resolve cached: %v", err)
	}
	if _, ok := wrapped.(*countingStore); ok {
		t.Fatalf("cached bucket should be wrapped, got bare base %T", wrapped)
	}
}

// TestWrapIf_CachesByPath covers the shared-bucket case: with BySuffix(".snap")
// only snapshots are cached — snap reads are served from the write-through warm,
// while delta (.dat) reads always reach the backend.
func TestWrapIf_CachesByPath(t *testing.T) {
	base := newCountingStore()
	ctx := context.Background()
	w := WrapIf("oss|shared", base, NewMemoryCache(time.Minute), BySuffix(".snap"))

	// Snapshot (.snap): write-through warms the cache → repeated Gets skip the backend.
	if err := w.Put(ctx, "users", "ab/cd/100.snap", []byte(`{"s":1}`)); err != nil {
		t.Fatalf("Put snap: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := w.Get(ctx, "users", "ab/cd/100.snap"); err != nil {
			t.Fatalf("Get snap #%d: %v", i, err)
		}
	}
	if n := base.gets.Load(); n != 0 {
		t.Fatalf("snap backend gets = %d, want 0 (served from write-through warm)", n)
	}

	// Delta (.dat): predicate excludes it — Put does not warm, and every Get reaches the backend.
	if err := w.Put(ctx, "users", "ab/cd/d.dat", []byte(`{"d":2}`)); err != nil {
		t.Fatalf("Put delta: %v", err)
	}
	for i := 0; i < 3; i++ {
		got, err := w.Get(ctx, "users", "ab/cd/d.dat")
		if err != nil {
			t.Fatalf("Get delta #%d: %v", i, err)
		}
		if string(got) != `{"d":2}` {
			t.Fatalf("Get delta #%d = %s, want {\"d\":2}", i, got)
		}
	}
	if n := base.gets.Load(); n != 3 {
		t.Fatalf("delta backend gets = %d, want 3 (never cached)", n)
	}
}
