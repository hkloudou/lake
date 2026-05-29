// Package mem is an in-memory storage backend for tests and local dev.
// One Store vends many buckets; it does not support presign.
package mem

import (
	"context"
	"fmt"
	"sync"

	"github.com/hkloudou/lake/v3/storage"
)

// Store is a process-local object store.
type Store struct {
	mu sync.RWMutex
	m  map[string][]byte // "bucket/path" -> data
}

func New() *Store { return &Store{m: map[string][]byte{}} }

// Bucket returns a bucket-scoped Storage; use it inside a storage.Resolver.
func (s *Store) Bucket(name string) storage.Storage { return &view{s: s, bucket: name} }

type view struct {
	s      *Store
	bucket string
}

func (v *view) key(path string) string { return v.bucket + "/" + path }

// Get / Put ignore catalog (path locates fully); it is part of the contract
// only as backend context.
func (v *view) Get(_ context.Context, _ /*catalog*/, path string) ([]byte, error) {
	v.s.mu.RLock()
	data, ok := v.s.m[v.key(path)]
	v.s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("mem: not found: %s", v.key(path))
	}
	return append([]byte(nil), data...), nil
}

func (v *view) Put(_ context.Context, _ /*catalog*/, path string, data []byte) error {
	cp := append([]byte(nil), data...)
	v.s.mu.Lock()
	v.s.m[v.key(path)] = cp
	v.s.mu.Unlock()
	return nil
}
