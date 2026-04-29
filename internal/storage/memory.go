package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/hkloudou/lake/v3/internal/index"
)

// memoryStorage is an in-memory Storage backend used by tests.
type memoryStorage struct {
	mu   sync.RWMutex
	name string
	data map[string][]byte
}

// NewMemoryStorage returns an empty in-memory storage with the given name.
func NewMemoryStorage(name string) *memoryStorage {
	return &memoryStorage{name: name, data: make(map[string][]byte)}
}

func (m *memoryStorage) Put(ctx context.Context, key string, data []byte) error {
	cp := append([]byte(nil), data...)
	m.mu.Lock()
	m.data[key] = cp
	m.mu.Unlock()
	return nil
}

func (m *memoryStorage) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	data, ok := m.data[key]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return append([]byte(nil), data...), nil
}

func (m *memoryStorage) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	delete(m.data, key)
	m.mu.Unlock()
	return nil
}

func (m *memoryStorage) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.RLock()
	_, ok := m.data[key]
	m.mu.RUnlock()
	return ok, nil
}

func (m *memoryStorage) List(ctx context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.data {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *memoryStorage) RedisPrefix() string { return m.name }

func (m *memoryStorage) MakeDeltaKey(catalog string, ts index.TimeSeqID, mergeType int) string {
	return fmt.Sprintf("%s/delta/%s_%d.dat", catalog, ts, mergeType)
}

func (m *memoryStorage) MakeSnapKey(catalog string, stop index.TimeSeqID) string {
	return fmt.Sprintf("%s/snap/%s.snap", catalog, stop)
}
