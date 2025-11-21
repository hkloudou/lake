package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// MemoryStorage is an in-memory implementation of Storage (for testing)
type MemoryStorage struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryStorage creates a new in-memory storage
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[string][]byte),
	}
}

func (m *MemoryStorage) Put(ctx context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Make a copy to avoid external modifications
	copied := make([]byte, len(data))
	copy(copied, data)
	m.data[key] = copied
	return nil
}

func (m *MemoryStorage) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	// Return a copy to avoid external modifications
	copied := make([]byte, len(data))
	copy(copied, data)
	return copied, nil
}

func (m *MemoryStorage) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	return nil
}

func (m *MemoryStorage) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.data[key]
	return ok, nil
}

func (m *MemoryStorage) List(ctx context.Context, prefix string) ([]string, error) {
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
