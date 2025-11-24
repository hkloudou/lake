package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/hkloudou/lake/v2/internal/index"
)

// MemoryStorage is an in-memory implementation of Storage (for testing)
type memoryStorage struct {
	mu   sync.RWMutex
	name string
	data map[string][]byte
}

// NewMemoryStorage creates a new in-memory storage
func NewMemoryStorage(name string) *memoryStorage {
	return &memoryStorage{
		data: make(map[string][]byte),
		name: name,
	}
}

func (m *memoryStorage) Put(ctx context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Make a copy to avoid external modifications
	copied := make([]byte, len(data))
	copy(copied, data)
	m.data[key] = copied
	return nil
}

func (m *memoryStorage) Get(ctx context.Context, key string) ([]byte, error) {
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

func (m *memoryStorage) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	return nil
}

func (m *memoryStorage) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.data[key]
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

func (m *memoryStorage) RedisPrefix() string {
	return fmt.Sprintf("memory:%s", m.name)
}

// MakeDeltaKey generates storage key for data files with MD5-sharded path
// Format: {md5[0:4]}/{hex(catalog)}/delta/{ts}_{seqid}_{mergeTypeInt}.json
// Example: f9aa/5573657273/delta/1700000000_123_1.json (for catalog "Users")
func (s *memoryStorage) MakeDeltaKey(catalog string, tsSeqID index.TimeSeqID, mergeType int) string {
	return fmt.Sprintf("%s/delta/%s_%d.json", catalog, tsSeqID.String(), mergeType)
}

// MakeSnapKey generates storage key for snapshot files with MD5-sharded path
// Format: {md5[0:4]}/{hex(catalog)}/snap/{startTsSeq}~{stopTsSeq}.snap
// Example: f9aa/5573657273/snap/1700000000_1~1700000100_500.snap (for catalog "Users")
func (s *memoryStorage) MakeSnapKey(catalog string, startTsSeq, stopTsSeq index.TimeSeqID) string {
	return fmt.Sprintf("%s/snap/%s~%s.snap", catalog, startTsSeq.String(), stopTsSeq.String())
}
