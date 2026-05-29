package storage

// File-storage layout: "{md5(catalog)[0:2]}/{encodedCatalog}/{h1}/{h2}/{h3}/{filename}".
// Deeper than OSS because traditional filesystems degrade past ~10k
// entries per directory; the timestamp-derived h1/h2/h3 split keeps
// each leaf under that cap.
//
// Bodies are stored RAW (no Lake-side gzip / AES). The File backend
// does NOT support presigned uploads — direct-upload writes only work
// against OSS-class backends.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hkloudou/lake/v3/internal/index"
)

type fileStorage struct {
	name     string
	basePath string
}

// FileConfig holds local-file storage configuration.
type FileConfig struct {
	Name     string
	BasePath string
}

func NewFileStorage(cfg FileConfig) (*fileStorage, error) {
	base, err := filepath.Abs(cfg.BasePath)
	if err != nil {
		return nil, fmt.Errorf("resolve base path: %w", err)
	}
	if err := os.MkdirAll(base, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", base, err)
	}
	return &fileStorage{name: cfg.Name, basePath: base}, nil
}

func (s *fileStorage) Put(ctx context.Context, key string, data []byte) error {
	full := filepath.Join(s.basePath, key)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	tmp := full + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := os.Rename(tmp, full); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

func (s *fileStorage) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(s.basePath, key))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("key not found: %s", key)
		}
		return nil, fmt.Errorf("read %s: %w", key, err)
	}
	return data, nil
}

func (s *fileStorage) Delete(ctx context.Context, key string) error {
	if err := os.Remove(filepath.Join(s.basePath, key)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

func (s *fileStorage) RedisPrefix() string { return s.name }

func (s *fileStorage) MakeDeltaKey(catalog, uuid string) string {
	return fmt.Sprintf("%s/%s/%s.dat",
		catalogMd5Prefix2(catalog), encodeOssCatalogName(catalog), uuid)
}

func (s *fileStorage) MakeSnapKey(catalog string, stop index.TimeSeqID) string {
	h1, h2, h3 := timeHash(stop)
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s.snap",
		catalogMd5Prefix2(catalog), encodeOssCatalogName(catalog), h1, h2, h3, stop)
}
