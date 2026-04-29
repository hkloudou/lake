package storage

// File-storage layout: "{md5(catalog)[0:2]}/{encodedCatalog}/{h1}/{h2}/{h3}/{filename}".
// Deeper than OSS because traditional filesystems (ext4/NTFS/XFS)
// degrade past ~10k entries per directory; the timestamp-derived
// h1/h2/h3 split keeps each leaf well under that cap.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hkloudou/lake/v3/internal/encrypt"
	"github.com/hkloudou/lake/v3/internal/index"
)

type fileStorage struct {
	name     string
	basePath string
	aesKey   []byte
}

// FileConfig holds local-file storage configuration.
type FileConfig struct {
	Name     string
	BasePath string // e.g. "/data/lake"
	AESKey   string
}

// NewFileStorage builds a local-file backend; mkdir -p the base path.
func NewFileStorage(cfg FileConfig) (*fileStorage, error) {
	base, err := filepath.Abs(cfg.BasePath)
	if err != nil {
		return nil, fmt.Errorf("resolve base path: %w", err)
	}
	if err := os.MkdirAll(base, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", base, err)
	}
	return &fileStorage{name: cfg.Name, basePath: base, aesKey: []byte(cfg.AESKey)}, nil
}

func (s *fileStorage) Put(ctx context.Context, key string, data []byte) error {
	enc, err := encrypt.Encrypt(data, s.aesKey)
	if err != nil {
		return err
	}
	full := filepath.Join(s.basePath, key)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	tmp := full + ".tmp"
	if err := os.WriteFile(tmp, enc, 0644); err != nil {
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
	return encrypt.Decrypt(data, s.aesKey)
}

func (s *fileStorage) Delete(ctx context.Context, key string) error {
	if err := os.Remove(filepath.Join(s.basePath, key)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

func (s *fileStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, err := os.Stat(filepath.Join(s.basePath, key))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (s *fileStorage) List(ctx context.Context, prefix string) ([]string, error) {
	root := filepath.Join(s.basePath, prefix)
	var keys []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || strings.HasSuffix(path, ".tmp") {
			return nil
		}
		rel, err := filepath.Rel(s.basePath, path)
		if err != nil {
			return err
		}
		keys = append(keys, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("walk: %w", err)
	}
	return keys, nil
}

func (s *fileStorage) RedisPrefix() string { return s.name }

func (s *fileStorage) MakeDeltaKey(catalog string, ts index.TimeSeqID, mergeType int) string {
	h1, h2, h3 := timeHash(ts)
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s_%d.dat",
		catalogMd5Prefix2(catalog), encodeOssCatalogName(catalog), h1, h2, h3, ts, mergeType)
}

func (s *fileStorage) MakeSnapKey(catalog string, stop index.TimeSeqID) string {
	h1, h2, h3 := timeHash(stop)
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s.snap",
		catalogMd5Prefix2(catalog), encodeOssCatalogName(catalog), h1, h2, h3, stop)
}
