package storage

/*
Directory Structure Design Philosophy (File Storage):

Traditional filesystem requires careful balance between directory depth and files per directory.

Design Goals:
  1. Keep files per directory under 10k (optimal for ext4/NTFS/XFS performance)
  2. Minimize directory depth (reduce inode lookups)
  3. Ensure uniform distribution across all levels
  4. Prevent conflicts and enable long-term accumulation

Structure: {md5[0:2]}/{hex(catalog)}/{hash1}/{hash2}/{hash3}/{file}
Example:   f9/5573657273/ab/cd/ef/1700000000_123_1.dat

Breakdown:
  - md5[0:2]: 256 dirs (catalog distribution, prevents single dir with too many catalogs)
  - hex(catalog): Catalog-specific directory (~391 catalogs per md5 shard for 100k catalogs)
  - hash1/hash2/hash3: 256×256×256=16.7M leaf dirs (timestamp-based sharding)
    * Hash from timestamp low 24 bits (cycles every ~194 days)
    * Filename contains full timestamp, so no collision despite hash cycle

Performance:
  - Each level: ≤4000 dirs/files (filesystem-friendly)
  - Leaf directory: ~999 files per 194-day cycle
  - 10-year accumulation: ~18,780 files per leaf (still excellent)

Why This Design:
  - Traditional filesystems struggle with >10k entries per directory
  - Prioritize controlling per-directory count over depth
  - 6 levels is acceptable (path limit: 4096 chars on Linux, 260 chars on Windows)
*/

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/hkloudou/lake/v2/internal/index"
)

// FileStorage implements Storage interface for local file system
type fileStorage struct {
	name     string
	basePath string // Base directory path for storage
	aesKey   []byte // AES encryption key
	mu       sync.RWMutex
}

// FileConfig holds file storage configuration
type FileConfig struct {
	Name     string // Storage name
	BasePath string // Base directory path (e.g., "/data/lake" or "./storage")
	AESKey   string // AES encryption key
}

// NewFileStorage creates a new file storage instance
func NewFileStorage(cfg FileConfig) (*fileStorage, error) {
	// Normalize base path
	basePath, err := filepath.Abs(cfg.BasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve base path: %w", err)
	}

	// Create base directory if it doesn't exist
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &fileStorage{
		name:     cfg.Name,
		basePath: basePath,
		aesKey:   []byte(cfg.AESKey),
	}, nil
}

// Put stores data with the given key (with compression and AES encryption)
func (s *fileStorage) Put(ctx context.Context, key string, data []byte) error {
	s.mu.RLock()
	aesKey := s.aesKey
	basePath := s.basePath
	s.mu.RUnlock()

	// Compress and encrypt data
	dataToWrite, err := compressAndEncrypt(data, aesKey)
	if err != nil {
		return err
	}

	// Get full file path and create directory if needed
	fullPath := filepath.Join(basePath, key)
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write data to file atomically (write to temp file then rename)
	tmpFile := fullPath + ".tmp"
	if err := os.WriteFile(tmpFile, dataToWrite, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	if err := os.Rename(tmpFile, fullPath); err != nil {
		os.Remove(tmpFile) // Clean up on error
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// Get retrieves data by key (with AES decryption and decompression)
func (s *fileStorage) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.RLock()
	aesKey := s.aesKey
	basePath := s.basePath
	s.mu.RUnlock()

	fullPath := filepath.Join(basePath, key)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("key not found: %s", key)
		}
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Decrypt and decompress data
	return decryptAndDecompress(data, aesKey)
}

// Delete removes data by key
func (s *fileStorage) Delete(ctx context.Context, key string) error {
	s.mu.RLock()
	basePath := s.basePath
	s.mu.RUnlock()

	fullPath := filepath.Join(basePath, key)
	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			return nil // Already deleted, treat as success
		}
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}

// Exists checks if key exists
func (s *fileStorage) Exists(ctx context.Context, key string) (bool, error) {
	s.mu.RLock()
	basePath := s.basePath
	s.mu.RUnlock()

	fullPath := filepath.Join(basePath, key)
	_, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check file existence: %w", err)
	}
	return true, nil
}

// List lists all keys with the given prefix
func (s *fileStorage) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.RLock()
	basePath := s.basePath
	s.mu.RUnlock()

	searchPath := filepath.Join(basePath, prefix)
	var keys []string

	err := filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Skip directories and temp files
		if info.IsDir() || strings.HasSuffix(path, ".tmp") {
			return nil
		}

		// Get relative path from basePath
		relPath, err := filepath.Rel(basePath, path)
		if err != nil {
			return err
		}
		// Normalize path separators to forward slashes (like OSS)
		relPath = filepath.ToSlash(relPath)
		keys = append(keys, relPath)
		return nil
	})

	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil // Prefix doesn't exist, return empty list
		}
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	return keys, nil
}

func (s *fileStorage) RedisPrefix() string {
	return fmt.Sprintf("%s:%s", "file", s.name)
}

// MakeDeltaKey generates storage key for data files with MD5-sharded path
// Format: {md5[0:2]}/{hex(catalog)}/{hash1}/{hash2}/{hash3}/{ts}_{seqid}_{mergeTypeInt}.dat
// Example: f9/5573657273/ab/cd/ef/1700000000_123_1.dat (for catalog "Users")
// Sharding: md5[0:2]=256 dirs, hash1/hash2/hash3=256×256×256 leaf dirs, ~1k files/dir (194 days)
func (s *fileStorage) MakeDeltaKey(catalog string, tsSeqID index.TimeSeqID, mergeType int) string {
	md5Prefix := getCatalogMd5Prefix0xff(catalog)
	catalogEncoded := encodeOssCatalogName(catalog)
	hash1, hash2, hash3 := getTimeHash(tsSeqID)
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s_%d.dat", md5Prefix, catalogEncoded, hash1, hash2, hash3, tsSeqID.String(), mergeType)
}

// MakeSnapKey generates storage key for snapshot files with MD5-sharded path
// Format: {md5[0:2]}/{hex(catalog)}/{hash1}/{hash2}/{hash3}/{startTsSeq}~{stopTsSeq}.snap
// Example: f9/5573657273/ab/cd/ef/1700000000_1~1700000100_500.snap (for catalog "Users")
// Sharding: md5[0:2]=256 dirs, hash1/hash2/hash3=256×256×256 leaf dirs, ~1k files/dir (194 days)
func (s *fileStorage) MakeSnapKey(catalog string, startTsSeq, stopTsSeq index.TimeSeqID) string {
	md5Prefix := getCatalogMd5Prefix0xff(catalog)
	catalogEncoded := encodeOssCatalogName(catalog)
	hash1, hash2, hash3 := getTimeHash(stopTsSeq)
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s~%s.snap", md5Prefix, catalogEncoded, hash1, hash2, hash3, startTsSeq.String(), stopTsSeq.String())
}
