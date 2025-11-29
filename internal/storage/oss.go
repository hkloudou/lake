package storage

/*
Directory Structure Design Philosophy (OSS/Object Storage):

Object storage (OSS/S3/MinIO) is fundamentally different from traditional filesystems:
  - No real "directories" - keys are flat strings with "/" as part of the name
  - No performance penalty for millions of objects under same prefix
  - Direct key access (Get/Put) is O(1) regardless of prefix depth or sibling count

Design Goals:
  1. Short key length (reduces storage cost and request size)
  2. Uniform distribution (avoid hot partitions)
  3. Enable lifecycle management via Redis (not via prefix)
  4. Keep structure simple since directory count doesn't matter

Structure: {md5[0:4]}/{hex(catalog)}/{file}
Example:   f9aa/5573657273/1700000000_123_1.dat

Breakdown:
  - md5[0:4]: 65,536 shards (distributed storage load balancing, avoid hot spots)
  - hex(catalog): Catalog identifier (encoded for OSS path safety)
  - file: Direct filename with timestamp (no yearMonth - lifecycle managed by Redis)

Why NO yearMonth:
  - Cleanup is handled by Redis index (knows exact keys to delete)
  - No List operations on OSS (all keys come from Redis)
  - Infinite accumulation is fine - object storage handles billions of objects
  - Adding yearMonth would only increase key length without benefit

Why NO hash sharding on filename:
  - Not needed - object storage has no "files per directory" limit
  - Direct key access doesn't care about sibling count
  - Simpler structure = shorter keys = lower cost

Performance:
  - Direct access: O(1) regardless of total objects
  - List by prefix: Rarely used (keys come from Redis)
  - Billions of objects: No problem for object storage
*/

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/hkloudou/lake/v2/internal/index"
)

// OSSStorage implements Storage interface for Aliyun OSS
type ossStorage struct {
	name     string
	client   *oss.Client
	bucket   *oss.Bucket
	endpoint string
	aesKey   []byte // AES encryption key
	mu       sync.RWMutex
}

// OSSConfig holds OSS configuration
type OSSConfig struct {
	Name      string
	Endpoint  string // OSS endpoint (e.g., "oss-cn-hangzhou")
	Bucket    string // Bucket name
	AccessKey string // Access key
	SecretKey string // Secret key
	AESKey    string // AES encryption key
	Internal  bool   // Use internal endpoint
}

// NewOSSStorage creates a new OSS storage instance
func NewOSSStorage(cfg OSSConfig) (*ossStorage, error) {
	// Build endpoint URL
	endpoint := cfg.Endpoint
	if cfg.Internal {
		endpoint = endpoint + "-internal"
	}
	if !strings.HasPrefix(endpoint, "http") {
		// step1: check if FC_REGION is set and if the endpoint contains the FC_REGION and the endpoint does not contain -internal
		fcRegion := os.Getenv("FC_REGION")
		if fcRegion != "" && strings.Contains(endpoint, fcRegion) && !strings.Contains(endpoint, "-internal") {
			endpoint = endpoint + "-internal"
		}

		//end step: fill the endpoint with the region
		endpoint = fmt.Sprintf("https://%s.aliyuncs.com", endpoint)
	}

	// Create OSS client
	client, err := oss.New(endpoint, cfg.AccessKey, cfg.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create OSS client: %w", err)
	}

	// Get bucket
	bucket, err := client.Bucket(cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket: %w", err)
	}

	return &ossStorage{
		client:   client,
		bucket:   bucket,
		name:     cfg.Name,
		endpoint: endpoint,
		aesKey:   []byte(cfg.AESKey),
	}, nil
}

// Put stores data with the given key (with compression and AES encryption)
func (s *ossStorage) Put(ctx context.Context, key string, data []byte) error {
	s.mu.RLock()
	bucket := s.bucket
	aesKey := s.aesKey
	s.mu.RUnlock()

	// Compress and encrypt data
	dataToWrite, err := compressAndEncrypt(data, aesKey)
	if err != nil {
		return err
	}

	return bucket.PutObject(key, bytes.NewReader(dataToWrite))
}

// Get retrieves data by key (with AES decryption and decompression)
func (s *ossStorage) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.RLock()
	bucket := s.bucket
	aesKey := s.aesKey
	s.mu.RUnlock()

	reader, err := bucket.GetObject(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	// Decrypt and decompress data
	return decryptAndDecompress(data, aesKey)
}

// Delete removes data by key
// Returns nil if the object doesn't exist (idempotent delete)
func (s *ossStorage) Delete(ctx context.Context, key string) error {
	s.mu.RLock()
	bucket := s.bucket
	s.mu.RUnlock()

	err := bucket.DeleteObject(key)
	if err != nil {
		// Check if error is "object not found" - treat as success (idempotent)
		if ossErr, ok := err.(oss.ServiceError); ok {
			if ossErr.StatusCode == 404 || ossErr.Code == "NoSuchKey" {
				return nil
			}
		}
		return err
	}
	return nil
}

// Exists checks if key exists
func (s *ossStorage) Exists(ctx context.Context, key string) (bool, error) {
	s.mu.RLock()
	bucket := s.bucket
	s.mu.RUnlock()

	exists, err := bucket.IsObjectExist(key)
	if err != nil {
		return false, fmt.Errorf("failed to check existence: %w", err)
	}
	return exists, nil
}

// List lists all keys with the given prefix
func (s *ossStorage) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.RLock()
	bucket := s.bucket
	s.mu.RUnlock()

	var keys []string
	marker := ""

	for {
		result, err := bucket.ListObjects(oss.Prefix(prefix), oss.Marker(marker))
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range result.Objects {
			keys = append(keys, obj.Key)
		}

		if !result.IsTruncated {
			break
		}
		marker = result.NextMarker
	}

	return keys, nil
}

func (s *ossStorage) RedisPrefix() string {
	return fmt.Sprintf("%s:%s", "oss", s.name)
}

// MakeDeltaKey generates storage key for data files with MD5-sharded path
// Format: {md5[0:4]}/{hex(catalog)}/{ts}_{seqid}_{mergeTypeInt}.dat
// Example: f9aa/5573657273/1700000000_123_1.dat (for catalog "Users")
func (s *ossStorage) MakeDeltaKey(catalog string, tsSeqID index.TimeSeqID, mergeType int) string {
	shardedPath := encodeOssCatalogPath(catalog, 4) // Default: 4-char MD5 prefix (65,536 dirs)
	return fmt.Sprintf("%s/%s_%d.dat", shardedPath, tsSeqID.String(), mergeType)
}

// MakeSnapKey generates storage key for snapshot files with MD5-sharded path
// Format: {md5[0:4]}/{hex(catalog)}/{startTsSeq}~{stopTsSeq}.snap
// Example: f9aa/5573657273/1700000000_1~1700000100_500.snap (for catalog "Users")
func (s *ossStorage) MakeSnapKey(catalog string, startTsSeq, stopTsSeq index.TimeSeqID) string {
	shardedPath := encodeOssCatalogPath(catalog, 4) // Default: 4-char MD5 prefix (65,536 dirs)
	return fmt.Sprintf("%s/%s~%s.snap", shardedPath, startTsSeq.String(), stopTsSeq.String())
}

// encodeOssCatalogPath generates OSS path with MD5 sharding
// Format: md5(catalog)[0:shardSize]/EncodeOssCatalogName(catalog)
func encodeOssCatalogPath(catalog string, shardSize int) string {
	hash := md5.Sum([]byte(catalog))
	md5Prefix := hex.EncodeToString(hash[:])[0:shardSize]
	catalogEncoded := encodeOssCatalogName(catalog)
	return md5Prefix + "/" + catalogEncoded
}

// IsOssLowerSafe checks if catalog contains only lowercase safe characters
// Allows: a-z, 0-9, -, _, /, .
func isOssLowerSafe(catalog string) bool {
	if len(catalog) == 0 {
		return false
	}
	for _, r := range catalog {
		if !((r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == '/' || r == '.') {
			return false
		}
	}
	return true
}

// IsOssUpperSafe checks if catalog contains only uppercase safe characters
// Allows: A-Z, 0-9, -, _, /, .
func isOssUpperSafe(catalog string) bool {
	if len(catalog) == 0 {
		return false
	}
	for _, r := range catalog {
		if !((r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == '/' || r == '.') {
			return false
		}
	}
	return true
}

// encodeOssCatalogName encodes catalog name for OSS paths
// Returns the encoded name with prefix for type identification
func encodeOssCatalogName(catalog string) string {
	// Check if all lowercase safe
	if isOssLowerSafe(catalog) {
		// Prefix: ( for lowercase
		return "(" + catalog
	}

	// Check if all uppercase safe
	if isOssUpperSafe(catalog) {
		// Prefix: ) for uppercase
		return ")" + catalog
	}

	// Mixed case or unsafe characters: use base32 lowercase
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString([]byte(catalog))
	return strings.ToLower(encoded)
}
