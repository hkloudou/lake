package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/hkloudou/lake/v2/internal/encrypt"
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

// Put stores data with the given key (with AES encryption)
func (s *ossStorage) Put(ctx context.Context, key string, data []byte) error {
	s.mu.RLock()
	bucket := s.bucket
	aesKey := s.aesKey
	s.mu.RUnlock()

	// Encrypt data if AES key is provided
	var dataToWrite []byte
	if len(aesKey) > 0 {
		encrypted, err := encrypt.AesGcmEncrypt(data, aesKey)
		if err != nil {
			return fmt.Errorf("failed to encrypt data: %w", err)
		}
		dataToWrite = encrypted
	} else {
		dataToWrite = data
	}

	return bucket.PutObject(key, bytes.NewReader(dataToWrite))
}

// Get retrieves data by key (with AES decryption)
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

	// Decrypt data if AES key is provided
	if len(aesKey) > 0 {
		decrypted, err := encrypt.AesGcmDecrypt(data, aesKey)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt data: %w", err)
		}
		return decrypted, nil
	}

	return data, nil
}

// Delete removes data by key
func (s *ossStorage) Delete(ctx context.Context, key string) error {
	s.mu.RLock()
	bucket := s.bucket
	s.mu.RUnlock()

	return bucket.DeleteObject(key)
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

// PutStream stores data from a reader
// func (s *OSSStorage) PutStream(ctx context.Context, key string, reader io.Reader, size int64) error {
// 	s.mu.RLock()
// 	bucket := s.bucket
// 	s.mu.RUnlock()

// 	return bucket.PutObject(key, reader)
// }

// // GetStream retrieves data as a reader
// func (s *OSSStorage) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
// 	s.mu.RLock()
// 	bucket := s.bucket
// 	s.mu.RUnlock()

// 	return bucket.GetObject(key)
// }
