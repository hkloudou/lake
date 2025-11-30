package storage

import (
	"context"

	"github.com/hkloudou/lake/v2/internal/index"
)

// Storage is the interface for object storage (OSS/S3/Local)
type Storage interface {
	// Put stores data with the given key
	Put(ctx context.Context, key string, data []byte) error

	// Get retrieves data by key
	Get(ctx context.Context, key string) ([]byte, error)

	// Delete removes data by key
	Delete(ctx context.Context, key string) error

	// Exists checks if key exists
	Exists(ctx context.Context, key string) (bool, error)

	// List lists all keys with the given prefix
	List(ctx context.Context, prefix string) ([]string, error)

	RedisPrefix() string
	MakeDeltaKey(catalog string, tsSeqID index.TimeSeqID, mergeType int) string
	MakeSnapKey(catalog string, startTsSeq, stopTsSeq index.TimeSeqID) string
	MakeFileKey(catalog string, path string) string
}

// // MakeDeltaKey generates storage key for data files with MD5-sharded path
// // Format: {md5[0:4]}/{hex(catalog)}/delta/{ts}_{seqid}_{mergeTypeInt}.json
// // Example: f9aa/5573657273/delta/1700000000_123_1.json (for catalog "Users")
// func (s *ossStorage) MakeDeltaKey(catalog string, tsSeqID index.TimeSeqID, mergeType int) string {
// 	shardedPath := encode.EncodeOssCatalogPath(catalog, 4) // Default: 4-char MD5 prefix (65,536 dirs)
// 	return fmt.Sprintf("%s/delta/%s_%d.json", shardedPath, tsSeqID.String(), mergeType)
// }

// // MakeSnapKey generates storage key for snapshot files with MD5-sharded path
// // Format: {md5[0:4]}/{hex(catalog)}/snap/{startTsSeq}~{stopTsSeq}.snap
// // Example: f9aa/5573657273/snap/1700000000_1~1700000100_500.snap (for catalog "Users")
// func (s *ossStorage) MakeSnapKey(catalog string, startTsSeq, stopTsSeq index.TimeSeqID) string {
// 	shardedPath := encode.EncodeOssCatalogPath(catalog, 4) // Default: 4-char MD5 prefix (65,536 dirs)
// 	return fmt.Sprintf("%s/snap/%s~%s.snap", shardedPath, startTsSeq.String(), stopTsSeq.String())
// }

// StreamStorage extends Storage with streaming support
// type StreamStorage interface {
// 	Storage

// 	// PutStream stores data from a reader
// 	PutStream(ctx context.Context, key string, reader io.Reader, size int64) error

// 	// GetStream retrieves data as a reader
// 	GetStream(ctx context.Context, key string) (io.ReadCloser, error)
// }

// MakeKey generates storage key for catalog and file identifier
// For data files: catalog/{ts}_{seqid}_{mergeTypeInt}.json
// For snap files: catalog/{uuid}.json (legacy format)
// func MakeKey(catalog, identifier string) string {
// 	return catalog + "/" + identifier + ".json"
// }

// compressAndEncrypt compresses data with highest compression level and optionally encrypts it
// Returns the processed data ready for storage
// func compressAndEncrypt(data []byte, aesKey []byte) ([]byte, error) {
// 	// Step 1: Compress data with highest compression level
// 	var compressedBuf bytes.Buffer
// 	gzipWriter, err := gzip.NewWriterLevel(&compressedBuf, gzip.BestCompression)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to create gzip writer: %w", err)
// 	}
// 	if _, err := gzipWriter.Write(data); err != nil {
// 		gzipWriter.Close()
// 		return nil, fmt.Errorf("failed to compress data: %w", err)
// 	}
// 	if err := gzipWriter.Close(); err != nil {
// 		return nil, fmt.Errorf("failed to close gzip writer: %w", err)
// 	}
// 	compressedData := compressedBuf.Bytes()

// 	// Step 2: Encrypt data if AES key is provided
// 	if len(aesKey) > 0 {
// 		encrypted, err := encrypt.AesGcmEncrypt(compressedData, aesKey)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to encrypt data: %w", err)
// 		}
// 		return encrypted, nil
// 	}

// 	return compressedData, nil
// }

// // decryptAndDecompress decrypts data (if encrypted) and decompresses it
// // Returns the original data
// func decryptAndDecompress(data []byte, aesKey []byte) ([]byte, error) {
// 	// Step 1: Decrypt data if AES key is provided
// 	var compressedData []byte
// 	if len(aesKey) > 0 {
// 		decrypted, err := encrypt.AesGcmDecrypt(data, aesKey)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to decrypt data: %w", err)
// 		}
// 		compressedData = decrypted
// 	} else {
// 		compressedData = data
// 	}

// 	// Step 2: Decompress data
// 	gzipReader, err := gzip.NewReader(bytes.NewReader(compressedData))
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
// 	}
// 	defer gzipReader.Close()

// 	decompressedData, err := io.ReadAll(gzipReader)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to decompress data: %w", err)
// 	}

// 	return decompressedData, nil
// }
