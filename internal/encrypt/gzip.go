package encrypt

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

func Encrypt(data []byte, aesKey []byte) ([]byte, error) {
	// Step 1: Compress data with highest compression level
	var compressedBuf bytes.Buffer
	gzipWriter, err := gzip.NewWriterLevel(&compressedBuf, gzip.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip writer: %w", err)
	}
	if _, err := gzipWriter.Write(data); err != nil {
		gzipWriter.Close()
		return nil, fmt.Errorf("failed to compress data: %w", err)
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close gzip writer: %w", err)
	}
	compressedData := compressedBuf.Bytes()

	// Step 2: Encrypt data if AES key is provided
	if len(aesKey) > 0 {
		encrypted, err := aesGcmEncrypt(compressedData, aesKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt data: %w", err)
		}
		return encrypted, nil
	}

	return compressedData, nil
}

// AesGcmDecryptAndDecompress decrypts data (if encrypted) and decompresses it
// Returns the original data
func Decrypt(data []byte, aesKey []byte) ([]byte, error) {
	// Step 1: Decrypt data if AES key is provided
	var compressedData []byte
	if len(aesKey) > 0 {
		decrypted, err := aesGcmDecrypt(data, aesKey)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt data: %w", err)
		}
		compressedData = decrypted
	} else {
		compressedData = data
	}

	// Step 2: Decompress data
	gzipReader, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzipReader.Close()

	decompressedData, err := io.ReadAll(gzipReader)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress data: %w", err)
	}

	return decompressedData, nil
}
