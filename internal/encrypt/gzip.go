package encrypt

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// Encrypt gzips data, then optionally AES-GCM-encrypts it (when aesKey
// is non-empty). The reverse pipeline is in Decrypt.
func Encrypt(data, aesKey []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, fmt.Errorf("gzip writer: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, fmt.Errorf("gzip write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	if len(aesKey) == 0 {
		return buf.Bytes(), nil
	}
	out, err := aesGcmEncrypt(buf.Bytes(), aesKey)
	if err != nil {
		return nil, fmt.Errorf("aes encrypt: %w", err)
	}
	return out, nil
}

// Decrypt is Encrypt's inverse: optional AES-GCM-decrypt then gunzip.
func Decrypt(data, aesKey []byte) ([]byte, error) {
	if len(aesKey) > 0 {
		dec, err := aesGcmDecrypt(data, aesKey)
		if err != nil {
			return nil, fmt.Errorf("aes decrypt: %w", err)
		}
		data = dec
	}
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("gzip read: %w", err)
	}
	return out, nil
}
