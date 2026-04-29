package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
)

// aesGcmEncrypt seals plaintext with AES-GCM using a 32-byte key
// (zero-padded if shorter). The nonce is prefixed onto the ciphertext.
func aesGcmEncrypt(plaintext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(padKey(key))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// aesGcmDecrypt opens AES-GCM ciphertext (nonce-prefixed).
func aesGcmDecrypt(ciphertext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(padKey(key))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	n := gcm.NonceSize()
	if len(ciphertext) < n {
		return nil, io.ErrUnexpectedEOF
	}
	return gcm.Open(nil, ciphertext[:n], ciphertext[n:], nil)
}

// padKey forces the key into exactly 32 bytes (zero-padding short keys).
func padKey(key []byte) []byte {
	out := make([]byte, 32)
	copy(out, key)
	return out
}
