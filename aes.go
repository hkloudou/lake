package lake

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
)

func encrypt(plaintext []byte, key []byte) (ciphertext []byte, err error) {
	paddedKey := padKey(key)
	block, err := aes.NewCipher(paddedKey)
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

	ciphertext = gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

func decrypt(ciphertext []byte, key []byte) (plaintext []byte, err error) {
	paddedKey := padKey(key)
	block, err := aes.NewCipher(paddedKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]

	plaintext, err = gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

// padKey ensures the key is exactly 32 bytes long by either truncating or padding with zeros
func padKey(key []byte) []byte {
	paddedKey := make([]byte, 32)
	copy(paddedKey, key)
	return paddedKey
}
