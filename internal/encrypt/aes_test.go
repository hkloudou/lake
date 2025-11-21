package encrypt

import (
	"bytes"
	"testing"
)

func TestAesGcmEncryptDecrypt(t *testing.T) {
	key := []byte("my-secret-key")
	plaintext := []byte("Hello, World!")

	// Encrypt
	ciphertext, err := AesGcmEncrypt(plaintext, key)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	// Decrypt
	decrypted, err := AesGcmDecrypt(ciphertext, key)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	// Verify
	if !bytes.Equal(plaintext, decrypted) {
		t.Errorf("Decrypted text doesn't match original.\nGot: %s\nWant: %s", decrypted, plaintext)
	}
}

func TestPadKey(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected int
	}{
		{"short key", []byte("short"), 32},
		{"exact 32 bytes", make([]byte, 32), 32},
		{"long key", make([]byte, 64), 32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := padKey(tt.input)
			if len(result) != tt.expected {
				t.Errorf("padKey length = %d, want %d", len(result), tt.expected)
			}
		})
	}
}
