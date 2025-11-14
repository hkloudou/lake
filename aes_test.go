package lake

import (
	"errors"
	"io"
	"testing"
)

func TestDecryptShortCiphertext(t *testing.T) {
	_, err := decrypt([]byte{1, 2, 3}, []byte("key"))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}
