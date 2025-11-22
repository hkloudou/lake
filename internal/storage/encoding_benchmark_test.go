package storage

import (
	"crypto/md5"
	"encoding/base32"
	"encoding/hex"
	"strings"
	"testing"
)

// Benchmark different encoding methods
func BenchmarkEncodings(b *testing.B) {
	catalog := "users"

	b.Run("hex", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = hex.EncodeToString([]byte(catalog))
		}
	})

	b.Run("base32_lowercase", func(b *testing.B) {
		encoder := base32.StdEncoding.WithPadding(base32.NoPadding)
		for i := 0; i < b.N; i++ {
			result := encoder.EncodeToString([]byte(catalog))
			_ = strings.ToLower(result)
		}
	})

	b.Run("md5_then_hex", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			hash := md5.Sum([]byte(catalog))
			_ = hex.EncodeToString(hash[:])
		}
	})
}

func TestEncodingComparison(t *testing.T) {
	tests := []string{
		"users",
		"USER",
		"Users",
		"very-long-catalog-name-with-special-chars-12345",
		"短中文目录",
	}

	for _, catalog := range tests {
		// 1. Hex (current)
		// hexResult := hex.EncodeToString([]byte(catalog))

		// 2. Base32 lowercase (no case issues)
		// encoder := base32.StdEncoding.WithPadding(base32.NoPadding)
		// b32Result := strings.ToLower(encoder.EncodeToString([]byte(catalog)))

		// 3. Safe chars only (alphanumeric + dash/underscore)
		safeResult := encodeCatalogName(catalog)

		t.Logf("\nCatalog: %q", catalog)
		// t.Logf("  Hex:          %s (len=%d)", hexResult, len(hexResult))
		// t.Logf("  Base32Lower:  %s (len=%d)", b32Result, len(b32Result))
		t.Logf("  Safe:         %s (len=%d)", safeResult, len(safeResult))
	}
}

// makeSafe converts catalog to safe filename using only alphanumeric and allowed chars
// Falls back to hex for unsafe characters
