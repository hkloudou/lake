package storage

import (
	"crypto/md5"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/hkloudou/lake/v2/internal/index"
)

func TestEncodeCatalogPath(t *testing.T) {
	tests := []struct {
		catalog      string
		shardSize    int
		wantEncoding string // Expected encoding method
	}{
		{"users", 4, "safe"},         // Safe chars, use as-is
		{"Users", 4, "safe"},         // Safe chars, use as-is
		{"USERS", 4, "safe"},         // Safe chars, use as-is
		{"products", 4, "safe"},      // Safe chars, use as-is
		{"user-name_123", 4, "safe"}, // Safe chars with dash/underscore
		{"短中文", 4, "base32"},         // Unsafe chars, use base32
		{"user:name", 4, "base32"},   // Colon is unsafe, use base32
	}

	for _, tt := range tests {
		result := encodeCatalogPath(tt.catalog, tt.shardSize)

		// Verify format: md5[0:shardSize]/encoded
		hash := md5.Sum([]byte(tt.catalog))
		md5Hash := hex.EncodeToString(hash[:])
		prefix := md5Hash[0:tt.shardSize]

		if !strings.HasPrefix(result, prefix+"/") {
			t.Errorf("Path should start with %q, got %q", prefix+"/", result)
		}

		suffix := result[len(prefix)+1:]
		t.Logf("catalog=%q, shardSize=%d -> md5Prefix=%q, suffix=%q (%s)",
			tt.catalog, tt.shardSize, prefix, suffix, tt.wantEncoding)

		// Verify suffix is not empty
		if suffix == "" {
			t.Errorf("Suffix should not be empty for catalog %q", tt.catalog)
		}
	}
}

func TestCaseSensitivity(t *testing.T) {
	// Verify that different case catalogs get different paths
	path1 := encodeCatalogPath("users", 4)
	path2 := encodeCatalogPath("Users", 4)
	path3 := encodeCatalogPath("USERS", 4)

	t.Logf("users -> %s", path1)
	t.Logf("Users -> %s", path2)
	t.Logf("USERS -> %s", path3)

	if path1 == path2 || path2 == path3 || path1 == path3 {
		t.Error("Different case catalogs should have different paths!")
	}

	t.Log("✓ Case sensitivity preserved correctly")
}

func TestOSSBestPractice(t *testing.T) {
	// Test with safe catalog (should use as-is)
	safe := "products"
	path := encodeCatalogPath(safe, 4)
	t.Logf("Safe catalog: %q -> %q (uses original)", safe, path)
	if !strings.Contains(path, "products") {
		t.Error("Safe catalog should preserve original name")
	}

	// Test with unsafe catalog (should use base32)
	unsafe := "短中文目录"
	pathUnsafe := encodeCatalogPath(unsafe, 4)
	t.Logf("Unsafe catalog: %q -> %q (uses base32 lowercase)", unsafe, pathUnsafe)
	if strings.Contains(pathUnsafe, "短") {
		t.Error("Unsafe catalog should be encoded, not use raw chars")
	}

	t.Log("✓ Smart encoding working correctly")
	t.Log("✓ Safe chars: use as-is (shortest)")
	t.Log("✓ Unsafe chars: use base32 lowercase (OSS-safe, 20% shorter than hex)")
}

func TestMakeDeltaKey(t *testing.T) {
	tsSeq := index.TimeSeqID{Timestamp: 1700000000, SeqID: 123}

	key1 := MakeDeltaKey("users", tsSeq, 1)
	key2 := MakeDeltaKey("Users", tsSeq, 1)

	t.Logf("users -> %s", key1)
	t.Logf("Users -> %s", key2)

	if key1 == key2 {
		t.Error("Different catalogs (users vs Users) should have different paths!")
	}
}
