package storage

import (
	"crypto/md5"
	"encoding/hex"
	"testing"

	"github.com/hkloudou/lake/v2/internal/index"
)

func TestEncodeCatalogPath(t *testing.T) {
	tests := []struct {
		catalog   string
		shardSize int
	}{
		// shardSize=4 (default, 65,536 directories)
		{"users", 4},
		{"Users", 4},
		{"USERS", 4},
		{"products", 4},
		{"a", 4},
		{"ab", 4},

		// shardSize=6 (overkill, 16M directories)
		{"users", 6},
		{"products", 6},
	}

	for _, tt := range tests {
		result := encodeCatalogPath(tt.catalog, tt.shardSize)

		// Calculate expected path: md5[0:shardSize]/hex(catalog)
		hash := md5.Sum([]byte(tt.catalog))
		md5Hash := hex.EncodeToString(hash[:])
		catalogHex := hex.EncodeToString([]byte(tt.catalog))

		expectedPath := md5Hash[0:tt.shardSize] + "/" + catalogHex

		if result != expectedPath {
			t.Errorf("encodeCatalogPath(%q, %d) = %q, want %q",
				tt.catalog, tt.shardSize, result, expectedPath)
		}

		t.Logf("catalog=%q, shardSize=%d -> md5=%q, catalogHex=%q -> path=%q",
			tt.catalog, tt.shardSize, md5Hash, catalogHex, result)
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
	// Test different shard sizes with MD5 prefix + hex suffix
	catalog := "products"
	hash := md5.Sum([]byte(catalog))
	md5Hash := hex.EncodeToString(hash[:])
	catalogHex := hex.EncodeToString([]byte(catalog))

	t.Logf("catalog=%q -> MD5=%q, hex=%q", catalog, md5Hash, catalogHex)

	// Test size=4 (recommended, 65,536 dirs)
	path4 := encodeCatalogPath(catalog, 4)
	expected4 := md5Hash[0:4] + "/" + catalogHex
	t.Logf("shardSize=4: %q (65,536 dirs) ✅ recommended", path4)
	t.Logf("  Format: md5[0:4]/hex(catalog)")
	if path4 != expected4 {
		t.Errorf("Size=4: got %q, want %q", path4, expected4)
	}

	// Test size=6 (overkill, 16M dirs)
	path6 := encodeCatalogPath(catalog, 6)
	expected6 := md5Hash[0:6] + "/" + catalogHex
	t.Logf("shardSize=6: %q (16M dirs) ⚠️ overkill", path6)
	if path6 != expected6 {
		t.Errorf("Size=6: got %q, want %q", path6, expected6)
	}

	t.Log("✓ MD5 prefix + hex suffix working correctly")
	t.Log("✓ Recommendation: use shardSize=4 (65,536 dirs sufficient)")
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
