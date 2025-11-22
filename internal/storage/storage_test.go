package storage

import (
	"encoding/hex"
	"testing"

	"github.com/hkloudou/lake/v2/internal/index"
)

func TestEncodeCatalogPath(t *testing.T) {
	tests := []struct {
		catalog   string
		shardSize int
		wantPath  string
	}{
		// shardSize=4 (default, 65,536 directories)
		{"users", 4, "7573.657273"},          // hex: 7573657273
		{"Users", 4, "5573.657273"},          // hex: 5573657273 (different!)
		{"USERS", 4, "5553.455253"},          // hex: 5553455253 (different!)
		{"products", 4, "7072.6f6475637473"}, // hex: 70726f6475637473
		{"a", 4, "61"},                       // hex: 61 (<=4, no shard)
		{"ab", 4, "6162"},                    // hex: 6162 (<=4, no shard)
		{"abc", 4, "6162.63"},                // hex: 616263 (>4, sharded)
		{"abcd", 4, "6162.6364"},             // hex: 61626364 (>4, sharded)

		// shardSize=6 (overkill, 16M directories)
		{"users", 6, "757365.7273"},          // hex: 7573657273
		{"products", 6, "70726f.6475637473"}, // hex: 70726f6475637473
		{"ab", 6, "6162"},                    // hex: 6162 (<=6, no shard)
	}

	for _, tt := range tests {
		result := encodeCatalogPath(tt.catalog, tt.shardSize)
		hexFull := hex.EncodeToString([]byte(tt.catalog))
		t.Logf("catalog=%q, shardSize=%d -> hex=%q -> path=%q", tt.catalog, tt.shardSize, hexFull, result)

		if result != tt.wantPath {
			t.Errorf("encodeCatalogPath(%q, %d) = %q, want %q",
				tt.catalog, tt.shardSize, result, tt.wantPath)
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
	// Test different shard sizes
	catalog := "products"
	hexFull := hex.EncodeToString([]byte(catalog)) // "70726f6475637473"

	// Test size=4 (recommended)
	path4 := encodeCatalogPath(catalog, 4)
	expected4 := hexFull[0:4] + "." + hexFull[4:]
	t.Logf("shardSize=4: %q (65,536 dirs) ✅ recommended", path4)
	if path4 != expected4 {
		t.Errorf("Size=4: got %q, want %q", path4, expected4)
	}

	// Test size=6 (overkill)
	path6 := encodeCatalogPath(catalog, 6)
	expected6 := hexFull[0:6] + "." + hexFull[6:]
	t.Logf("shardSize=6: %q (16M dirs) ⚠️ overkill", path6)
	if path6 != expected6 {
		t.Errorf("Size=6: got %q, want %q", path6, expected6)
	}

	t.Log("✓ Configurable shard size working correctly")
	t.Log("✓ Recommendation: use shardSize=4 for most applications")
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
