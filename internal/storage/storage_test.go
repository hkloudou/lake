package storage

import (
	"encoding/hex"
	"testing"

	"github.com/hkloudou/lake/v2/internal/index"
)

func TestEncodeCatalogPath(t *testing.T) {
	tests := []struct {
		catalog  string
		wantPath string
	}{
		{"users", "7573/7573657273"},          // hex: 7573657273, 4-char prefix
		{"Users", "5573/5573657273"},          // hex: 5573657273 (different!)
		{"USERS", "5553/5553455253"},          // hex: 5553455253 (different!)
		{"products", "7072/70726f6475637473"}, // hex: 70726f6475637473
		{"a", "61"},                           // hex: 61 (<=4 chars, no shard)
		{"ab", "6162"},                        // hex: 6162 (<=4 chars, no shard)
		{"abc", "616263"},                     // hex: 616263 (<=4 chars, no shard)
		{"abcd", "6162/61626364"},             // hex: 61626364 (>4 chars, shard)
	}

	for _, tt := range tests {
		result := encodeCatalogPath(tt.catalog)
		hexFull := hex.EncodeToString([]byte(tt.catalog))
		t.Logf("catalog=%q -> hex=%q -> path=%q", tt.catalog, hexFull, result)

		if result != tt.wantPath {
			t.Errorf("encodeCatalogPath(%q) = %q, want %q",
				tt.catalog, result, tt.wantPath)
		}
	}
}

func TestCaseSensitivity(t *testing.T) {
	// Verify that different case catalogs get different paths
	path1 := encodeCatalogPath("users")
	path2 := encodeCatalogPath("Users")
	path3 := encodeCatalogPath("USERS")

	t.Logf("users -> %s", path1)
	t.Logf("Users -> %s", path2)
	t.Logf("USERS -> %s", path3)

	if path1 == path2 || path2 == path3 || path1 == path3 {
		t.Error("Different case catalogs should have different paths!")
	}

	t.Log("✓ Case sensitivity preserved correctly")
}

func TestOSSBestPractice(t *testing.T) {
	// OSS best practice: hash[0:4]/hash format
	catalog := "products"
	path := encodeCatalogPath(catalog)
	hexFull := hex.EncodeToString([]byte(catalog))

	t.Logf("catalog=%q -> hex=%q -> path=%q", catalog, hexFull, path)

	// Verify format: should be "7072/70726f6475637473"
	expected := hexFull[0:4] + "/" + hexFull
	if path != expected {
		t.Errorf("Path should follow hash[0:4]/hash format, got %q, want %q", path, expected)
	}

	t.Log("✓ OSS best practice format verified")
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
