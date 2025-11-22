package storage

import (
	"testing"

	"github.com/hkloudou/lake/v2/internal/index"
)

func TestEncodeCatalogPath(t *testing.T) {
	tests := []struct {
		catalog   string
		shardSize int
		wantPath  string
	}{
		{"users", 3, "757/365/3"},       // hex: 7573657273
		{"Users", 3, "557/365/3"},       // hex: 5573657273 (different!)
		{"USERS", 3, "555/345/3"},       // hex: 5553455253 (different!)
		{"products", 3, "707/26f/3"},    // hex: 70726f6475637473
		{"a", 3, "61"},                  // hex: 61 (short)
		{"ab", 3, "616/2"},              // hex: 6162 (2 parts)
		{"users", 2, "75/73/73"},        // different shard size
		{"users", 4, "7573/6572/73"},    // different shard size
	}

	for _, tt := range tests {
		result := encodeCatalogPath(tt.catalog, tt.shardSize)
		t.Logf("catalog=%q, shardSize=%d -> path=%q", tt.catalog, tt.shardSize, result)

		if result != tt.wantPath {
			t.Errorf("encodeCatalogPath(%q, %d) = %q, want %q",
				tt.catalog, tt.shardSize, result, tt.wantPath)
		}
	}
}

func TestCaseSensitivity(t *testing.T) {
	// Verify that different case catalogs get different paths
	path1 := encodeCatalogPath("users", 3)
	path2 := encodeCatalogPath("Users", 3)
	path3 := encodeCatalogPath("USERS", 3)

	t.Logf("users -> %s", path1)
	t.Logf("Users -> %s", path2)
	t.Logf("USERS -> %s", path3)

	if path1 == path2 || path2 == path3 || path1 == path3 {
		t.Error("Different case catalogs should have different paths!")
	}

	t.Log("✓ Case sensitivity preserved correctly")
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

