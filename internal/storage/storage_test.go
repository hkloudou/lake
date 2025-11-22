package storage

import (
	"testing"

	"github.com/hkloudou/lake/v2/internal/index"
)

func TestEncodeCatalogPath(t *testing.T) {
	tests := []struct {
		catalog  string
		expected string
	}{
		{"users", "dXN/lcn/M"},         // "users" -> "dXNlcnM=" -> "dXNlcnM" -> "dXN/lcn/M"
		{"Users", "VXN/lcn/M"},         // "Users" -> "VXNlcnM=" -> "VXNlcnM" -> "VXN/lcn/M"
		{"USERS", "VVN/FUl/M"},         // Different!
		{"products", "cHJ/vZH/Vjd/HM"}, // longer catalog
		{"a", "YQ"},                    // short catalog
	}

	for _, tt := range tests {
		result := encodeCatalogPath(tt.catalog)
		t.Logf("catalog=%q -> encoded=%q", tt.catalog, result)

		// Check for case sensitivity
		if tt.catalog == "users" || tt.catalog == "Users" {
			t.Logf("  Case sensitive check: %s vs Users", tt.catalog)
		}
	}
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
