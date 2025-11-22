package storage

import (
	"crypto/md5"
	"encoding/hex"
	"testing"
)

func TestCatalogEncodingTypes(t *testing.T) {
	tests := []struct {
		catalog      string
		wantPrefix   string
		description  string
	}{
		{"users", "_", "pure lowercase"},
		{"user_name", "_", "lowercase with underscore"},
		{"user-123", "_", "lowercase with dash and number"},
		
		{"USERS", "^", "pure uppercase"},
		{"USER_NAME", "^", "uppercase with underscore"},
		{"USER-123", "^", "uppercase with dash and number"},
		
		{"Users", "", "mixed case (base32)"},
		{"userS", "", "mixed case (base32)"},
		{"user name", "", "space (base32)"},
		{"短中文", "", "chinese (base32)"},
		{"user:name", "", "colon (base32)"},
	}

	for _, tt := range tests {
		result := encodeCatalogName(tt.catalog)
		
		var gotPrefix string
		if len(result) > 0 {
			gotPrefix = string(result[0])
		}
		
		if tt.wantPrefix != "" {
			if gotPrefix != tt.wantPrefix {
				t.Errorf("%s: catalog=%q -> prefix=%q, want %q",
					tt.description, tt.catalog, gotPrefix, tt.wantPrefix)
			} else {
				t.Logf("✓ %s: %q -> %q (prefix=%q)",
					tt.description, tt.catalog, result, gotPrefix)
			}
		} else {
			// Base32: should NOT start with _ or ^
			if gotPrefix == "_" || gotPrefix == "^" {
				t.Errorf("%s: catalog=%q should use base32, got %q",
					tt.description, tt.catalog, result)
			} else {
				t.Logf("✓ %s: %q -> %q (base32)",
					tt.description, tt.catalog, result)
			}
		}
	}
}

func TestFullPathExamples(t *testing.T) {
	shardSize := 4
	
	examples := []struct {
		catalog string
		desc    string
	}{
		{"users", "lowercase"},
		{"USERS", "uppercase"},
		{"Users", "mixed case"},
		{"user-api_v2", "lowercase with separators"},
		{"短中文目录", "chinese"},
	}

	for _, ex := range examples {
		path := encodeCatalogPath(ex.catalog, shardSize)
		
		// Extract parts
		hash := md5.Sum([]byte(ex.catalog))
		md5Hash := hex.EncodeToString(hash[:])
		
		t.Logf("\n%s: catalog=%q", ex.desc, ex.catalog)
		t.Logf("  MD5: %s", md5Hash)
		t.Logf("  Path: %s", path)
		t.Logf("  Example full: %s/delta/1700000000_123_1.json", path)
	}
}

