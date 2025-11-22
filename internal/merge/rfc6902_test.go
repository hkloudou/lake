package merge

import (
	"encoding/json"
	"testing"
)

// TestRFC6902Examples tests RFC 6902 JSON Patch operations
// https://datatracker.ietf.org/doc/html/rfc6902
func TestRFC6902Examples(t *testing.T) {
	tests := []struct {
		name     string
		original string
		patch    string
		expected string
	}{
		{
			name:     "add operation",
			original: `{"a":"b"}`,
			patch: `[
				{"op": "add", "path": "/c", "value": "d"}
			]`,
			expected: `{"a":"b","c":"d"}`,
		},
		{
			name:     "add with auto parent creation",
			original: `{}`,
			patch: `[
				{"op": "add", "path": "/a/b/c", "value": 42}
			]`,
			expected: `{"a":{"b":{"c":42}}}`,
		},
		{
			name:     "remove operation",
			original: `{"a":"b","c":"d"}`,
			patch: `[
				{"op": "remove", "path": "/c"}
			]`,
			expected: `{"a":"b"}`,
		},
		{
			name:     "replace operation",
			original: `{"a":"b"}`,
			patch: `[
				{"op": "replace", "path": "/a", "value": "c"}
			]`,
			expected: `{"a":"c"}`,
		},
		{
			name:     "move operation",
			original: `{"a":"b","c":"d"}`,
			patch: `[
				{"op": "move", "from": "/c", "path": "/e"}
			]`,
			expected: `{"a":"b","e":"d"}`,
		},
		{
			name:     "copy operation",
			original: `{"a":"b"}`,
			patch: `[
				{"op": "copy", "from": "/a", "path": "/c"}
			]`,
			expected: `{"a":"b","c":"b"}`,
		},
		{
			name:     "complex operations",
			original: `{}`,
			patch: `[
				{"op": "add", "path": "/a/b/c", "value": {"name": "John"}},
				{"op": "replace", "path": "/a/b/c", "value": 42},
				{"op": "move", "from": "/a/b/c", "path": "/a/b/d"},
				{"op": "copy", "from": "/a/b/d", "path": "/a/b/e"}
			]`,
			expected: `{"a":{"b":{"d":42,"e":42}}}`,
		},
	}

	merger := NewRFC6902Merger()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test root-level merge
			result, err := merger.Merge([]byte(tt.original), []byte(tt.patch), "")
			if err != nil {
				t.Fatalf("RFC6902 merge failed: %v", err)
			}

			// Normalize JSON for comparison
			var expectedNorm, resultNorm interface{}
			json.Unmarshal([]byte(tt.expected), &expectedNorm)
			json.Unmarshal(result, &resultNorm)

			expectedJSON, _ := json.Marshal(expectedNorm)
			resultJSON, _ := json.Marshal(resultNorm)

			if string(expectedJSON) != string(resultJSON) {
				t.Errorf("Result mismatch:\n  Expected: %s\n  Got:      %s", expectedJSON, resultJSON)
			} else {
				t.Logf("✓ Passed: %s", tt.name)
			}
		})
	}
}

// TestRFC6902FieldScope tests field-level RFC 6902 operations
func TestRFC6902FieldScope(t *testing.T) {
	merger := NewRFC6902Merger()

	// Test field-level patch
	original := []byte(`{"user":{"name":"Alice"},"other":"data"}`)
	patch := []byte(`[
		{"op": "add", "path": "/age", "value": 30},
		{"op": "add", "path": "/city", "value": "NYC"}
	]`)

	// Patch should only affect "user" field
	result, err := merger.Merge(original, patch, "user")
	if err != nil {
		t.Fatalf("Field-level merge failed: %v", err)
	}

	var resultData map[string]interface{}
	json.Unmarshal(result, &resultData)

	// Verify user field was patched
	user, ok := resultData["user"].(map[string]interface{})
	if !ok {
		t.Fatalf("user field not found")
	}

	if user["name"] != "Alice" {
		t.Errorf("name should be 'Alice', got %v", user["name"])
	}
	if user["age"].(float64) != 30 {
		t.Errorf("age should be 30, got %v", user["age"])
	}
	if user["city"] != "NYC" {
		t.Errorf("city should be 'NYC', got %v", user["city"])
	}

	// Verify other field unchanged
	if resultData["other"] != "data" {
		t.Errorf("other field should be unchanged, got %v", resultData["other"])
	}

	t.Logf("✓ Field-level RFC6902 successful: %+v", resultData)
}

