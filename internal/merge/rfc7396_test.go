package merge

import (
	"encoding/json"
	"testing"
)

// TestRFC7396Examples tests all examples from RFC 7396 Appendix A
// https://datatracker.ietf.org/doc/html/rfc7396
func TestRFC7396Examples(t *testing.T) {
	tests := []struct {
		name     string
		original string
		patch    string
		expected string
	}{
		{
			name:     "simple replace",
			original: `{"a":"b"}`,
			patch:    `{"a":"c"}`,
			expected: `{"a":"c"}`,
		},
		{
			name:     "add new field",
			original: `{"a":"b"}`,
			patch:    `{"b":"c"}`,
			expected: `{"a":"b","b":"c"}`,
		},
		{
			name:     "delete with null",
			original: `{"a":"b"}`,
			patch:    `{"a":null}`,
			expected: `{}`,
		},
		{
			name:     "delete one of two",
			original: `{"a":"b","b":"c"}`,
			patch:    `{"a":null}`,
			expected: `{"b":"c"}`,
		},
		{
			name:     "replace array with string",
			original: `{"a":["b"]}`,
			patch:    `{"a":"c"}`,
			expected: `{"a":"c"}`,
		},
		{
			name:     "replace string with array",
			original: `{"a":"c"}`,
			patch:    `{"a":["b"]}`,
			expected: `{"a":["b"]}`,
		},
		{
			name:     "nested object merge",
			original: `{"a":{"b":"c"}}`,
			patch:    `{"a":{"b":"d","c":null}}`,
			expected: `{"a":{"b":"d"}}`,
		},
		{
			name:     "replace nested array",
			original: `{"a":[{"b":"c"}]}`,
			patch:    `{"a":[1]}`,
			expected: `{"a":[1]}`,
		},
		{
			name:     "array replace",
			original: `["a","b"]`,
			patch:    `["c","d"]`,
			expected: `["c","d"]`,
		},
		{
			name:     "object to array",
			original: `{"a":"b"}`,
			patch:    `["c"]`,
			expected: `["c"]`,
		},
		{
			name:     "replace with null",
			original: `{"a":"foo"}`,
			patch:    `null`,
			expected: `null`,
		},
		{
			name:     "replace with string",
			original: `{"a":"foo"}`,
			patch:    `"bar"`,
			expected: `"bar"`,
		},
		{
			name:     "add to object with null",
			original: `{"e":null}`,
			patch:    `{"a":1}`,
			expected: `{"e":null,"a":1}`,
		},
		{
			name:     "array to object",
			original: `[1,2]`,
			patch:    `{"a":"b","c":null}`,
			expected: `{"a":"b"}`,
		},
		{
			name:     "nested object with null deletion",
			original: `{}`,
			patch:    `{"a":{"bb":{"ccc":null}}}`,
			expected: `{"a":{"bb":{}}}`,
		},
	}

	merger := NewRFC7396Merger()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use RFC7396Merger.Merge with empty field (root document)
			result, err := merger.Merge([]byte(tt.original), []byte(tt.patch), "")
			if err != nil {
				t.Fatalf("MergePatch failed: %v", err)
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

// TestRFC7396FieldScoping tests field-scoped merge patch
func TestRFC7396FieldScoping(t *testing.T) {
	tests := []struct {
		name     string
		original string
		patch    string
		field    string
		expected string
	}{
		{
			name:     "merge patch to field",
			original: `{"user":{"name":"Alice","age":30},"settings":{}}`,
			patch:    `{"age":31,"city":"NYC"}`,
			field:    "user",
			expected: `{"user":{"name":"Alice","age":31,"city":"NYC"},"settings":{}}`,
		},
		{
			name:     "merge patch to nested field",
			original: `{"data":{"user":{"name":"Bob"}}}`,
			patch:    `{"email":"bob@example.com"}`,
			field:    "data.user",
			expected: `{"data":{"user":{"name":"Bob","email":"bob@example.com"}}}`,
		},
		{
			name:     "merge patch to non-existent field",
			original: `{"other":"data"}`,
			patch:    `{"name":"Charlie"}`,
			field:    "user",
			expected: `{"other":"data","user":{"name":"Charlie"}}`,
		},
		{
			name:     "delete field within scope",
			original: `{"user":{"name":"Alice","age":30,"city":"LA"}}`,
			patch:    `{"age":null}`,
			field:    "user",
			expected: `{"user":{"name":"Alice","city":"LA"}}`,
		},
	}

	merger := NewRFC7396Merger()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := merger.Merge([]byte(tt.original), []byte(tt.patch), tt.field)
			if err != nil {
				t.Fatalf("MergePatch failed: %v", err)
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
