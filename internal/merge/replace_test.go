package merge

import (
	"encoding/json"
	"testing"
)

func TestReplaceMerger(t *testing.T) {
	tests := []struct {
		name     string
		original string
		data     string
		field    string
		expected string
	}{
		{
			name:     "replace entire document",
			original: `{"old":"data"}`,
			data:     `{"new":"data"}`,
			field:    "",
			expected: `{"new":"data"}`,
		},
		{
			name:     "replace field value",
			original: `{"user":{"name":"Alice","age":30}}`,
			data:     `"Bob"`,
			field:    "user.name",
			expected: `{"user":{"name":"Bob","age":30}}`,
		},
		{
			name:     "replace nested field",
			original: `{"data":{"user":{"profile":{"city":"LA"}}}}`,
			data:     `"NYC"`,
			field:    "data.user.profile.city",
			expected: `{"data":{"user":{"profile":{"city":"NYC"}}}}`,
		},
		{
			name:     "replace with object",
			original: `{"user":"simple"}`,
			data:     `{"name":"Alice","age":30}`,
			field:    "user",
			expected: `{"user":{"name":"Alice","age":30}}`,
		},
		{
			name:     "replace with array",
			original: `{"items":null}`,
			data:     `[1,2,3]`,
			field:    "items",
			expected: `{"items":[1,2,3]}`,
		},
		{
			name:     "replace non-existent field (creates it)",
			original: `{"existing":"data"}`,
			data:     `"new value"`,
			field:    "newField",
			expected: `{"existing":"data","newField":"new value"}`,
		},
		{
			name:     "replace with number",
			original: `{"user":{"age":30}}`,
			data:     `31`,
			field:    "user.age",
			expected: `{"user":{"age":31}}`,
		},
		{
			name:     "replace with null",
			original: `{"user":{"name":"Alice"}}`,
			data:     `null`,
			field:    "user.name",
			expected: `{"user":{"name":null}}`,
		},
	}

	merger := NewReplaceMerger()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := merger.Merge([]byte(tt.original), []byte(tt.data), tt.field)
			if err != nil {
				t.Fatalf("Merge failed: %v", err)
			}

			// Normalize JSON for comparison
			var expectedNorm, resultNorm interface{}
			json.Unmarshal([]byte(tt.expected), &expectedNorm)
			json.Unmarshal(result, &resultNorm)

			expectedJSON, _ := json.Marshal(expectedNorm)
			resultJSON, _ := json.Marshal(resultNorm)

			if string(expectedJSON) != string(resultJSON) {
				t.Errorf("Result mismatch:\n  Expected: %s\n  Got:      %s", expectedJSON, resultJSON)
			}
		})
	}
}
