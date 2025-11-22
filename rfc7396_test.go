package lake_test

import (
	"context"
	"encoding/json"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/hkloudou/lake/v2"
	"github.com/hkloudou/lake/v2/internal/index"
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use jsonpatch.MergePatch (RFC 7396 implementation)
			result, err := jsonpatch.MergePatch([]byte(tt.original), []byte(tt.patch))
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

// TestWriteRFC7396 tests the WriteRFC7396 method
func TestWriteRFC7396(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")
	ctx := context.Background()
	catalog := "test_rfc7396"

	// Test 1: Simple merge at root level
	t.Run("root level merge", func(t *testing.T) {
		patch := []byte(`{"title":"Hello!","phoneNumber":"+01-123-456-7890"}`)
		_, err := client.WriteRFC7396(ctx, catalog, "", patch)
		if err != nil {
			t.Fatalf("WriteRFC7396 failed: %v", err)
		}
		t.Log("✓ Root level merge successful")
	})

	// Test 2: Merge at specific field (local scope)
	t.Run("field level merge", func(t *testing.T) {
		// Initialize author field first
		_, err := client.Write(ctx, lake.WriteRequest{
			Catalog:   catalog,
			Field:     "author",
			Value:     map[string]interface{}{"givenName": "John", "familyName": "Doe"},
			MergeType: index.MergeTypeReplace,
		})
		if err != nil {
			t.Fatalf("Initial write failed: %v", err)
		}

		// Apply merge patch to author field only
		patch := []byte(`{"familyName":null}`)
		_, err = client.WriteRFC7396(ctx, catalog, "author", patch)
		if err != nil {
			t.Fatalf("WriteRFC7396 failed: %v", err)
		}
		t.Log("✓ Field level merge successful (deleted familyName)")
	})

	// Test 3: Delete field with null
	t.Run("delete with null", func(t *testing.T) {
		patch := []byte(`{"phoneNumber":null}`)
		_, err := client.WriteRFC7396(ctx, catalog, "", patch)
		if err != nil {
			t.Fatalf("WriteRFC7396 failed: %v", err)
		}
		t.Log("✓ Delete with null successful")
	})

	// Verify final result
	result, err := client.List(ctx, catalog)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	data, err := lake.ReadMap(ctx, result)
	if err != nil {
		t.Fatalf("ReadMap failed: %v", err)
	}

	t.Logf("Final data: %+v", data)
}

// TestMergeTypeImplementsRFC7396 verifies that MergeTypeMerge implements RFC 7396
func TestMergeTypeImplementsRFC7396(t *testing.T) {
	client := lake.NewLake("redis://lake-redis-master.cs:6379/2")
	ctx := context.Background()
	catalog := "test_merge_rfc7396"

	// Write initial data
	_, err := client.Write(ctx, lake.WriteRequest{
		Catalog:   catalog,
		Field:     "user",
		Value:     map[string]interface{}{"name": "Alice", "age": 30},
		MergeType: index.MergeTypeReplace,
	})
	if err != nil {
		t.Fatalf("Initial write failed: %v", err)
	}

	// Apply RFC7396 merge patch using MergeTypeMerge
	_, err = client.Write(ctx, lake.WriteRequest{
		Catalog:   catalog,
		Field:     "user",
		Value:     map[string]interface{}{"age": 31, "city": "NYC"},
		MergeType: index.MergeTypeMerge, // This should use RFC7396
	})
	if err != nil {
		t.Fatalf("Merge write failed: %v", err)
	}

	// Read and verify
	result, err := client.List(ctx, catalog)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	data, err := lake.ReadMap(ctx, result)
	if err != nil {
		t.Fatalf("ReadMap failed: %v", err)
	}

	// Verify: user should be {"name":"Alice","age":31,"city":"NYC"}
	user, ok := data["user"].(map[string]interface{})
	if !ok {
		t.Fatalf("user field not found or wrong type")
	}

	if user["name"] != "Alice" {
		t.Errorf("name should be 'Alice', got %v", user["name"])
	}
	if user["age"].(float64) != 31 {
		t.Errorf("age should be 31, got %v", user["age"])
	}
	if user["city"] != "NYC" {
		t.Errorf("city should be 'NYC', got %v", user["city"])
	}

	t.Logf("✓ MergeTypeMerge correctly implements RFC7396: %+v", user)
}

