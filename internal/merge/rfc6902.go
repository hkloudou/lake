package merge

import (
	"encoding/json"
	"fmt"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// RFC6902Merger implements RFC 6902 JSON Patch with auto parent path creation
// https://datatracker.ietf.org/doc/html/rfc6902
type RFC6902Merger struct{}

// NewRFC6902Merger creates a new RFC 6902 merger
func NewRFC6902Merger() *RFC6902Merger {
	return &RFC6902Merger{}
}

// Merge applies RFC 6902 JSON Patch with automatic parent path creation
// original: the original JSON document
// patchData: the JSON patch operations to apply
// field: optional field scope (empty "" means root document)
// Returns: the patched result
func (m *RFC6902Merger) Merge(original, patchData []byte, field string) ([]byte, error) {
	// If field is specified, extract that field's value and patch it
	if field != "" {
		return m.mergeField(original, patchData, field)
	}

	// Otherwise, patch the entire document
	return m.mergeRoot(original, patchData)
}

// mergeRoot applies patch to the entire document
func (m *RFC6902Merger) mergeRoot(original, patchData []byte) ([]byte, error) {
	// Parse patch operations
	var patchOps []map[string]interface{}
	if err := json.Unmarshal(patchData, &patchOps); err != nil {
		return nil, fmt.Errorf("failed to parse patch: %w", err)
	}

	// Auto-create parent paths for "add" operations
	for _, op := range patchOps {
		if op["op"] == "add" {
			path, ok := op["path"].(string)
			if !ok || path == "" {
				continue
			}
			// Create all parent paths
			original = ensureParentPath(original, path)
		}
	}

	// Apply patch after ensuring parent paths exist
	decoder, err := jsonpatch.DecodePatch(patchData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode patch: %w", err)
	}

	result, err := decoder.Apply(original)
	if err != nil {
		return nil, fmt.Errorf("RFC6902 patch apply failed: %w", err)
	}

	return result, nil
}

// mergeField applies patch to a specific field's value
func (m *RFC6902Merger) mergeField(original, patchData []byte, field string) ([]byte, error) {
	// Get the field value
	fieldValue := gjson.GetBytes(original, field).Raw
	if fieldValue == "" {
		fieldValue = "{}" // Default to empty object
	}

	// Apply patch to the field value
	patched, err := m.mergeRoot([]byte(fieldValue), patchData)
	if err != nil {
		return nil, err
	}

	// Set the patched value back to the field
	result, err := sjson.SetRawBytes(original, field, patched)
	if err != nil {
		return nil, fmt.Errorf("failed to set field after patch: %w", err)
	}

	return result, nil
}

func (m *RFC6902Merger) UpdatedMap(entries []index.DeltaInfo) map[string]index.TimeSeqID {
	updatedMap := make(map[string]index.TimeSeqID, 0)
	for _, entry := range entries {
		updatedMap[entry.Path] = entry.TsSeq
	}
	return updatedMap
}

// ensureParentPath ensures all parent paths exist for a given path
// For path "/a/b/c", creates "/a" = {} and "/a/b" = {} if they don't exist
func ensureParentPath(data []byte, path string) []byte {
	if path == "" || path == "/" {
		return data
	}

	// Split path into parts (RFC6902 uses "/" as separator)
	parts := splitPath(path)
	if len(parts) <= 1 {
		return data // No parent paths needed
	}

	// Create parent paths (exclude the last part)
	currentPath := ""
	for i := 0; i < len(parts)-1; i++ {
		if currentPath == "" {
			currentPath = parts[i]
		} else {
			currentPath = currentPath + "." + parts[i]
		}

		// Check if path exists
		if !gjson.GetBytes(data, currentPath).Exists() {
			// Create empty object at this path
			var err error
			data, err = sjson.SetBytes(data, currentPath, map[string]interface{}{})
			if err != nil {
				// Ignore error, will fail later in patch apply
				continue
			}
		}
	}

	return data
}

// splitPath splits a JSON pointer path into parts
// "/a/b/c" -> ["a", "b", "c"]
func splitPath(path string) []string {
	if path == "" {
		return []string{}
	}
	parts := []string{}
	current := ""
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(path[i])
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}
