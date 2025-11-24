package merge

import (
	"fmt"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// RFC7396Merger implements RFC 7396 JSON Merge Patch
// https://datatracker.ietf.org/doc/html/rfc7396
type RFC7396Merger struct{}

// NewRFC7396Merger creates a new RFC 7396 merger
func NewRFC7396Merger() *RFC7396Merger {
	return &RFC7396Merger{}
}

// Merge applies RFC 7396 JSON Merge Patch with optional field scoping
// original: the original JSON document
// patchData: the merge patch to apply
// field: optional field scope (empty "" means root document)
// Returns: the merged result
func (m *RFC7396Merger) Merge(original, patchData []byte, field string) ([]byte, error) {
	// If field is specified, apply patch to that field's value
	if field != "" {
		return m.mergeField(original, patchData, field)
	}

	// Otherwise, patch the entire document
	return m.mergeRoot(original, patchData)
}

// mergeRoot applies merge patch to the entire document
func (m *RFC7396Merger) mergeRoot(original, patchData []byte) ([]byte, error) {
	result, err := jsonpatch.MergePatch(original, patchData)
	if err != nil {
		return nil, fmt.Errorf("RFC7396 merge failed: %w", err)
	}
	return result, nil
}

// mergeField applies merge patch to a specific field's value
func (m *RFC7396Merger) mergeField(original, patchData []byte, field string) ([]byte, error) {
	// Get the field value
	fieldValue := gjson.GetBytes(original, field).Raw
	if fieldValue == "" {
		fieldValue = "{}" // Default to empty object
	}

	// Apply merge patch to the field value
	merged, err := m.mergeRoot([]byte(fieldValue), patchData)
	if err != nil {
		return nil, err
	}

	// Set the merged value back to the field
	result, err := sjson.SetRawBytes(original, field, merged)
	if err != nil {
		return nil, fmt.Errorf("failed to set field after merge: %w", err)
	}

	return result, nil
}

// UpdatedMap builds a hierarchical update map from delta entries
// For each field update, also updates all parent paths with the latest timestamp
// Example: /base/gsxx update → also updates /base (if newer)
func (m *RFC7396Merger) UpdatedMap(entries []index.DeltaInfo) map[string]index.TimeSeqID {
	hm := NewHierarchicalUpdateMap()
	for _, entry := range entries {
		hm.Update(entry.Field, entry.TsSeq)
	}
	return hm.GetAll()
}
