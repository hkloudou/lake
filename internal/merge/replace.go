package merge

import (
	"encoding/json"
	"fmt"

	"github.com/tidwall/sjson"
)

// ReplaceMerger implements simple field replacement
type ReplaceMerger struct{}

// NewReplaceMerger creates a new replace merger
func NewReplaceMerger() *ReplaceMerger {
	return &ReplaceMerger{}
}

// Merge replaces a field's value with new data
// original: the original JSON document
// data: the new value to set
// field: the field path to replace (empty "" means root document)
// Returns: the result with replaced value
//
// data is validated for EVERY replace, not just root: it is a client-uploaded
// body Lake never inspected before this point, and sjson.SetRawBytes splices
// raw bytes verbatim — an invalid body would silently corrupt the whole
// document (and the snapshot then persisted from it) instead of failing loudly
// with the offending delta identified.
func (m *ReplaceMerger) Merge(original, data []byte, field string) ([]byte, error) {
	if !json.Valid(data) {
		return nil, fmt.Errorf("invalid JSON body for replace")
	}

	// If field is empty, replace entire document
	if field == "" {
		return data, nil
	}

	// Replace the field value
	result, err := sjson.SetRawBytes(original, field, data)
	if err != nil {
		return nil, fmt.Errorf("failed to set field: %w", err)
	}

	return result, nil
}
