package merge

import (
	"fmt"

	"github.com/hkloudou/lake/v2/internal/index"
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
func (m *ReplaceMerger) Merge(original, data []byte, field string) ([]byte, error) {
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

func (m *ReplaceMerger) UpdatedMap(entries []index.DeltaInfo) map[string]index.TimeSeqID {
	updatedMap := make(map[string]index.TimeSeqID, 0)
	for _, entry := range entries {
		updatedMap[entry.Path] = entry.TsSeq
	}
	return updatedMap
}
