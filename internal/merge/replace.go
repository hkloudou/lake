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
	return m.merge(original, data, field, nil)
}

// mergeOwned is Merge for a buffer the engine owns (see ownedMerger,
// engine.go): the field-scoped set may patch original's backing in place
// instead of rebuilding the document.
func (m *ReplaceMerger) mergeOwned(original, data []byte, field string) ([]byte, error) {
	return m.merge(original, data, field, sjsonInPlace)
}

// merge is the single body behind Merge/mergeOwned — one implementation so a
// future validation or semantics change cannot diverge by chain position.
func (m *ReplaceMerger) merge(original, data []byte, field string, opts *sjson.Options) ([]byte, error) {
	if !json.Valid(data) {
		return nil, fmt.Errorf("invalid JSON body for replace")
	}

	// If field is empty, replace entire document. Copied, not aliased: data
	// is the delta's Body, which stays cached on the caller's ListResult —
	// returning it verbatim would let a caller who mutates the merged
	// document (explicitly allowed) silently corrupt the retained Body, and
	// through it any re-read or re-snapshot from the same ListResult.
	if field == "" {
		return append([]byte(nil), data...), nil
	}

	// Replace the field value
	result, err := sjson.SetRawBytesOptions(original, field, data, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to set field: %w", err)
	}

	return result, nil
}

// sjsonInPlace lets sjson splice into the existing buffer when the new value
// fits — valid only for buffers the merge engine owns.
var sjsonInPlace = &sjson.Options{Optimistic: true, ReplaceInPlace: true}
