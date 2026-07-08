package merge

import (
	"bytes"
	"fmt"

	jsonpatch "github.com/evanphx/json-patch/v5"
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

// jsonNull matches a document/field whose value is the JSON literal null.
// RFC 7396's algorithm treats ANY non-object target as an empty object when
// the patch is an object — and evanphx/json-patch honors that for scalars and
// arrays but returns ErrBadJSONDoc for null specifically. Without the
// normalization, two individually valid writes (a Replace of `null`, then any
// RFC7396 patch at the same scope) would wedge every read of the catalog.
var jsonNull = []byte("null")

func isJSONNull(doc []byte) bool {
	return bytes.Equal(bytes.TrimSpace(doc), jsonNull)
}

// mergeRoot applies merge patch to the entire document
func (m *RFC7396Merger) mergeRoot(original, patchData []byte) ([]byte, error) {
	if isJSONNull(original) {
		original = []byte("{}")
	}
	result, err := jsonpatch.MergePatch(original, patchData)
	if err != nil {
		return nil, fmt.Errorf("RFC7396 merge failed: %w", err)
	}
	// evanphx returns patchData ITSELF when the patch is a non-object literal
	// (null / number / string / bool) — that would alias the delta Body the
	// caller retains, and the engine would then treat the Body's backing as
	// an owned buffer (in-place splices, caller mutation). Same for a
	// non-object original passed through. Copy to keep the result private.
	if sharesBacking(result, patchData) || sharesBacking(result, original) {
		result = append([]byte(nil), result...)
	}
	return result, nil
}

// sharesBacking reports whether a and b share their first backing byte —
// the only aliasing shape MergePatch can produce (it returns an input
// verbatim, never a sub-slice).
func sharesBacking(a, b []byte) bool {
	return len(a) > 0 && len(b) > 0 && &a[0] == &b[0]
}

// mergeField applies merge patch to a specific field's value
func (m *RFC7396Merger) mergeField(original, patchData []byte, field string) ([]byte, error) {
	return m.mergeFieldOptions(original, patchData, field, nil)
}

// mergeOwned is Merge for a buffer the engine owns (see ownedMerger,
// engine.go): the write-back may patch original's backing in place. Root
// scope is unchanged — MergePatch always allocates its result.
func (m *RFC7396Merger) mergeOwned(original, patchData []byte, field string) ([]byte, error) {
	if field == "" {
		return m.mergeRoot(original, patchData)
	}
	return m.mergeFieldOptions(original, patchData, field, sjsonInPlace)
}

// mergeFieldOptions is the single body behind mergeField/mergeOwned — one
// implementation so a future semantics change cannot diverge by chain
// position.
func (m *RFC7396Merger) mergeFieldOptions(original, patchData []byte, field string, opts *sjson.Options) ([]byte, error) {
	// Get the field value. A MISSING field defaults to the empty object; an
	// existing null value passes through as "null", which mergeRoot itself
	// normalizes (single home for the null rule — see isJSONNull).
	res := gjson.GetBytes(original, field)
	fieldValue := res.Raw
	if !res.Exists() {
		fieldValue = "{}"
	}

	// Apply merge patch to the field value
	merged, err := m.mergeRoot([]byte(fieldValue), patchData)
	if err != nil {
		return nil, err
	}

	// Set the merged value back to the field
	result, err := sjson.SetRawBytesOptions(original, field, merged, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to set field after merge: %w", err)
	}
	return result, nil
}
