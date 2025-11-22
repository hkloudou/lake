package merge

import (
	"fmt"

	jsonpatch "github.com/evanphx/json-patch/v5"
)

// RFC7396Merger implements RFC 7396 JSON Merge Patch
// https://datatracker.ietf.org/doc/html/rfc7396
type RFC7396Merger struct{}

// NewRFC7396Merger creates a new RFC 7396 merger
func NewRFC7396Merger() *RFC7396Merger {
	return &RFC7396Merger{}
}

// Merge applies RFC 7396 JSON Merge Patch
// original: the original JSON document
// patch: the merge patch to apply
// Returns: the merged result
func (m *RFC7396Merger) Merge(original, patch []byte) ([]byte, error) {
	result, err := jsonpatch.MergePatch(original, patch)
	if err != nil {
		return nil, fmt.Errorf("RFC7396 merge failed: %w", err)
	}
	return result, nil
}

