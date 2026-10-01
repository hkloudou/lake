package merge

import (
	"bytes"
	"fmt"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// mergePatch applies an RFC 7396 JSON Merge Patch to field (gjson path; "" =
// whole document). A missing field starts from {}.
func mergePatch(doc, patch []byte, field string) ([]byte, error) {
	if field == "" {
		return mergeRoot(doc, patch)
	}
	target := "{}"
	if res := gjson.GetBytes(doc, field); res.Exists() {
		target = res.Raw
	}
	merged, err := mergeRoot([]byte(target), patch)
	if err != nil {
		return nil, err
	}
	out, err := sjson.SetRawBytes(doc, field, merged)
	if err != nil {
		return nil, fmt.Errorf("failed to set field after merge: %w", err)
	}
	return out, nil
}

// mergeRoot patches a whole document. RFC 7396 treats ANY non-object target
// as {} when the patch is an object; evanphx honours that for scalars and
// arrays but rejects a literal null, so null is normalised here — otherwise a
// Replace of `null` followed by any patch would wedge every read.
func mergeRoot(doc, patch []byte) ([]byte, error) {
	if bytes.Equal(bytes.TrimSpace(doc), []byte("null")) {
		doc = []byte("{}")
	}
	out, err := jsonpatch.MergePatch(doc, patch)
	if err != nil {
		return nil, fmt.Errorf("RFC7396 merge failed: %w", err)
	}
	// MergePatch returns an input verbatim for a non-object patch; copy so the
	// result never aliases a memoised Body.
	if len(out) > 0 && (len(patch) > 0 && &out[0] == &patch[0] || len(doc) > 0 && &out[0] == &doc[0]) {
		out = append([]byte(nil), out...)
	}
	return out, nil
}
