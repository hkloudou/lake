package merge

import (
	"encoding/json"
	"fmt"

	"github.com/tidwall/sjson"
)

// replace sets field (gjson path; "" = whole document) to data. The body is
// validated here because sjson splices raw bytes verbatim: an invalid
// client-uploaded body would otherwise silently corrupt the document and the
// snapshot persisted from it, instead of failing loudly.
func replace(doc, data []byte, field string) ([]byte, error) {
	if !json.Valid(data) {
		return nil, fmt.Errorf("invalid JSON body for replace")
	}
	if field == "" {
		return append([]byte(nil), data...), nil // copy: data is the memoised Body
	}
	out, err := sjson.SetRawBytes(doc, field, data)
	if err != nil {
		return nil, fmt.Errorf("failed to set field: %w", err)
	}
	return out, nil
}
