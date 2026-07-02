package index

import (
	"encoding/json"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// MergeType selects how a Write merges into the existing document.
type MergeType int

const (
	MergeTypeUnknown MergeType = 0
	MergeTypeReplace MergeType = 1 // simple field set
	MergeTypeRFC7396 MergeType = 2 // JSON Merge Patch
)

func (m MergeType) String() string {
	switch m {
	case MergeTypeReplace:
		return "replace"
	case MergeTypeRFC7396:
		return "rfc7396"
	default:
		return "unknown"
	}
}

func MergeTypeFromInt(i int) MergeType {
	if i < 1 || i > 2 {
		return MergeTypeUnknown
	}
	return MergeType(i)
}

// Delta zset member layout: a JSON array [mergeType, fieldPath, tsSeq, uri].
// It is *written* by the notify Lua script (the single authoritative encoder,
// via cjson); DecodeDeltaMember below is the matching reader and its tests pin
// the format. The uri (provider://bucket/path) is a complete object locator,
// so the read path resolves the body without any key-derivation knowledge.

// DecodeDeltaMember parses a delta member and verifies its score matches the
// embedded tsSeq.
func DecodeDeltaMember(member string, score float64) (*DeltaInfo, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(member), &arr); err != nil {
		return nil, fmt.Errorf("invalid delta member %q: %w", member, err)
	}
	if len(arr) != 4 {
		return nil, fmt.Errorf("invalid delta member %q (want 4 elements)", member)
	}
	var mt int
	if err := json.Unmarshal(arr[0], &mt); err != nil || mt < 1 || mt > 2 {
		return nil, fmt.Errorf("invalid merge type in %q", member)
	}
	var path string
	if err := json.Unmarshal(arr[1], &path); err != nil {
		return nil, fmt.Errorf("invalid path in %q: %w", member, err)
	}
	if err := utils.ValidateFieldPath(path); err != nil {
		return nil, fmt.Errorf("invalid path in %q: %w", member, err)
	}
	var tsSeqStr string
	if err := json.Unmarshal(arr[2], &tsSeqStr); err != nil {
		return nil, fmt.Errorf("invalid tsSeq in %q: %w", member, err)
	}
	tsSeq, err := ParseTimeSeqID(tsSeqStr)
	if err != nil {
		return nil, fmt.Errorf("invalid tsSeq in %q: %w", member, err)
	}
	if tsSeq.Score() != score {
		return nil, fmt.Errorf("score mismatch in %q (member=%.6f, redis=%.6f)", member, tsSeq.Score(), score)
	}
	var uri string
	if err := json.Unmarshal(arr[3], &uri); err != nil || uri == "" {
		return nil, fmt.Errorf("invalid uri in %q", member)
	}
	return &DeltaInfo{
		Member:    member,
		Score:     score,
		Path:      path,
		TsSeq:     tsSeq,
		MergeType: MergeTypeFromInt(mt),
		URI:       uri,
	}, nil
}

// Snap value layout: a JSON array [tsSeq, uri], stored as the field value
// under "<prefix>:s" keyed by catalog.
func EncodeSnapValue(stop TimeSeqID, uri string) (string, error) {
	b, err := json.Marshal([2]string{stop.String(), uri})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func DecodeSnapValue(value string) (TimeSeqID, string, error) {
	var arr [2]string
	if err := json.Unmarshal([]byte(value), &arr); err != nil {
		return TimeSeqID{}, "", fmt.Errorf("invalid snap value %q: %w", value, err)
	}
	stop, err := ParseTimeSeqID(arr[0])
	if err != nil {
		return TimeSeqID{}, "", fmt.Errorf("invalid snap value %q: %w", value, err)
	}
	if arr[1] == "" {
		return TimeSeqID{}, "", fmt.Errorf("invalid snap value %q (empty uri)", value)
	}
	return stop, arr[1], nil
}

// snapScoreLua defines snap_score(raw) → score|nil for use inside index Lua
// scripts (prepend this const to the script body). It is the Lua mirror of
// DecodeSnapValue + ParseTimeSeqID above and MUST accept exactly what they
// accept: a 2-string [tsSeq, uri] with non-empty uri; ts with no leading zero
// within the year-3000 cap; seq 1..999999 with no leading zero. Accepting
// more would let a script trust a value the Go reader rejects (wedging the
// catalog); accepting less would make it discard a valid snap. The "0_0"
// sentinel deliberately yields nil: it scores 0, so no caller's comparison
// against a real stop can need it.
const snapScoreLua = `
local function snap_score(raw)
  local ok, arr = pcall(cjson.decode, raw)
  if not (ok and type(arr) == "table" and type(arr[1]) == "string"
        and type(arr[2]) == "string" and arr[2] ~= "") then
    return nil
  end
  local ts, seq = string.match(arr[1], "^([1-9]%d*)_([1-9]%d?%d?%d?%d?%d?)$")
  if not ts or tonumber(ts) > 32503680000 then
    return nil
  end
  return tonumber(ts) + tonumber(seq) / 1000000.0
end
`

// IsDeltaMember reports whether a zset member looks like a delta (JSON array).
func IsDeltaMember(m string) bool { return len(m) > 0 && m[0] == '[' }
