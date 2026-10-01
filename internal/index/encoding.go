package index

import (
	"encoding/json"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// MergeType selects how a write merges into the existing document.
type MergeType int

const (
	MergeTypeUnknown MergeType = 0
	MergeTypeReplace MergeType = 1 // simple field set
	MergeTypeRFC7396 MergeType = 2 // JSON Merge Patch
)

// DecodeDeltaMember parses a delta member — the JSON array [mergeType, path,
// tsSeq, uri] the notify script writes via cjson — and verifies its score
// matches the embedded tsSeq.
func DecodeDeltaMember(member string, score float64) (*DeltaInfo, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(member), &arr); err != nil || len(arr) != 4 {
		return nil, fmt.Errorf("invalid delta member %q", member)
	}
	var mt int
	if err := json.Unmarshal(arr[0], &mt); err != nil || mt < 1 || mt > 2 {
		return nil, fmt.Errorf("invalid merge type in %q", member)
	}
	var path, tsSeqStr, uri string
	if err := json.Unmarshal(arr[1], &path); err != nil {
		return nil, fmt.Errorf("invalid path in %q: %w", member, err)
	}
	if err := utils.ValidateFieldPath(path); err != nil {
		return nil, fmt.Errorf("invalid path in %q: %w", member, err)
	}
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
	if err := json.Unmarshal(arr[3], &uri); err != nil || uri == "" {
		return nil, fmt.Errorf("invalid uri in %q", member)
	}
	return &DeltaInfo{Member: member, Score: score, Path: path, TsSeq: tsSeq, MergeType: MergeType(mt), URI: uri}, nil
}

// Snap value layout: the JSON array [tsSeq, uri], stored under "<prefix>:s"
// keyed by catalog.
func EncodeSnapValue(stop TimeSeqID, uri string) (string, error) {
	b, err := json.Marshal([2]string{stop.String(), uri})
	return string(b), err
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

// snapScoreLua is the Lua mirror of DecodeSnapValue + ParseTimeSeqID, shared
// by every index script. It must accept exactly what the Go decoders accept:
// a 2-string [tsSeq, uri] with non-empty uri, ts without leading zero within
// MaxTimestamp, seq 1..999999. Accepting more would let a script trust a
// value the Go reader rejects (wedging the catalog); accepting less would
// discard a valid snap. parse_tsseq is the tsSeq half, also used by notify.
const snapScoreLua = `
local function parse_tsseq(s)
  local a, b = string.match(s, "^([1-9]%d*)_([1-9]%d?%d?%d?%d?%d?)$")
  if a and tonumber(a) <= 8589934591 then
    return tonumber(a), tonumber(b)
  end
  return nil
end
local function snap_score(raw)
  local ok, arr = pcall(cjson.decode, raw)
  if not (ok and type(arr) == "table" and type(arr[1]) == "string"
        and type(arr[2]) == "string" and arr[2] ~= "") then
    return nil
  end
  local ts, seq = parse_tsseq(arr[1])
  if not ts then
    return nil
  end
  return ts + seq / 1000000.0
end
`
