package index

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// MergeType selects how a Write merges into the existing document.
type MergeType int

const (
	MergeTypeUnknown MergeType = 0
	MergeTypeReplace MergeType = 1 // simple field set
	MergeTypeRFC7396 MergeType = 2 // JSON Merge Patch
	MergeTypeRFC6902 MergeType = 3 // JSON Patch
)

func (m MergeType) String() string {
	switch m {
	case MergeTypeReplace:
		return "replace"
	case MergeTypeRFC7396:
		return "rfc7396"
	case MergeTypeRFC6902:
		return "rfc6902"
	default:
		return "unknown"
	}
}

func MergeTypeFromInt(i int) MergeType {
	if i < 1 || i > 3 {
		return MergeTypeUnknown
	}
	return MergeType(i)
}

// EncodeDeltaMember formats a delta zset member.
//
// V3 layout: "delta|{type}|{path}|{tsSeq}|{uuid}". The uuid is the OSS
// object identifier (allocated client-side at WriteBegin); it lives in
// the member so Read can resolve the storage key without a side hash.
func EncodeDeltaMember(field string, mergeType MergeType, tsSeq TimeSeqID, uuid string) string {
	return fmt.Sprintf("delta|%d|%s|%s|%s", mergeType, field, tsSeq, uuid)
}

// DecodeDeltaMember parses a delta member and verifies its score
// matches the embedded tsSeq.
func DecodeDeltaMember(member string, score float64) (*DeltaInfo, error) {
	parts := strings.Split(member, "|")
	if len(parts) != 5 || parts[0] != "delta" {
		return nil, fmt.Errorf("invalid delta member %q", member)
	}
	mt, err := strconv.Atoi(parts[1])
	if err != nil || mt < 1 || mt > 3 {
		return nil, fmt.Errorf("invalid merge type in %q", member)
	}
	if err := utils.ValidateFieldPath(parts[2]); err != nil {
		return nil, fmt.Errorf("invalid path in %q: %w", member, err)
	}
	tsSeq, err := ParseTimeSeqID(parts[3])
	if err != nil {
		return nil, fmt.Errorf("invalid tsSeq in %q: %w", member, err)
	}
	if tsSeq.Score() != score {
		return nil, fmt.Errorf("score mismatch in %q (member=%.6f, redis=%.6f)", member, tsSeq.Score(), score)
	}
	if parts[4] == "" {
		return nil, fmt.Errorf("empty uuid in %q", member)
	}
	return &DeltaInfo{
		Member:    member,
		Score:     score,
		Path:      parts[2],
		TsSeq:     tsSeq,
		MergeType: MergeTypeFromInt(mt),
		UUID:      parts[4],
	}, nil
}

// EncodeSnapValue / DecodeSnapValue handle the value stored under
// "<prefix>:snaps", keyed by catalog. The value is just "{stopTsSeq}".
func EncodeSnapValue(stopTsSeq TimeSeqID) string { return stopTsSeq.String() }

func DecodeSnapValue(value string) (TimeSeqID, error) {
	t, err := ParseTimeSeqID(value)
	if err != nil {
		return TimeSeqID{}, fmt.Errorf("invalid snap value: %w", err)
	}
	return t, nil
}

func IsDeltaMember(m string) bool { return strings.HasPrefix(m, "delta|") }
