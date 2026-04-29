package index

import (
	"fmt"
	"strings"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// MergeType defines how to merge values
type MergeType int

const (
	MergeTypeUnknown MergeType = 0 // Unknown merge type
	MergeTypeReplace MergeType = 1 // Replace existing value (simple set)
	MergeTypeRFC7396 MergeType = 2 // RFC 7396 JSON Merge Patch
	MergeTypeRFC6902 MergeType = 3 // RFC 6902 JSON Patch
)

// String returns the string representation
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

// MergeTypeFromInt converts int to MergeType
func MergeTypeFromInt(i int) MergeType {
	switch i {
	case 1:
		return MergeTypeReplace
	case 2:
		return MergeTypeRFC7396
	case 3:
		return MergeTypeRFC6902
	default:
		return MergeTypeUnknown
	}
}

// EncodeDeltaMember encodes field, mergeType and tsSeq into Redis ZADD member format
// Format: "delta|{mergeType}|{field}|{tsSeq}"
// Example: "delta|1|/user/name|1700000000_1"
// Note: tsSeq is required for snapshot time-range merging and keeping history
func EncodeDeltaMember(field string, mergeType MergeType, tsSeq TimeSeqID) string {
	return fmt.Sprintf("delta|%d|%s|%s", mergeType, field, tsSeq.String())
}

// DecodeDeltaMember decodes Redis ZADD entry into DeltaInfo
// Format: "delta|{mergeType}|{field}|{tsSeq}"
// Validates all fields and verifies tsSeq.Score() matches score
func DecodeDeltaMember(member string, score float64) (*DeltaInfo, error) {

	// Split by "|" delimiter
	parts := strings.Split(member, "|")
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid member format (expected 4 parts): %s", member)
	}

	if parts[0] != "delta" {
		return nil, fmt.Errorf("invalid member prefix (expected 'delta'): %s", parts[0])
	}

	// Parse merge type
	var mergeTypeInt int
	_, err := fmt.Sscanf(parts[1], "%d", &mergeTypeInt)
	if err != nil {
		return nil, fmt.Errorf("invalid merge type: %s", parts[1])
	}

	// Validate merge type range (1: Replace, 2: RFC7396, 3: RFC6902)
	if mergeTypeInt < 1 || mergeTypeInt > 3 {
		return nil, fmt.Errorf("invalid merge type: %d (must be 1-3)", mergeTypeInt)
	}

	fieldPath := parts[2]
	if err := utils.ValidateFieldPath(fieldPath); err != nil {
		return nil, fmt.Errorf("invalid field path: %w", err)
	}

	// Parse tsSeq
	tsSeq, err := ParseTimeSeqID(parts[3])
	if err != nil {
		return nil, fmt.Errorf("invalid tsSeq: %w", err)
	}

	// Verify tsSeq matches score (data integrity check)
	if tsSeq.Score() != score {
		return nil, fmt.Errorf("data integrity error: tsSeq in member %q (score=%.6f) doesn't match Redis score (%.6f)", member, tsSeq.Score(), score)
	}

	return &DeltaInfo{
		Member:    member,
		Score:     score,
		Path:      fieldPath,
		TsSeq:     tsSeq,
		MergeType: MergeTypeFromInt(mergeTypeInt),
	}, nil
}

// EncodeSnapValue encodes a snapshot time range as the value stored under
// the "<prefix>:snaps" Redis Hash, keyed by catalog. Format:
//
//	"{startTsSeq}|{stopTsSeq}"
//
// e.g. "1700000000_1|1700000100_500", or "0_0|1700000100_500" for the
// first snap on a catalog.
//
// The legacy "snap|" type-marker prefix used by the v3-pre ZSet encoding
// is dropped — snap values now live in their own Hash and never share a
// member namespace with delta entries.
func EncodeSnapValue(startTsSeq, stopTsSeq TimeSeqID) string {
	return fmt.Sprintf("%s|%s", startTsSeq, stopTsSeq)
}

// DecodeSnapValue parses a snap hash value "<startTsSeq>|<stopTsSeq>".
func DecodeSnapValue(value string) (startTsSeq, stopTsSeq TimeSeqID, err error) {
	parts := strings.Split(value, "|")
	if len(parts) != 2 {
		return TimeSeqID{}, TimeSeqID{}, fmt.Errorf("invalid snap value format (expected 2 parts): %s", value)
	}
	startTsSeq, err = ParseTimeSeqID(parts[0])
	if err != nil {
		return TimeSeqID{}, TimeSeqID{}, fmt.Errorf("failed to parse start tsSeqID: %w", err)
	}
	stopTsSeq, err = ParseTimeSeqID(parts[1])
	if err != nil {
		return TimeSeqID{}, TimeSeqID{}, fmt.Errorf("failed to parse stop tsSeqID: %w", err)
	}
	return startTsSeq, stopTsSeq, nil
}

// IsDeltaMember checks if member is a delta member
func IsDeltaMember(member string) bool {
	return strings.HasPrefix(member, "delta|")
}

// IsPendingMember checks if member is a pending (uncommitted) member
func IsPendingMember(member string) bool {
	return strings.HasPrefix(member, "pending|")
}

// ParsePendingMemberTimestamp extracts TimeSeqID from a pending member
// Format: "pending|delta|{mergeType}|{field}|{tsSeq}"
// Example: "pending|delta|1|/user/name|1700000000_1"
func ParsePendingMemberTimestamp(member string) (TimeSeqID, error) {
	parts := strings.Split(member, "|")
	if len(parts) < 5 {
		return TimeSeqID{}, fmt.Errorf("invalid pending member format")
	}

	// parts[4] is tsSeq
	return ParseTimeSeqID(parts[4])
}
