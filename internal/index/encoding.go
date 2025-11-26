package index

import (
	"fmt"
	"strings"

	"github.com/hkloudou/lake/v2/internal/utils"
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

// DecodeDeltaMember decodes Redis ZADD member into field, mergeType and tsSeq
// Format: "delta|{mergeType}|{field}|{tsSeq}"
func DecodeDeltaMember(member string) (fieldPath string, mergeType MergeType, tsSeq TimeSeqID, err error) {
	// Split by "|" delimiter
	parts := strings.Split(member, "|")
	if len(parts) != 4 {
		return "", 0, TimeSeqID{}, fmt.Errorf("invalid member format (expected 4 parts): %s", member)
	}

	if parts[0] != "delta" {
		return "", 0, TimeSeqID{}, fmt.Errorf("invalid member prefix (expected 'delta'): %s", parts[0])
	}

	// Parse merge type
	var mergeTypeInt int
	_, err = fmt.Sscanf(parts[1], "%d", &mergeTypeInt)
	if err != nil {
		return "", 0, TimeSeqID{}, fmt.Errorf("invalid merge type: %s", parts[1])
	}

	fieldPath = parts[2]
	if err := utils.ValidateFieldPath(fieldPath); err != nil {
		return "", 0, TimeSeqID{}, fmt.Errorf("invalid field path: %w", err)
	}

	// Parse tsSeq
	tsSeq, err = ParseTimeSeqID(parts[3])
	if err != nil {
		return "", 0, TimeSeqID{}, fmt.Errorf("invalid tsSeq: %w", err)
	}

	return fieldPath, MergeTypeFromInt(mergeTypeInt), tsSeq, nil
}

// EncodeSnapMember encodes snapshot time range into Redis ZADD member format
// Format: "snap|{startTsSeq}|{stopTsSeq}"
// Example: "snap|1700000000_1|1700000100_500"
// If no previous snap (first snap): "snap|0_0|1700000100_500"
func EncodeSnapMember(startTsSeq, stopTsSeq TimeSeqID) string {
	return fmt.Sprintf("snap|%s|%s", startTsSeq, stopTsSeq)
}

// DecodeSnapMember decodes snapshot member and returns start and stop tsSeqID
func DecodeSnapMember(member string) (startTsSeq, stopTsSeq TimeSeqID, err error) {
	// Split by "|" delimiter
	parts := strings.Split(member, "|")
	if len(parts) != 3 {
		return TimeSeqID{}, TimeSeqID{}, fmt.Errorf("invalid snap member format (expected 3 parts): %s", member)
	}

	if parts[0] != "snap" {
		return TimeSeqID{}, TimeSeqID{}, fmt.Errorf("invalid snap member prefix (expected 'snap'): %s", parts[0])
	}
	startTsSeq, err = ParseTimeSeqID(parts[1])
	if err != nil {
		return TimeSeqID{}, TimeSeqID{}, fmt.Errorf("failed to parse start tsSeqID: %w", err)
	}
	stopTsSeq, err = ParseTimeSeqID(parts[2])
	if err != nil {
		return TimeSeqID{}, TimeSeqID{}, fmt.Errorf("failed to parse stop tsSeqID: %w", err)
	}

	return startTsSeq, stopTsSeq, nil
}

// IsSnapMember checks if member is a snapshot member
func IsSnapMember(member string) bool {
	return strings.HasPrefix(member, "snap|")
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
