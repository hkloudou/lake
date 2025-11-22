package index

import (
	"encoding/base64"
	"fmt"
	"strings"
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

// EncodeMember encodes field, tsSeqID, and mergeType into Redis ZADD member format
// Format: "data|{base64_field}|{tsSeqID}|{mergeTypeInt}"
// Example: "data|dXNlci5uYW1l|1700000000_123|0"
func EncodeDeltaMember(field, tsSeqID string, mergeType MergeType) string {
	// Encode field using base64 URL encoding (safe for Redis keys)
	encodedField := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString([]byte(field))
	return fmt.Sprintf("delta|%s|%s|%d", encodedField, tsSeqID, mergeType)
}

// DecodeMember decodes Redis ZADD member into field, tsSeqID, and mergeType
// Returns tsSeqID in format "ts_seqid"
func DecodeDeltaMember(member string) (field, tsSeqID string, mergeType MergeType, err error) {
	// Split by "|" delimiter
	parts := strings.Split(member, "|")
	if len(parts) != 4 {
		return "", "", 0, fmt.Errorf("invalid member format (expected 4 parts): %s", member)
	}

	if parts[0] != "delta" {
		return "", "", 0, fmt.Errorf("invalid member prefix (expected 'delta'): %s", parts[0])
	}

	// Decode base64 field
	fieldBytes, err := base64.URLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", 0, fmt.Errorf("failed to decode field: %w", err)
	}
	field = string(fieldBytes)

	// Parse tsSeqID (already in correct format)
	tsSeqID = parts[2]

	// Parse merge type
	var mergeTypeInt int
	_, err = fmt.Sscanf(parts[3], "%d", &mergeTypeInt)
	if err != nil {
		return "", "", 0, fmt.Errorf("invalid merge type: %s", parts[3])
	}

	return field, tsSeqID, MergeTypeFromInt(mergeTypeInt), nil
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
