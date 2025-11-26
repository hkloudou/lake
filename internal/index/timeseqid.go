package index

import (
	"fmt"
	"strings"
)

// TimeSeqID represents a timestamp + sequence ID pair
type TimeSeqID struct {
	Timestamp int64 // Unix timestamp in seconds
	SeqID     int64 // Sequence ID within that second
}

// Score returns the score value for Redis ZADD
// Uses float64: timestamp as integer part, seqid as fractional part
// Example: ts=1700000000, seqid=123 -> 1700000000.000123
func (t TimeSeqID) Score() float64 {
	// Scale seqid to fractional part (supports up to 999999 ops/sec)
	return float64(t.Timestamp) + float64(t.SeqID)/1000000.0
}

// String returns the string representation used in filenames
// Format: {timestamp}_{seqid}
func (t TimeSeqID) String() string {
	return fmt.Sprintf("%d_%d", t.Timestamp, t.SeqID)
}

// ParseTimeSeqID parses a TimeSeqID from string format "timestamp_seqid"
// Validates:
// - Format must be "timestamp_seqid"
// - timestamp must be in range [0, 32503680000] (0 to year 3000)
// - seqPart length must be <= 6
// - seqPart must not start with 0 unless it is exactly "0"
// - Special case: "0_0" is valid (initial snapshot marker)
func ParseTimeSeqID(s string) (TimeSeqID, error) {
	// Special case: "0_0" is valid (initial snapshot marker)
	if s == "0_0" {
		return TimeSeqID{Timestamp: 0, SeqID: 0}, nil
	}

	// Split by underscore
	parts := strings.Split(s, "_")
	if len(parts) != 2 {
		return TimeSeqID{}, fmt.Errorf("invalid TimeSeqID format: %s (expected format: timestamp_seqid)", s)
	}

	tsPart := parts[0]
	seqPart := parts[1]

	// Validate: no minus sign (negative numbers not allowed)
	if strings.HasPrefix(tsPart, "-") {
		return TimeSeqID{}, fmt.Errorf("invalid timestamp: %s (negative values not allowed)", tsPart)
	}
	if strings.HasPrefix(seqPart, "-") {
		return TimeSeqID{}, fmt.Errorf("invalid seqid: %s (negative values not allowed)", seqPart)
	}

	// Validate: no scientific notation (e/E not allowed)
	if strings.ContainsAny(tsPart, "eE") {
		return TimeSeqID{}, fmt.Errorf("invalid timestamp: %s (scientific notation not allowed)", tsPart)
	}
	if strings.ContainsAny(seqPart, "eE") {
		return TimeSeqID{}, fmt.Errorf("invalid seqid: %s (scientific notation not allowed)", seqPart)
	}

	// Validate: no leading zeros (already handled "0_0" above)
	if strings.HasPrefix(tsPart, "0") {
		return TimeSeqID{}, fmt.Errorf("invalid timestamp: %s (cannot have leading zeros)", tsPart)
	}
	if strings.HasPrefix(seqPart, "0") {
		return TimeSeqID{}, fmt.Errorf("invalid seqid: %s (cannot have leading zeros)", seqPart)
	}

	// Validate seqPart length (max 6 digits for 999,999 ops/sec)
	if len(seqPart) > 6 {
		return TimeSeqID{}, fmt.Errorf("invalid seqid: %s (length %d > 6)", seqPart, len(seqPart))
	}

	// Parse timestamp and seqid
	var ts, seqid int64
	_, err := fmt.Sscanf(tsPart, "%d", &ts)
	if err != nil {
		return TimeSeqID{}, fmt.Errorf("invalid timestamp: %s", tsPart)
	}

	// Validate timestamp range: 0 (epoch) to 32503680000 (year 3000)
	// 0 is valid for initial snapshot marker (0_0)
	if ts < 0 || ts > 32503680000 {
		return TimeSeqID{}, fmt.Errorf("invalid timestamp: %d (must be in range [0, 32503680000], 0 to year 3000)", ts)
	}

	_, err = fmt.Sscanf(seqPart, "%d", &seqid)
	if err != nil {
		return TimeSeqID{}, fmt.Errorf("invalid seqid: %s", seqPart)
	}

	// Validate seqid range: 1 to 999,999
	// Note: seqid = 0 only allowed in special case "0_0" (already handled above)
	if seqid < 1 || seqid > 999999 {
		return TimeSeqID{}, fmt.Errorf("invalid seqid: %d (must be in range [1, 999999])", seqid)
	}

	return TimeSeqID{Timestamp: ts, SeqID: seqid}, nil
}
