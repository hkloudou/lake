package index

import (
	"fmt"
	"strconv"
	"strings"
)

// TimeSeqID is a (Unix-second timestamp, intra-second seqid) pair. The
// pair is encoded as a Redis ZSet score
// (timestamp + seqid/1e6, 6-decimal precision).
type TimeSeqID struct {
	Timestamp int64 // Unix seconds
	SeqID     int64 // 1..999999 within that second; 0 only inside the sentinel "0_0"
}

// Score returns the float used as Redis ZADD score.
// Example: ts=1700000000, seqid=123 → 1700000000.000123.
func (t TimeSeqID) Score() float64 {
	return float64(t.Timestamp) + float64(t.SeqID)/1000000.0
}

// String renders as "{timestamp}_{seqid}", the on-disk filename form.
func (t TimeSeqID) String() string {
	return fmt.Sprintf("%d_%d", t.Timestamp, t.SeqID)
}

// ParseTimeSeqID parses "{timestamp}_{seqid}".
//
// Rules: timestamp ∈ [0, year-3000]; seqid ∈ [1, 999999]; no leading
// zeros (except the literal sentinel "0_0", which represents "no prior
// snap"); no negatives, no scientific notation.
func ParseTimeSeqID(s string) (TimeSeqID, error) {
	if s == "0_0" {
		return TimeSeqID{}, nil
	}
	tsPart, seqPart, ok := strings.Cut(s, "_")
	if !ok {
		return TimeSeqID{}, fmt.Errorf("invalid TimeSeqID %q (want timestamp_seqid)", s)
	}
	ts, err := parseTsSeqPart(tsPart, "timestamp")
	if err != nil {
		return TimeSeqID{}, err
	}
	if ts > 32503680000 { // year 3000
		return TimeSeqID{}, fmt.Errorf("invalid timestamp: %d (must be ≤ 32503680000)", ts)
	}
	seq, err := parseTsSeqPart(seqPart, "seqid")
	if err != nil {
		return TimeSeqID{}, err
	}
	if seq < 1 || seq > 999999 || len(seqPart) > 6 {
		return TimeSeqID{}, fmt.Errorf("invalid seqid: %s (must be 1..999999, ≤6 digits)", seqPart)
	}
	return TimeSeqID{Timestamp: ts, SeqID: seq}, nil
}

func parseTsSeqPart(s, label string) (int64, error) {
	if s == "" || strings.HasPrefix(s, "-") || strings.HasPrefix(s, "0") || strings.ContainsAny(s, "eE") {
		return 0, fmt.Errorf("invalid %s: %q (no leading zero / negative / scientific)", label, s)
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %s", label, s)
	}
	return v, nil
}
