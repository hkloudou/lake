package lake

import (
	"github.com/hkloudou/lake/v3/internal/index"
)

// MergeType defines how to merge values (re-exported from index for public API).
type MergeType = index.MergeType

// Merge type constants (re-exported for convenience).
const (
	MergeTypeUnknown = index.MergeTypeUnknown // Unknown/invalid merge type
	MergeTypeReplace = index.MergeTypeReplace // Replace existing value (simple set)
	MergeTypeRFC7396 = index.MergeTypeRFC7396 // RFC 7396 JSON Merge Patch
	MergeTypeRFC6902 = index.MergeTypeRFC6902 // RFC 6902 JSON Patch
)

// SnapInfo describes the time range covered by a catalog's latest
// snapshot. Re-exported from internal/index so callers do not need to
// import an internal package.
//
// The OSS object key for the snap is derivable as
// Storage.MakeSnapKey(catalog, info.StartTsSeq, info.StopTsSeq).
type SnapInfo = index.SnapInfo

// TimeSeqID is the (timestamp, seqid) pair Lake stamps onto every write
// and snap. Re-exported for callers that need to construct or inspect
// SnapInfo values directly.
type TimeSeqID = index.TimeSeqID
