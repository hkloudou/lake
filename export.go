package lake

import "github.com/hkloudou/lake/v3/internal/index"

// MergeType selects how a Write merges into the existing document.
type MergeType = index.MergeType

const (
	MergeTypeUnknown = index.MergeTypeUnknown
	MergeTypeReplace = index.MergeTypeReplace // simple field set
	MergeTypeRFC7396 = index.MergeTypeRFC7396 // JSON Merge Patch
)

// SnapInfo records a catalog's latest snapshot point.
// The OSS object key is Storage.MakeSnapKey(catalog, info.StopTsSeq).
type SnapInfo = index.SnapInfo

// TimeSeqID is the (timestamp, seqid) pair Lake stamps onto every write.
type TimeSeqID = index.TimeSeqID
