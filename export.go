package lake

import (
	"github.com/hkloudou/lake/v2/internal/index"
)

// MergeType defines how to merge values (re-exported from index for public API)
type MergeType = index.MergeType

// Merge type constants (re-exported for convenience)
const (
	MergeTypeUnknown = index.MergeTypeUnknown // Unknown/invalid merge type
	MergeTypeReplace = index.MergeTypeReplace // Replace existing value (simple set)
	MergeTypeRFC7396 = index.MergeTypeRFC7396 // RFC 7396 JSON Merge Patch
	MergeTypeRFC6902 = index.MergeTypeRFC6902 // RFC 6902 JSON Patch
)
