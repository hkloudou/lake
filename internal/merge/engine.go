package merge

import (
	"fmt"

	"github.com/hkloudou/lake/v3/internal/index"
)

// Merge applies the delta entries to baseData in order and returns the merged
// document. Entries must have Body populated. The result never aliases an
// entry's Body, so a caller may mutate it while the bodies stay memoised on
// the ListResult.
//
// Consecutive RFC 7396 patches must NOT be precombined — merge-patch
// application is only left-associative (TestRFC7396PrecombineUnsound).
func Merge(baseData []byte, entries []index.DeltaInfo) ([]byte, error) {
	merged := baseData
	for _, e := range entries {
		if len(e.Body) == 0 {
			return nil, fmt.Errorf("missing body data for delta entry: path=%s, tsSeq=%s", e.Path, e.TsSeq)
		}
		var err error
		switch e.MergeType {
		case index.MergeTypeReplace:
			merged, err = replace(merged, e.Body, ToGjsonPath(e.Path))
		case index.MergeTypeRFC7396:
			merged, err = mergePatch(merged, e.Body, ToGjsonPath(e.Path))
		default:
			err = fmt.Errorf("unknown merge type: %d", e.MergeType)
		}
		if err != nil {
			// Name the exact offending delta: one unappliable body fails every
			// read of the catalog, and this tsSeq is what RemoveDelta takes.
			return nil, fmt.Errorf("merge failed (path=%s tsSeq=%s uri=%s type=%d): %w",
				e.Path, e.TsSeq, e.URI, e.MergeType, err)
		}
	}
	return merged, nil
}
