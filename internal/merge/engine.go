package merge

import (
	"fmt"

	"github.com/hkloudou/lake/v3/internal/index"
)

// Stateless merger instances, safe to share.
var (
	replaceMerger = NewReplaceMerger()
	rfc7396Merger = NewRFC7396Merger()
)

var mergers = map[int]Merger{
	1: replaceMerger, // index.MergeTypeReplace
	2: rfc7396Merger, // index.MergeTypeRFC7396
}

// Merge applies the given delta entries to baseData in order and returns the
// merged document. Entries must have Body populated; an empty Body is treated
// as a data-integrity error rather than silently skipped.
func Merge(baseData []byte, entries []index.DeltaInfo) ([]byte, error) {
	merged := baseData
	for _, entry := range entries {
		if len(entry.Body) == 0 {
			return nil, fmt.Errorf("missing body data for delta entry: path=%s, tsSeq=%s, mergeType=%d", entry.Path, entry.TsSeq.String(), entry.MergeType)
		}

		merger, ok := mergers[int(entry.MergeType)]
		if !ok {
			return nil, fmt.Errorf("unknown merge type: %d", entry.MergeType)
		}

		var err error
		merged, err = merger.Merge(merged, entry.Body, ToGjsonPath(entry.Path))
		if err != nil {
			// Identify the exact offending delta: a single unappliable patch
			// fails every read of the catalog, so operators need its tsSeq /
			// uuid to locate (MakeDeltaKey) and remove it.
			return nil, fmt.Errorf("merge failed (path=%s tsSeq=%s uuid=%s type=%d): %w",
				entry.Path, entry.TsSeq, entry.UUID, entry.MergeType, err)
		}
	}
	return merged, nil
}
