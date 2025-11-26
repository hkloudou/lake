package merge

import (
	"fmt"

	"github.com/hkloudou/lake/v2/internal/index"
)

// Global merger instances (stateless, safe to share)
var (
	replaceMerger = NewReplaceMerger()
	rfc7396Merger = NewRFC7396Merger()
	rfc6902Merger = NewRFC6902Merger()
)

// mergers maps merge type to merger implementation
var mergers = map[int]Merger{
	1: replaceMerger, // index.MergeTypeReplace
	2: rfc7396Merger, // index.MergeTypeRFC7396
	3: rfc6902Merger, // index.MergeTypeRFC6902
}

func Merge(catalog string, baseData []byte, entries []index.DeltaInfo) ([]byte, map[string]index.TimeSeqID, error) {
	merged := baseData
	var updatedAtMap = make(map[string]index.TimeSeqID, 0)
	for _, entry := range entries {
		// Use pre-loaded Body data (filled by fillDeltasBody)
		if len(entry.Body) == 0 {
			// Empty body means storage load failed - this is a data integrity error
			return nil, nil, fmt.Errorf("missing body data for delta entry: path=%s, tsSeq=%s, mergeType=%d", entry.Path, entry.TsSeq.String(), entry.MergeType)
		}

		// Get merger by type
		merger, ok := mergers[int(entry.MergeType)]
		if !ok {
			return nil, nil, fmt.Errorf("unknown merge type: %d", entry.MergeType)
		}

		// Apply merge using unified interface
		var err error
		merged, err = merger.Merge(merged, entry.Body, ToGjsonPath(entry.Path))
		if err != nil {
			return nil, nil, fmt.Errorf("merge failed (type=%d): %w", entry.MergeType, err)
		}
	}
	return merged, updatedAtMap, nil
}
