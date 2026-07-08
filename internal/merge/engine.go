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

// ownedMerger is the optional in-place variant: mergeOwned may reuse
// original's backing array, so the engine calls it only when it owns the
// buffer — i.e. from the second merge step onward, where the input is the
// previous step's freshly allocated output, never the caller's baseData or a
// cached slice.
type ownedMerger interface {
	mergeOwned(original, data []byte, field string) ([]byte, error)
}

// Merge applies the given delta entries to baseData in order and returns the
// merged document. Entries must have Body populated; an empty Body is treated
// as a data-integrity error rather than silently skipped.
//
// NOTE for future optimizers: consecutive RFC 7396 patches must NOT be
// precombined (jsonpatch.MergeMergePatches) — merge-patch application is only
// left-associative. Counterexample pinned in TestRFC7396PrecombineUnsound:
// doc {"k":{"old":2}}, p1 {"k":null}, p2 {"k":{"a":1}} — sequential
// application yields {"k":{"a":1}}, the precombined patch resurrects "old".
func Merge(baseData []byte, entries []index.DeltaInfo) ([]byte, error) {
	merged := baseData
	owned := false // does merged belong to us (vs the caller / a cache)?
	for _, entry := range entries {
		if len(entry.Body) == 0 {
			return nil, fmt.Errorf("missing body data for delta entry: path=%s, tsSeq=%s, mergeType=%d", entry.Path, entry.TsSeq.String(), entry.MergeType)
		}

		merger, ok := mergers[int(entry.MergeType)]
		if !ok {
			return nil, fmt.Errorf("unknown merge type: %d", entry.MergeType)
		}

		var err error
		if om, isOwned := merger.(ownedMerger); owned && isOwned {
			merged, err = om.mergeOwned(merged, entry.Body, ToGjsonPath(entry.Path))
		} else {
			merged, err = merger.Merge(merged, entry.Body, ToGjsonPath(entry.Path))
		}
		if err != nil {
			// Identify the exact offending delta: a single unappliable patch
			// fails every read of the catalog, and the tsSeq printed here is
			// precisely what Client.RemoveDelta takes to clear it.
			return nil, fmt.Errorf("merge failed (path=%s tsSeq=%s uri=%s type=%d): %w",
				entry.Path, entry.TsSeq, entry.URI, entry.MergeType, err)
		}
		// Every merger returns a buffer the caller may own: root Replace
		// copies, everything else allocates (or, when owned, reuses OUR
		// buffer).
		owned = true
	}
	return merged, nil
}
