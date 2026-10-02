package lake

import (
	"context"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/utils"
)

// RemoveDelta is the operator's escape hatch for a poison delta — one whose
// body cannot be merged and therefore fails every read of the catalog. The
// merge error names its tsSeq ("{timestamp}_{seqid}") exactly for this call.
// Only the index entry goes; the body object stays. Returns whether an entry
// was removed.
//
// The removal bumps the catalog's removal generation atomically, so a read
// in flight that listed the removed delta can neither persist a snapshot nor
// have its cached sample served afterwards (both carry the generation they
// were computed under). Snapshots that already absorbed the delta keep it:
// this unblocks the log, it does not rewrite history.
//
// DESTRUCTIVE: the removed write disappears from every future read.
func (c *Client) RemoveDelta(ctx context.Context, catalog, tsSeq string) (bool, error) {
	c.emitEvent(catalog, "RemoveDelta", map[string]any{"tsSeq": tsSeq})
	if err := utils.ValidateCatalog(catalog); err != nil {
		return false, err
	}
	id, err := index.ParseTimeSeqID(tsSeq)
	if err != nil {
		return false, err
	}
	return c.idx.RemoveDelta(ctx, catalog, id)
}
