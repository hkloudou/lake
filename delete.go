package lake

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// DeleteCatalog removes a catalog from the index — delta log, snapshot
// pointer, tsSeq allocator and every indicator's cached sample. Objects in
// storage stay. The index step is atomic and bumps the removal generation,
// so a read or sample computation in flight across the delete cannot
// persist a snapshot or be served from the deleted state; the catalog may be
// written again immediately from an empty document. Returns whether the
// catalog had any index state.
//
// DESTRUCTIVE: every write to the catalog disappears from every future read.
func (c *Client) DeleteCatalog(ctx context.Context, catalog string) (bool, error) {
	c.emitEvent(catalog, "DeleteCatalog", nil)
	if err := utils.ValidateCatalog(catalog); err != nil {
		return false, err
	}
	existed, err := c.idx.DeleteCatalog(ctx, catalog)
	if err != nil || !existed {
		return existed, err
	}
	if err := c.sweepSamples(ctx, catalog); err != nil {
		return true, fmt.Errorf("catalog deleted, but memo sweep failed (stale entries are ignored on read): %w", err)
	}
	return true, nil
}
