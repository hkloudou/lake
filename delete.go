package lake

import (
	"context"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// DeleteCatalog removes a catalog from the index — delta log, snapshot
// pointer and tsSeq allocator — atomically, bumping the removal generation
// first, so a read or sample computation in flight across the delete cannot
// persist a snapshot or be served from the deleted state; the catalog may be
// written again immediately from an empty document. Returns whether the
// catalog had any index state.
//
// Objects in storage stay, and so do the catalog's cached samples: they are
// rejected by their generation on the next read and overwritten on the next
// compute. Nothing in Lake deletes or sweeps — see Collectable.
//
// DESTRUCTIVE: every write to the catalog disappears from every future read.
func (c *Client) DeleteCatalog(ctx context.Context, catalog string) (bool, error) {
	c.emitEvent(catalog, "DeleteCatalog", nil)
	if err := utils.ValidateCatalog(catalog); err != nil {
		return false, err
	}
	return c.idx.DeleteCatalog(ctx, catalog)
}
