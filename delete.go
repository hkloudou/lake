package lake

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// DeleteCatalog removes a catalog from the index: its delta log, snapshot
// pointer, tsSeq allocator, and every indicator's cached sample. Returns
// whether the catalog had any index state (false: nothing to delete).
//
// Like Compact and RemoveDelta it touches Redis only — delta and snapshot
// OBJECTS stay in storage, where bucket lifecycle rules own deletion. The
// index step is atomic and bumps the catalog's removal generation, so a read
// or sample computation in flight across the delete can neither persist a
// snapshot nor cache a value from the deleted state; the catalog may be
// written again immediately and starts from an empty document.
//
// DESTRUCTIVE: every write to the catalog disappears from every future read.
func (c *Client) DeleteCatalog(ctx context.Context, catalog string) (bool, error) {
	c.emitEvent(catalog, "DeleteCatalog", nil)
	if err := utils.ValidateCatalog(catalog); err != nil {
		return false, err
	}
	existed, err := c.writer.DeleteCatalog(ctx, catalog)
	if err != nil || !existed {
		return existed, err
	}
	// Same sample barrier RemoveDelta installs (an in-flight first-ever
	// sampler must not land a value computed from the deleted log), then
	// reclaim the memo entries eagerly; stale ones are rejected at read time
	// by their generation regardless.
	if err := c.sampleRdb.HIncrBy(ctx, c.reader.MakeSampleRemoveGenKey(), catalog, 1).Err(); err != nil {
		return true, fmt.Errorf("catalog deleted, but sample barrier failed: %w", err)
	}
	if err := c.sweepSamples(ctx, catalog); err != nil {
		return true, fmt.Errorf("catalog deleted, but memo sweep failed (retry InvalidateSamples to reclaim now): %w", err)
	}
	return true, nil
}
