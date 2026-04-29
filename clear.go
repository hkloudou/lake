package lake

import (
	"context"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// ClearHistory clears all expired deltas and merged snaps.
// Equivalent to ClearHistoryWithRetention(ctx, catalog, 0).
func (c *Client) ClearHistory(ctx context.Context, catalog string) error {
	return c.ClearHistoryWithRetention(ctx, catalog, 0)
}

// ClearHistoryWithRetention clears expired deltas and merged snaps while
// keeping the latest snapshot plus keepSnaps historical snapshots.
//
//   - keepSnaps = 0: only keep the latest snap, remove all historical snaps
//   - keepSnaps = N: keep the latest snap + N historical snaps (N+1 total)
//   - The latest snap is always kept; keepSnaps only affects historical ones.
//   - Deltas are cleared up to the latest snap regardless of keepSnaps.
//
// SingleFlight de-duplicates concurrent clears on the same catalog.
func (c *Client) ClearHistoryWithRetention(ctx context.Context, catalog string, keepSnaps int) error {
	if err := utils.ValidateCatalog(catalog); err != nil {
		return err
	}
	_, err := c.clearFlight.Do(catalog, func() (struct{}, error) {
		return struct{}{}, c.doClearHistoryOptimized(ctx, catalog, keepSnaps)
	})
	return err
}
