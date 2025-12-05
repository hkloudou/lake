package lake

import (
	"context"
	"fmt"
)

// ClearHistory clears all expired deltas and merged snaps
// This is equivalent to ClearHistoryWithRetention(ctx, catalog, 0)
func (c *Client) ClearHistory(ctx context.Context, catalog string) error {
	return c.ClearHistoryWithRetention(ctx, catalog, 0)
}

// ClearHistoryWithRetention clears expired deltas and merged snaps while keeping the latest snapshot and N historical snapshots
// keepSnaps: number of historical snapshots to keep (in addition to the latest one)
//   - keepSnaps = 0: only keep the latest snap, remove all historical snaps
//   - keepSnaps = 1: keep the latest snap + 1 historical snap (total 2)
//   - keepSnaps = 3: keep the latest snap + 3 historical snaps (total 4)
//   - Note: The latest snap is ALWAYS kept (excluded by < filter), keepSnaps only affects historical ones
//   - Deltas are cleared normally (up to the latest snap)
//
// Benefits: Reduces Redis size and OSS storage
// Trade-off: Cannot access historical versions (but you don't need them anyway)
//
// Uses SingleFlight to prevent concurrent clear operations on the same catalog (avoids deletion traffic storm)
func (c *Client) ClearHistoryWithRetention(ctx context.Context, catalog string, keepSnaps int) error {
	// Use SingleFlight to prevent concurrent deletion on same catalog
	key := fmt.Sprintf("%s_%d", catalog, keepSnaps)
	_, err := c.clearFlight.Do(key, func() (struct{}, error) {
		return struct{}{}, c.doClearHistoryWithRetention(ctx, catalog, keepSnaps)
	})
	return err
}

// doClearHistoryWithRetention is the actual implementation (wrapped by SingleFlight)
// Uses optimized version: batch Redis delete + concurrent Storage delete (10x performance improvement)
// See implementation in clear_optimized.go
func (c *Client) doClearHistoryWithRetention(ctx context.Context, catalog string, keepSnaps int) error {
	// Use optimized version (batch delete)
	return c.doClearHistoryOptimized(ctx, catalog, keepSnaps)
}
