package lake

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v2/trace"
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
func (c *Client) ClearHistoryWithRetention(ctx context.Context, catalog string, keepSnaps int) error {
	tr := trace.FromContext(ctx)

	// Ensure initialized before operation
	if err := c.ensureInitialized(ctx); err != nil {
		return err
	}
	tr.RecordSpan("ClearHistoryDelta.Init", map[string]any{
		"keepSnaps": keepSnaps,
	})
	snaps, readResult := c.reader.ReadSafeRemoveRangeWithRetention(ctx, catalog, keepSnaps)
	if readResult.Err != nil {
		return fmt.Errorf("failed to read safe remove range: %w", readResult.Err)
	}

	// if snap == nil {
	// 	return nil
	// }
	// tr.RecordSpan("ClearHistoryDelta.GetLatestSnap", map[string]any{
	// 	"snap": fmt.Sprintf("%s ~ %s", snap.StartTsSeq.String(), snap.StopTsSeq.String()),
	// })

	// readResult := c.reader.ReadRange(ctx, catalog, 0, snap.StopTsSeq.Score())
	// if readResult.Err != nil {
	// 	return fmt.Errorf("failed to read range: %w", readResult.Err)
	// }

	// if readResult.HasPending {
	// 	return fmt.Errorf("pending writes detected: %w", readResult.Err)
	// }
	deltaZsetKey := c.writer.MakeDeltaZsetKey(catalog)
	// snapKey := c.writer.makeSnapKey(catalog)

	for i := 0; i < len(readResult.Deltas); i++ {
		delta := readResult.Deltas[i]
		storageDeltaKey := c.storage.MakeDeltaKey(catalog, delta.TsSeq, int(delta.MergeType))
		if err := c.storage.Delete(ctx, storageDeltaKey); err != nil {
			tr.RecordSpan("ClearHistoryDelta.DeleteDelta.Error", map[string]any{
				"storageDeltaKey": storageDeltaKey,
				"error":           err.Error(),
			})
			continue
			// return fmt.Errorf("failed to delete delta: %w", err)
		}

		tr.RecordSpan("ClearHistoryDelta.DeleteDelta", map[string]any{
			"delta":        delta.Member,
			"deltaZsetKey": deltaZsetKey,
		})

		if err := c.rdb.ZRem(ctx, deltaZsetKey, delta.Member).Err(); err != nil {
			tr.RecordSpan("ClearHistoryDelta.DeleteDelta.Error", map[string]any{
				"delta":        delta.Member,
				"deltaZsetKey": deltaZsetKey,
				"error":        err.Error(),
			})
			continue
		}
		tr.RecordSpan("ClearHistoryDelta.DeleteDelta.Done", map[string]any{
			"delta":        delta.Member,
			"deltaZsetKey": deltaZsetKey,
		})
	}

	for _, snap := range snaps {
		storageSnapKey := c.storage.MakeSnapKey(catalog, snap.StartTsSeq, snap.StopTsSeq)
		tr.RecordSpan("ClearHistoryDelta.DeleteSnap", map[string]any{
			"storageSnapKey": storageSnapKey,
		})

		if err := c.storage.Delete(ctx, storageSnapKey); err != nil {
			tr.RecordSpan("ClearHistoryDelta.DeleteSnap.Error", map[string]any{
				"storageSnapKey": storageSnapKey,
				"error":          err.Error(),
			})
			continue
		}
		if err := c.rdb.ZRem(ctx, c.writer.MakeSnapZsetKey(catalog), snap.Member).Err(); err != nil {
			tr.RecordSpan("ClearHistoryDelta.DeleteSnap.Error", map[string]any{
				"storageSnapKey": storageSnapKey,
				"error":          err.Error(),
			})
			continue
		}
		tr.RecordSpan("ClearHistoryDelta.DeleteSnap.Done", map[string]any{
			"storageSnapKey": storageSnapKey,
		})
	}

	return nil
}
