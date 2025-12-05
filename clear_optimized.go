package lake

import (
	"context"
	"fmt"
	"sync"

	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/hkloudou/lake/v2/trace"
)

// doClearHistoryOptimized is the optimized version of history cleanup
// Key optimizations:
// 1. Batch delete Redis members (using ZREM batch delete)
// 2. Concurrent delete Storage files (10 workers in parallel)
// 3. Separate Delta and Snap cleanup flows for independent control
func (c *Client) doClearHistoryOptimized(ctx context.Context, catalog string, keepSnaps int) error {
	tr := trace.FromContext(ctx)

	// Ensure initialized before operation
	if err := c.ensureInitialized(ctx); err != nil {
		return err
	}
	tr.RecordSpan("ClearHistoryOptimized.Init", map[string]any{
		"keepSnaps": keepSnaps,
	})

	snaps, readResult := c.reader.ReadSafeRemoveRangeWithRetention(ctx, catalog, keepSnaps)
	if readResult.Err != nil {
		return fmt.Errorf("failed to read safe remove range: %w", readResult.Err)
	}

	// Optimization 1: Batch delete Deltas
	if len(readResult.Deltas) > 0 {
		if err := c.clearDeltasBatch(ctx, catalog, readResult.Deltas); err != nil {
			tr.RecordSpan("ClearHistoryOptimized.ClearDeltas.Error", map[string]any{
				"error": err.Error(),
			})
			// Don't return, continue to clear snaps
		} else {
			tr.RecordSpan("ClearHistoryOptimized.ClearDeltas.Success", map[string]any{
				"count": len(readResult.Deltas),
			})
		}
	}

	// Optimization 2: Batch delete Snaps
	if len(snaps) > 0 {
		if err := c.clearSnapsBatch(ctx, catalog, snaps); err != nil {
			tr.RecordSpan("ClearHistoryOptimized.ClearSnaps.Error", map[string]any{
				"error": err.Error(),
			})
		} else {
			tr.RecordSpan("ClearHistoryOptimized.ClearSnaps.Success", map[string]any{
				"count": len(snaps),
			})
		}
	}

	return nil
}

// clearDeltasBatch batch clears Deltas (concurrent Storage delete + batch Redis delete)
func (c *Client) clearDeltasBatch(ctx context.Context, catalog string, deltas []index.DeltaInfo) error {
	// Step 1: Concurrent delete Storage files (10 workers)
	type job struct {
		index int
		delta index.DeltaInfo
	}

	jobs := make(chan job, len(deltas))
	errors := make(chan error, len(deltas))
	var wg sync.WaitGroup

	maxWorkers := 10
	if len(deltas) < maxWorkers {
		maxWorkers = len(deltas)
	}

	// Start workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				storageDeltaKey := c.storage.MakeDeltaKey(catalog, j.delta.TsSeq, int(j.delta.MergeType))
				if err := c.storage.Delete(ctx, storageDeltaKey); err != nil {
					// Record error but don't block
					errors <- fmt.Errorf("failed to delete delta %d (%s): %w", j.index, storageDeltaKey, err)
				}
			}
		}()
	}

	// Send jobs
	for i, delta := range deltas {
		jobs <- job{index: i, delta: delta}
	}
	close(jobs)
	wg.Wait()
	close(errors)

	// Collect errors (non-blocking)
	var firstError error
	for err := range errors {
		if firstError == nil {
			firstError = err
		}
	}

	// Step 2: Batch delete Redis members (single ZREM for all)
	deltaZsetKey := c.writer.MakeDeltaZsetKey(catalog)
	members := make([]interface{}, len(deltas))
	for i, delta := range deltas {
		members[i] = delta.Member
	}

	if err := c.rdb.ZRem(ctx, deltaZsetKey, members...).Err(); err != nil {
		return fmt.Errorf("failed to batch delete delta members: %w", err)
	}

	return firstError // Return first storage error (if any)
}

// clearSnapsBatch batch clears Snaps (concurrent Storage delete + batch Redis delete)
func (c *Client) clearSnapsBatch(ctx context.Context, catalog string, snaps []index.SnapInfo) error {
	// Step 1: Concurrent delete Storage files
	type job struct {
		index int
		snap  index.SnapInfo
	}

	jobs := make(chan job, len(snaps))
	errors := make(chan error, len(snaps))
	var wg sync.WaitGroup

	maxWorkers := 10
	if len(snaps) < maxWorkers {
		maxWorkers = len(snaps)
	}

	// Start workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				storageSnapKey := c.storage.MakeSnapKey(catalog, j.snap.StartTsSeq, j.snap.StopTsSeq)
				if err := c.storage.Delete(ctx, storageSnapKey); err != nil {
					errors <- fmt.Errorf("failed to delete snap %d (%s): %w", j.index, storageSnapKey, err)
				}
			}
		}()
	}

	// Send jobs
	for i, snap := range snaps {
		jobs <- job{index: i, snap: snap}
	}
	close(jobs)
	wg.Wait()
	close(errors)

	// Collect errors
	var firstError error
	for err := range errors {
		if firstError == nil {
			firstError = err
		}
	}

	// Step 2: Batch delete Redis members
	snapZsetKey := c.writer.MakeSnapZsetKey(catalog)
	members := make([]interface{}, len(snaps))
	for i, snap := range snaps {
		members[i] = snap.Member
	}

	if err := c.rdb.ZRem(ctx, snapZsetKey, members...).Err(); err != nil {
		return fmt.Errorf("failed to batch delete snap members: %w", err)
	}

	return firstError
}
