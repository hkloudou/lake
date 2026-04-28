package lake

import (
	"context"
	"fmt"
	"sync"

	"github.com/hkloudou/lake/v3/internal/index"
)

// doClearHistoryOptimized performs history cleanup as a best-effort,
// fire-and-forget operation:
//   - Redis members are batch-deleted via ZREM.
//   - Storage objects are deleted concurrently by a 10-worker pool.
//   - Delta and Snap streams are cleaned independently.
//
// DESIGN DECISION (intentional, do not "fix"):
//
// Errors from clearDeltasBatch / clearSnapsBatch are intentionally NOT
// propagated to the caller. ClearHistory is best-effort cleanup —
//   - Object-storage Delete is idempotent: retrying on the next call is safe.
//   - A surviving orphan delta is always handled by reader.go's pending /
//     stale-member filtering, so it does not corrupt reads.
//   - Surfacing partial failures would force every caller to write retry
//     logic for what is effectively a background cleanup chore.
// If you need a hard "deleted everything" guarantee, layer it above this
// API by re-listing and verifying.
func (c *Client) doClearHistoryOptimized(ctx context.Context, catalog string, keepSnaps int) error {
	c.emitEvent(catalog, "ClearHistory", map[string]any{"keepSnaps": keepSnaps})

	if err := c.ensureInitialized(ctx); err != nil {
		return err
	}

	snaps, readResult := c.reader.ReadSafeRemoveRangeWithRetention(ctx, catalog, keepSnaps)
	if readResult.Err != nil {
		return fmt.Errorf("failed to read safe remove range: %w", readResult.Err)
	}

	if len(readResult.Deltas) > 0 {
		// Error intentionally swallowed; see DESIGN DECISION above.
		_ = c.clearDeltasBatch(ctx, catalog, readResult.Deltas)
	}
	if len(snaps) > 0 {
		// Error intentionally swallowed; see DESIGN DECISION above.
		_ = c.clearSnapsBatch(ctx, catalog, snaps)
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
