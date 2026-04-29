package lake

import (
	"context"
	"fmt"
	"sync"

	"github.com/hkloudou/lake/v3/internal/index"
)

// doClearHistoryOptimized performs delta history cleanup as a best-effort,
// fire-and-forget operation:
//   - Concurrent delete of OSS objects (10-worker pool).
//   - Batch ZREM on the delta zset.
//
// V3 keeps only one snap per catalog (overwritten on each save), so this
// routine no longer needs to enumerate or delete historical snap entries
// — there are none. The previous OSS snap object from each overwrite is
// left orphaned in storage; it represents one (catalog, time-range) of
// dead bytes per save and is the explicit cost of avoiding read-side
// "snap not found" races. A future SweepOrphans tool can reclaim them.
//
// DESIGN DECISION (intentional, do not "fix"):
//
// Errors from clearDeltasBatch are intentionally NOT propagated. Cleanup
// is best-effort:
//   - Object-storage Delete is idempotent: retrying on the next call is safe.
//   - A surviving orphan delta is filtered out of reads by reader.go's
//     pending / score-bound logic, so it does not corrupt results.
//   - Surfacing partial failures would force every caller to write retry
//     logic for what is effectively a background cleanup chore.
// If you need a hard "deleted everything" guarantee, layer it above this
// API by re-listing and verifying.
func (c *Client) doClearHistoryOptimized(ctx context.Context, catalog string) error {
	c.emitEvent(catalog, "ClearHistory", nil)

	if err := c.ensureInitialized(ctx); err != nil {
		return err
	}

	readResult := c.reader.ReadSafeRemoveDeltas(ctx, catalog)
	if readResult.Err != nil {
		return fmt.Errorf("failed to read safe remove range: %w", readResult.Err)
	}

	if len(readResult.Deltas) > 0 {
		// Error intentionally swallowed; see DESIGN DECISION above.
		_ = c.clearDeltasBatch(ctx, catalog, readResult.Deltas)
	}
	return nil
}

// clearDeltasBatch deletes a batch of deltas: OSS objects in parallel,
// then a single ZREM on the delta zset.
func (c *Client) clearDeltasBatch(ctx context.Context, catalog string, deltas []index.DeltaInfo) error {
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

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				storageDeltaKey := c.storage.MakeDeltaKey(catalog, j.delta.TsSeq, int(j.delta.MergeType))
				if err := c.storage.Delete(ctx, storageDeltaKey); err != nil {
					errors <- fmt.Errorf("failed to delete delta %d (%s): %w", j.index, storageDeltaKey, err)
				}
			}
		}()
	}

	for i, delta := range deltas {
		jobs <- job{index: i, delta: delta}
	}
	close(jobs)
	wg.Wait()
	close(errors)

	var firstError error
	for err := range errors {
		if firstError == nil {
			firstError = err
		}
	}

	deltaZsetKey := c.writer.MakeDeltaZsetKey(catalog)
	members := make([]interface{}, len(deltas))
	for i, delta := range deltas {
		members[i] = delta.Member
	}

	if err := c.rdb.ZRem(ctx, deltaZsetKey, members...).Err(); err != nil {
		return fmt.Errorf("failed to batch delete delta members: %w", err)
	}
	return firstError
}
