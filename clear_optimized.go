package lake

import (
	"context"
	"fmt"
	"sync"

	"github.com/hkloudou/lake/v3/internal/index"
)

// doClearHistoryOptimized deletes deltas at or before the catalog's latest
// snap. Best-effort: storage Delete errors are swallowed (idempotent
// retries on next call; surviving orphan deltas are filtered by the
// reader's score-bound logic so they never poison results).
func (c *Client) doClearHistoryOptimized(ctx context.Context, catalog string) error {
	c.emitEvent(catalog, "ClearHistory", nil)

	if err := c.ensureInitialized(ctx); err != nil {
		return err
	}
	res := c.reader.ReadSafeRemoveDeltas(ctx, catalog)
	if res.Err != nil {
		return fmt.Errorf("read safe-remove range: %w", res.Err)
	}
	if len(res.Deltas) > 0 {
		_ = c.clearDeltasBatch(ctx, catalog, res.Deltas)
	}
	return nil
}

// clearDeltasBatch deletes a batch of deltas: storage objects in
// parallel (10 workers), then a single ZREM on the delta zset.
func (c *Client) clearDeltasBatch(ctx context.Context, catalog string, deltas []index.DeltaInfo) error {
	jobs := make(chan index.DeltaInfo, len(deltas))
	errs := make(chan error, len(deltas))
	var wg sync.WaitGroup

	workers := 10
	if len(deltas) < workers {
		workers = len(deltas)
	}
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			for d := range jobs {
				key := c.storage.MakeDeltaKey(catalog, d.TsSeq, int(d.MergeType))
				if err := c.storage.Delete(ctx, key); err != nil {
					errs <- fmt.Errorf("delete %s: %w", key, err)
				}
			}
		})
	}
	for _, d := range deltas {
		jobs <- d
	}
	close(jobs)
	wg.Wait()
	close(errs)

	var firstErr error
	for err := range errs {
		if firstErr == nil {
			firstErr = err
		}
	}

	members := make([]any, len(deltas))
	for i, d := range deltas {
		members[i] = d.Member
	}
	if err := c.rdb.ZRem(ctx, c.writer.MakeDeltaZsetKey(catalog), members...).Err(); err != nil {
		return fmt.Errorf("zrem delta members: %w", err)
	}
	return firstErr
}
