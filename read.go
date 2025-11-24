package lake

import (
	"context"
	"fmt"
	"sync"

	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/hkloudou/lake/v2/internal/trace"
)

func (c *Client) readData(ctx context.Context, list *ListResult) ([]byte, error) {
	tr := trace.FromContext(ctx)

	// Check for pending writes error from List
	if list.Err != nil {
		return nil, list.Err
	}

	// If pending writes detected, return error
	if list.HasPending {
		return nil, fmt.Errorf("pending writes detected: %w", list.Err)
	}

	// Ensure initialized before operation
	if err := c.ensureInitialized(ctx); err != nil {
		return nil, err
	}
	tr.RecordSpan("Read.Init")

	// Parallel execution: load snapshot base data and delta bodies concurrently
	var baseData []byte
	var baseDataErr error
	var deltasErr error

	var wg sync.WaitGroup

	// Goroutine 1: Load snapshot base data from cache/storage
	wg.Add(1)
	go func() {
		defer wg.Done()
		if list.LatestSnap != nil {
			storageKey := c.storage.MakeSnapKey(list.catalog, list.LatestSnap.StartTsSeq, list.LatestSnap.StopTsSeq)
			tr.RecordSpan("Read.LoadSnapshot_Cache", map[string]interface{}{
				"startTsSeq": list.LatestSnap.StartTsSeq,
				"stopTsSeq":  list.LatestSnap.StopTsSeq,
				"storageKey": storageKey,
			})
			namespace := c.storage.RedisPrefix()

			// Use cache to load snapshot data with namespace
			baseData, baseDataErr = c.snapCache.Take(ctx, namespace, storageKey, func() ([]byte, error) {
				// Cache miss: load from storage
				return c.storage.Get(ctx, storageKey)
			})
		} else {
			tr.RecordSpan("Read.LoadSnapshot_Empty")
			baseData = []byte("{}")
		}
	}()

	// Goroutine 2: Load delta bodies concurrently (max 10 workers)
	wg.Add(1)
	go func() {
		defer wg.Done()
		deltasErr = c.fillDeltasBody(ctx, list.catalog, list.Entries)
	}()

	// Wait for both operations to complete
	wg.Wait()

	// Check for errors
	if baseDataErr != nil {
		return nil, fmt.Errorf("failed to load snapshot: %w", baseDataErr)
	}
	if deltasErr != nil {
		return nil, fmt.Errorf("failed to load deltas: %w", deltasErr)
	}

	tr.RecordSpan("Read.LoadData")

	// Merge entries with base data (pure CPU operation, all data loaded)
	resultData, err := c.merger.Merge(list.catalog, baseData, list.Entries)
	if err != nil {
		return nil, err
	}
	tr.RecordSpan("Read.Merge")

	// Generate and save new snapshot asynchronously (non-blocking)
	if nextSnap := list.NextSnap(); nextSnap != nil {
		// Async snapshot save with SingleFlight (prevents duplicate saves)
		tr.RecordSpan("Read.SnapshotSave_Async", map[string]interface{}{
			"startTsSeq": nextSnap.StartTsSeq,
			"stopTsSeq":  nextSnap.StopTsSeq,
			"size":       len(resultData),
			"hasNext":    nextSnap != nil,
		})
		go func() {
			// Use background context (don't cancel with original context)
			bgCtx := context.Background()
			_, err := c.saveSnapshot(bgCtx, list.catalog, nextSnap.StartTsSeq, nextSnap.StopTsSeq, resultData)
			if err != nil {
				// Snapshot save failure is non-critical, just log
				// Next read will regenerate snapshot
				tr.RecordSpan("Read.SnapshotSave_Async", map[string]interface{}{
					"success": false,
					"error":   err.Error(),
				})
			} else {
				tr.RecordSpan("Read.SnapshotSave_Async", map[string]interface{}{
					"success": true,
				})
			}
		}()
	}

	return resultData, nil
}

// fillDeltasBody fills the Body field for all deltas concurrently
// Uses a worker pool with max 10 concurrent goroutines
// Idempotent: skips deltas that already have Body loaded (len(Body) > 0)
// Returns error immediately if any delta fails to load (no partial success)
func (c *Client) fillDeltasBody(ctx context.Context, catalog string, deltas []index.DeltaInfo) error {
	if len(deltas) == 0 {
		return nil
	}

	// Channel for work distribution
	type job struct {
		index int
		delta *index.DeltaInfo
	}

	jobs := make(chan job, len(deltas))
	done := make(chan error, 1) // Buffered channel for first error

	// Count deltas that need loading
	needLoading := 0
	for i := range deltas {
		if len(deltas[i].Body) == 0 {
			needLoading++
		}
	}

	if needLoading == 0 {
		return nil // All bodies already loaded
	}

	// Worker pool with max 10 concurrent workers
	maxWorkers := 10
	if needLoading < maxWorkers {
		maxWorkers = needLoading
	}

	// Context for early cancellation on error
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				// Check if context cancelled (another worker failed)
				select {
				case <-workerCtx.Done():
					return
				default:
				}

				// Skip if already loaded
				if len(j.delta.Body) > 0 {
					continue
				}

				key := c.storage.MakeDeltaKey(catalog, j.delta.TsSeq, int(j.delta.MergeType))
				namespace := c.storage.RedisPrefix()

				// Use deltaCache (memory cache) for delta files
				data, err := c.deltaCache.Take(workerCtx, namespace, key, func() ([]byte, error) {
					// Cache miss: load from storage
					return c.storage.Get(workerCtx, key)
				})

				if err != nil {
					// Send error and cancel other workers
					select {
					case done <- fmt.Errorf("failed to load delta %d (%s): %w", j.index, j.delta.TsSeq, err):
					default:
					}
					cancel()
					return
				}
				j.delta.Body = data
			}
		}()
	}

	// Send jobs in a separate goroutine
	go func() {
		for i := range deltas {
			select {
			case <-workerCtx.Done():
				return
			case jobs <- job{index: i, delta: &deltas[i]}:
			}
		}
		close(jobs)
	}()

	// Wait for all workers or first error
	go func() {
		wg.Wait()
		close(done)
	}()

	// Return first error or nil
	if err := <-done; err != nil {
		return err
	}

	return nil
}
