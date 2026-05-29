package lake

import (
	"context"
	"fmt"
	"sync"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/merge"
)

// readData loads the snapshot bytes and delta bodies in parallel, merges
// them into the resulting document, and asynchronously persists a new
// snapshot if there are deltas past the latest snap.
func (c *Client) readData(ctx context.Context, list *ListResult) ([]byte, error) {
	if list.Err != nil {
		return nil, list.Err
	}
	if err := c.ensureInitialized(ctx); err != nil {
		return nil, err
	}

	var (
		baseData              []byte
		baseDataErr, deltaErr error
		wg                    sync.WaitGroup
	)
	wg.Go(func() {
		if list.LatestSnap == nil {
			baseData = []byte("{}")
			return
		}
		key := c.storage.MakeSnapKey(list.catalog, list.LatestSnap.StopTsSeq)
		baseData, baseDataErr = c.snapCache.Take(ctx, c.storage.RedisPrefix(), key, func() ([]byte, error) {
			return c.storage.Get(ctx, key)
		})
	})
	wg.Go(func() {
		deltaErr = c.fillDeltasBody(ctx, list.catalog, list.Entries)
	})
	wg.Wait()

	if baseDataErr != nil {
		return nil, fmt.Errorf("load snapshot: %w", baseDataErr)
	}
	if deltaErr != nil {
		return nil, fmt.Errorf("load deltas: %w", deltaErr)
	}

	resultData, err := merge.Merge(baseData, list.Entries)
	if err != nil {
		return nil, fmt.Errorf("merge catalog %s: %w", list.catalog, err)
	}

	// Async snapshot save: fire-and-forget on a background context so an
	// aborted Read does not cancel a snapshot that benefits everyone else.
	// An interrupted save leaves at most one orphan OSS object (reaped by
	// the next sweep); the next read regenerates the snap, so reads remain
	// correct. There is no drain on shutdown — a Client is process-lived.
	if next := list.NextSnap(); next != nil {
		go c.saveSnapshot(context.Background(), list.catalog, next.StopTsSeq, resultData)
	}
	return resultData, nil
}

// fillDeltasBody loads each delta's Body via deltaCache + storage,
// using a worker pool capped at 10. Idempotent: skips deltas already
// loaded. Cancels remaining workers on the first failure.
func (c *Client) fillDeltasBody(ctx context.Context, catalog string, deltas []index.DeltaInfo) error {
	pending := 0
	for i := range deltas {
		if len(deltas[i].Body) == 0 {
			pending++
		}
	}
	if pending == 0 {
		return nil
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := 10
	if pending < workers {
		workers = pending
	}

	jobs := make(chan *index.DeltaInfo, len(deltas))
	done := make(chan error, 1)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			for d := range jobs {
				if workerCtx.Err() != nil {
					return
				}
				if len(d.Body) > 0 {
					continue
				}
				key := c.storage.MakeDeltaKey(catalog, d.UUID)
				data, err := c.deltaCache.Take(workerCtx, c.storage.RedisPrefix(), key, func() ([]byte, error) {
					return c.storage.Get(workerCtx, key)
				})
				if err != nil {
					select {
					case done <- fmt.Errorf("load delta %s: %w", d.TsSeq, err):
					default:
					}
					cancel()
					return
				}
				d.Body = data
			}
		})
	}

	go func() {
		for i := range deltas {
			select {
			case <-workerCtx.Done():
				return
			case jobs <- &deltas[i]:
			}
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(done)
	}()

	if err := <-done; err != nil {
		return err
	}
	return nil
}
