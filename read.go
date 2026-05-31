package lake

import (
	"context"
	"fmt"
	"sync"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/merge"
	"github.com/hkloudou/lake/v3/internal/objkey"
	"github.com/hkloudou/lake/v3/storage"
)

// readData loads the snapshot bytes and delta bodies in parallel, merges them
// into the resulting document, and (when a snap target is configured)
// asynchronously persists a new snapshot if there are deltas past the snap.
func (c *Client) readData(ctx context.Context, list *ListResult) ([]byte, error) {
	if list.Err != nil {
		return nil, list.Err
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
		baseData, baseDataErr = c.fetchURI(ctx, storage.Snap, list.catalog, list.LatestSnap.URI)
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

	// Async snapshot save: fire-and-forget on a background context so an aborted
	// Read does not cancel a snapshot that benefits everyone else. Skipped
	// entirely when no snap target is configured.
	if c.snapProvider != "" {
		if next := list.NextSnap(); next != nil {
			go c.saveSnapshot(context.Background(), list.catalog, next.StopTsSeq, resultData)
		}
	}
	return resultData, nil
}

// fetchURI resolves a storage URI (provider://bucket/path) to a backend for the
// given kind and fetches the object. catalog is passed to the backend as context.
func (c *Client) fetchURI(ctx context.Context, kind storage.Kind, catalog, uri string) ([]byte, error) {
	provider, bucket, path, err := objkey.ParseURI(uri)
	if err != nil {
		return nil, err
	}
	st, err := c.storageFor(kind, provider, bucket)
	if err != nil {
		return nil, err
	}
	return st.Get(ctx, catalog, path)
}

// fillDeltasBody loads each delta's Body via the resolved storage, using a
// worker pool capped at 10. Idempotent: skips deltas already loaded. Cancels
// remaining workers on the first failure.
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

	jobs := make(chan *index.DeltaInfo, pending)
	for i := range deltas {
		if len(deltas[i].Body) == 0 {
			jobs <- &deltas[i]
		}
	}
	close(jobs)

	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			for d := range jobs {
				if err := workerCtx.Err(); err != nil {
					return
				}
				data, err := c.fetchURI(workerCtx, storage.Delta, catalog, d.URI)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("load delta %s: %w", d.TsSeq, err):
					default:
					}
					cancel()
					return
				}
				d.Body = data
			}
		})
	}

	wg.Wait()
	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}
