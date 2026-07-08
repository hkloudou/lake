package lake

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/merge"
	"github.com/hkloudou/lake/v3/internal/objkey"
	"github.com/hkloudou/lake/v3/storage"
)

// snapSaveTimeout bounds an async snapshot save (storage Put + AddSnap): a
// stalled backend must not pin the save goroutine, its full-document buffer,
// and the per-catalog save slot forever.
const snapSaveTimeout = 5 * time.Minute

// readData loads the snapshot bytes and delta bodies in parallel, merges them
// into the resulting document, and (when a snap target is configured)
// asynchronously persists a new snapshot if there are deltas past the snap.
func (c *Client) readData(ctx context.Context, list *ListResult) ([]byte, error) {
	if list.Err != nil {
		return nil, list.Err
	}

	// Entries that a LATER Replace fully overwrites can never affect the
	// merged document — drop them before fetching, so their bodies are never
	// loaded and a poison body among them cannot wedge the read. When nothing
	// is dead this returns list.Entries itself; when it prunes, survivors'
	// fetched bodies are copied back below — either way bodies memoise on the
	// ListResult for reuse.
	entries, aliveIdx := merge.PruneDead(list.Entries)

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
		deltaErr = c.fillDeltasBody(ctx, list.catalog, entries)
	})
	wg.Wait()

	if baseDataErr != nil {
		return nil, fmt.Errorf("load snapshot: %w", baseDataErr)
	}
	if deltaErr != nil {
		return nil, fmt.Errorf("load deltas: %w", deltaErr)
	}
	// Copy freshly fetched bodies back so they memoise on the ListResult —
	// but never overwrite one that is already loaded: fully-memoised re-reads
	// must stay write-free (concurrent readers of one ListResult would
	// otherwise race on the Body headers).
	for k, i := range aliveIdx {
		if len(list.Entries[i].Body) == 0 {
			list.Entries[i].Body = entries[k].Body
		}
	}

	resultData, err := merge.Merge(baseData, entries)
	if err != nil {
		return nil, fmt.Errorf("merge catalog %s: %w", list.catalog, err)
	}

	// Async snapshot save: fire-and-forget on a detached context so an aborted
	// Read does not cancel a snapshot that benefits everyone else. Skipped
	// entirely when no snap target is configured. At most ONE save per catalog
	// is in flight at a time (snapSaving): under a read storm — or a hot-write
	// catalog whose stop advances every read — the extra saves would all be
	// either duplicates or immediately superseded, yet each would copy the
	// full document and upload it. Reads that arrive while the slot is held
	// are simply skipped; the next read after the slot frees starts the next
	// save, so the snap pointer converges as long as the catalog is read (the
	// steady-state cost of the lag is one longer replay on that next read).
	// The save context's timeout is what frees a slot wedged on a slow
	// backend; a Put that ignores ctx entirely parks snapshotting for the
	// catalog — deliberately not defended against beyond the timeout.
	// The goroutine gets a private copy of resultData: the caller is free to
	// mutate its slice while the save is still reading — and a mutated
	// snapshot would poison every later read of the catalog.
	if c.snapProvider != "" {
		if next := list.NextSnap(); next != nil {
			if _, busy := c.snapSaving.LoadOrStore(list.catalog, struct{}{}); !busy {
				snapData := append([]byte(nil), resultData...)
				go func() {
					defer c.snapSaving.Delete(list.catalog)
					saveCtx, cancel := context.WithTimeout(context.Background(), snapSaveTimeout)
					defer cancel()
					if _, err := c.saveSnapshot(saveCtx, list.catalog, next.StopTsSeq, list.removeGen, snapData); err != nil {
						c.emitEvent(list.catalog, "SnapshotSaveError", map[string]any{"err": err.Error()})
					}
				}()
			}
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

// fetchDeltaBody loads one delta's body into d. A 0-byte object is an error
// in its own words: it is NOT "missing body data" (the object exists — the
// client uploaded nothing), and treating it as unloaded would refetch it on
// every read of a reused ListResult.
func (c *Client) fetchDeltaBody(ctx context.Context, catalog string, d *index.DeltaInfo) error {
	data, err := c.fetchURI(ctx, storage.Delta, catalog, d.URI)
	if err != nil {
		return fmt.Errorf("load delta %s: %w", d.TsSeq, err)
	}
	if len(data) == 0 {
		return fmt.Errorf("delta %s: object at %s is empty (0 bytes — likely an empty client upload; unblock with RemoveDelta %q)",
			d.TsSeq, d.URI, d.TsSeq.String())
	}
	d.Body = data
	return nil
}

// fillDeltasBody loads each delta's Body via the resolved storage. Idempotent:
// skips deltas already loaded. The common steady state with snapshotting on is
// 0–1 new deltas per read, so those cases run inline on the calling goroutine;
// larger backlogs use a worker pool capped at 10 that cancels on first failure.
func (c *Client) fillDeltasBody(ctx context.Context, catalog string, deltas []index.DeltaInfo) error {
	pending := 0
	last := -1
	for i := range deltas {
		if len(deltas[i].Body) == 0 {
			pending++
			last = i
		}
	}
	switch pending {
	case 0:
		return nil
	case 1:
		if err := ctx.Err(); err != nil {
			return err
		}
		return c.fetchDeltaBody(ctx, catalog, &deltas[last])
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
				if err := c.fetchDeltaBody(workerCtx, catalog, d); err != nil {
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
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
