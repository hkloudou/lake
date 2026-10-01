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

// readData loads the snapshot and the delta bodies in parallel, merges them,
// and — with a snap target configured and enough new deltas — persists a new
// snapshot asynchronously.
func (c *Client) readData(ctx context.Context, list *ListResult) ([]byte, error) {
	c.emitEvent(list.catalog, "Read", nil)
	if list.Err != nil {
		return nil, list.Err
	}

	// Entries a later Replace fully overwrites can never affect the document:
	// skip their fetch (and let a poison body among them do no harm). Bodies
	// fetched into the pruned copy are written back below so they memoise on
	// the ListResult.
	entries, aliveIdx := merge.PruneDead(list.Entries)

	var (
		base    = []byte("{}")
		baseErr error
		wg      sync.WaitGroup
	)
	if list.LatestSnap != nil {
		wg.Go(func() { base, baseErr = c.fetchURI(ctx, storage.Snap, list.catalog, list.LatestSnap.URI) })
	}
	deltaErr := c.fillBodies(ctx, list.catalog, entries)
	wg.Wait()
	if baseErr != nil {
		return nil, fmt.Errorf("load snapshot: %w", baseErr)
	}
	if deltaErr != nil {
		return nil, fmt.Errorf("load deltas: %w", deltaErr)
	}
	for k, i := range aliveIdx {
		if len(list.Entries[i].Body) == 0 {
			list.Entries[i].Body = entries[k].Body
		}
	}

	result, err := merge.Merge(base, entries)
	if err != nil {
		return nil, fmt.Errorf("merge catalog %s: %w", list.catalog, err)
	}

	// Fire-and-forget on a detached context (an aborted Read must not cancel
	// a snapshot that benefits every later reader), at most one save per
	// catalog in flight. The goroutine gets a private copy: the caller may
	// mutate result while the save is still reading.
	if c.snapProvider != "" && len(list.Entries) >= c.snapMinDeltas {
		if _, busy := c.snapSaving.LoadOrStore(list.catalog, struct{}{}); !busy {
			stop, gen, data := list.Entries[len(list.Entries)-1].TsSeq, list.removeGen, append([]byte(nil), result...)
			go func() {
				defer c.snapSaving.Delete(list.catalog)
				c.saveSnapshotAsync(list.catalog, stop, gen, data)
			}()
		}
	}
	return result, nil
}

// fillBodies loads every delta Body not yet loaded, up to 10 at a time,
// stopping at the first failure.
func (c *Client) fillBodies(ctx context.Context, catalog string, deltas []index.DeltaInfo) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg    sync.WaitGroup
		sem   = make(chan struct{}, 10)
		mu    sync.Mutex
		first error
	)
	for i := range deltas {
		d := &deltas[i]
		if len(d.Body) > 0 {
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break
		}
		if ctx.Err() != nil {
			break
		}
		wg.Go(func() {
			defer func() { <-sem }()
			if err := c.fetchDelta(ctx, catalog, d); err != nil {
				mu.Lock()
				if first == nil {
					first = err
					cancel()
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if first == nil {
		return ctx.Err()
	}
	return first
}

// fetchDelta loads one body. A 0-byte object is an error in its own words:
// the client uploaded nothing, and reads would otherwise refetch it forever.
func (c *Client) fetchDelta(ctx context.Context, catalog string, d *index.DeltaInfo) error {
	data, err := c.fetchURI(ctx, storage.Delta, catalog, d.URI)
	if err != nil {
		return fmt.Errorf("load delta %s: %w", d.TsSeq, err)
	}
	if len(data) == 0 {
		return fmt.Errorf("delta %s: object at %s is empty (unblock with RemoveDelta %q)", d.TsSeq, d.URI, d.TsSeq.String())
	}
	d.Body = data
	return nil
}

// fetchURI resolves provider://bucket/path through the Resolver and fetches it.
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
