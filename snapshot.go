package lake

import (
	"context"
	"fmt"
	"time"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/objkey"
	"github.com/hkloudou/lake/v3/storage"
)

// snapSaveTimeout bounds an async save so a stalled backend cannot pin the
// goroutine, its document buffer and the catalog's save slot forever.
const snapSaveTimeout = 5 * time.Minute

// IterateSnaps streams every catalog's snap to fn via HSCAN; stops when fn
// returns false. Each snap.URI is a complete object locator, so backup
// tooling can copy snapshots straight to an archive.
func (c *Client) IterateSnaps(ctx context.Context, fn func(catalog string, snap SnapInfo) bool) error {
	return c.idx.IterateSnaps(ctx, fn)
}

// saveSnapshotAsync is saveSnapshot for the read path's detached goroutine:
// bounded by snapSaveTimeout, and a panic in a storage backend or event
// handler is reported instead of killing the process (there is no caller
// stack to recover on).
func (c *Client) saveSnapshotAsync(catalog string, stop index.TimeSeqID, removeGen string, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			c.emitEvent(catalog, "SnapshotError", map[string]any{"stop": stop.String(), "err": fmt.Sprintf("panic: %v", r)})
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), snapSaveTimeout)
	defer cancel()
	_, _ = c.saveSnapshot(ctx, catalog, stop, removeGen, data)
}

// saveSnapshot uploads the snap object and publishes its pointer — only
// monotonically, and only if removeGen still matches the catalog's removal
// generation (AddSnap drops the upsert if a newer snap landed or a RemoveDelta
// interleaved since the read that produced data). A failure is user-invisible
// by design (the next read regenerates); a "SnapshotError" event reports it.
func (c *Client) saveSnapshot(ctx context.Context, catalog string, stop index.TimeSeqID, removeGen string, data []byte) (uri string, err error) {
	if c.snapProvider == "" {
		return "", nil
	}
	defer func() {
		if err != nil {
			c.emitEvent(catalog, "SnapshotError", map[string]any{"stop": stop.String(), "err": err.Error()})
		}
	}()
	// Unique per (stop, generation): removing a non-latest delta leaves the
	// stop unchanged, and if both generations shared one object path a stale
	// Put could finish last and overwrite the bytes the published pointer
	// references. Gen 0 keeps the plain name.
	name := stop.String()
	if removeGen != "" && removeGen != "0" {
		name += "-g" + removeGen
	}
	path := objkey.SnapPath(catalog, name)
	st, err := c.storageFor(storage.Snap, c.snapProvider, c.snapBucket)
	if err != nil {
		return "", fmt.Errorf("resolve snap target: %w", err)
	}
	if err := st.Put(ctx, catalog, path, data); err != nil {
		return "", fmt.Errorf("save snapshot: %w", err)
	}
	uri = objkey.BuildURI(c.snapProvider, c.snapBucket, path)
	if err := c.idx.AddSnap(ctx, catalog, stop, uri, removeGen); err != nil {
		return "", fmt.Errorf("index snapshot: %w", err)
	}
	return uri, nil
}
