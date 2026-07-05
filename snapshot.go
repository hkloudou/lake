package lake

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/objkey"
	"github.com/hkloudou/lake/v3/storage"
)

// IterateSnaps streams every catalog's snap to fn via HSCAN — the single
// primitive for enumerating snap metadata (e.g. for backup tooling that feeds
// each snap.URI to its archive copy). Stops early when fn returns false;
// honours ctx cancellation. No Redis op blocks the server's main thread for
// more than a few hundred fields, so it scales to large fleets without
// materialising the full set in memory. Callers that want the whole set in a
// map can accumulate one inside fn.
func (c *Client) IterateSnaps(ctx context.Context, fn func(catalog string, snap SnapInfo) bool) error {
	return c.reader.IterateSnaps(ctx, fn)
}

// saveSnapshotGuarded is the fire-and-forget form of saveSnapshot for the
// read path's async goroutine. That goroutine outlives the read and has no
// caller to recover a panic — from a storage backend, or a user event
// handler fired on the failure path — so an escaped panic would kill the
// whole process to save an optimization. Contained, not silent: a panic
// still emits SnapshotError (saveSnapshot's own error emit cannot fire
// during unwinding — its named err is nil then), and the emit itself is
// guarded again in case the panicking party IS a handler.
func (c *Client) saveSnapshotGuarded(catalog string, stop index.TimeSeqID, removeGen string, data []byte) {
	defer func() {
		if r := recover(); r != nil {
			defer func() { _ = recover() }() // a panicking handler must not escape either
			c.emitEvent(catalog, "SnapshotError", map[string]any{
				"stop": stop.String(), "err": fmt.Sprintf("panic: %v", r),
			})
		}
	}()
	// Background context: an aborted Read must not cancel a snapshot that
	// benefits every future reader.
	_, _ = c.saveSnapshot(context.Background(), catalog, stop, removeGen, data)
}

// saveSnapshot writes snap bytes to the configured snap target and upserts the
// Redis hash entry (as [tsSeq, uri]) — monotonically, and only if removeGen
// still matches the catalog's removal generation: AddSnap drops the upsert if
// a newer snap already landed OR a RemoveDelta interleaved since the read
// that produced data (which would otherwise resurrect the removed write).
// No-op when no snap target is configured. SingleFlight on (catalog, stop,
// gen) dedupes concurrent saves within this process.
//
// The read path calls this fire-and-forget, so a failure is user-invisible by
// design (the next read just regenerates); a "SnapshotError" event is emitted
// per failed attempt so operators still see a snap target that never works.
func (c *Client) saveSnapshot(ctx context.Context, catalog string, stop index.TimeSeqID, removeGen string, data []byte) (string, error) {
	if c.snapProvider == "" || c.snapBucket == "" {
		return "", nil
	}
	return c.snapFlight.Do(fmt.Sprintf("%s_%s_%s", catalog, stop, removeGen), func() (uri string, err error) {
		defer func() {
			if err != nil {
				c.emitEvent(catalog, "SnapshotError", map[string]any{"stop": stop.String(), "err": err.Error()})
			}
		}()
		// The object path must be unique per (stop, removal generation), not
		// just per stop: removing a non-latest delta leaves the stop
		// unchanged, and if both generations shared one path, the stale
		// generation's Put could finish LAST and overwrite the bytes the
		// already-published pointer references — resurrecting the removed
		// write behind AddSnap's back. Same stop + same generation implies
		// identical content, so sharing within a generation stays benign.
		// Readers fetch the URI recorded in the pointer verbatim, so the
		// name shape is free to vary; gen 0 keeps the legacy name.
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
		if err := c.writer.AddSnap(ctx, catalog, stop, uri, removeGen); err != nil {
			return "", fmt.Errorf("index snapshot: %w", err)
		}
		return uri, nil
	})
}
