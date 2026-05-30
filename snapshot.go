package lake

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/objkey"
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

// saveSnapshot writes snap bytes to the configured snap target and upserts the
// Redis hash entry (as [tsSeq, uri]). No-op when no snap target is configured.
// SingleFlight on (catalog, stop) dedupes concurrent saves.
func (c *Client) saveSnapshot(ctx context.Context, catalog string, stop index.TimeSeqID, data []byte) (string, error) {
	if c.snapProvider == "" || c.snapBucket == "" {
		return "", nil
	}
	return c.snapFlight.Do(fmt.Sprintf("%s_%s", catalog, stop), func() (string, error) {
		path := objkey.SnapPath(catalog, stop.String())
		st, err := c.storageFor(c.snapProvider, c.snapBucket)
		if err != nil {
			return "", fmt.Errorf("resolve snap target: %w", err)
		}
		if err := st.Put(ctx, catalog, path, data); err != nil {
			return "", fmt.Errorf("save snapshot: %w", err)
		}
		uri := objkey.BuildURI(c.snapProvider, c.snapBucket, path)
		if err := c.writer.AddSnap(ctx, catalog, stop, uri); err != nil {
			return "", fmt.Errorf("index snapshot: %w", err)
		}
		return uri, nil
	})
}
