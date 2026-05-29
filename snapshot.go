package lake

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/index"
)

// AllSnaps collects every catalog's snap metadata into a map (HSCAN under
// the hood; no Redis op blocks the server's main thread for more than a
// few hundred fields at a time). Backup tooling can feed each (catalog,
// info.StopTsSeq) into Storage.MakeSnapKey to enumerate every OSS snap
// key without a LIST.
//
// For very large fleets prefer IterateSnaps so the full map is never
// materialised in memory.
func (c *Client) AllSnaps(ctx context.Context) (map[string]SnapInfo, error) {
	if err := c.ensureInitialized(ctx); err != nil {
		return nil, err
	}
	return c.reader.AllSnaps(ctx)
}

// IterateSnaps streams every catalog's snap to fn — the scalable form of
// AllSnaps. Stops early when fn returns false; honours ctx cancellation.
// See (*index.Reader).IterateSnaps for the concurrent-modification
// semantics inherited from HSCAN.
func (c *Client) IterateSnaps(ctx context.Context, fn func(catalog string, snap SnapInfo) bool) error {
	if err := c.ensureInitialized(ctx); err != nil {
		return err
	}
	return c.reader.IterateSnaps(ctx, fn)
}

// saveSnapshot writes snap bytes to storage and upserts the Redis hash
// entry. Previous OSS snap object is left orphan (V3 contract).
// SingleFlight on (catalog, stop) dedupes concurrent saves.
func (c *Client) saveSnapshot(ctx context.Context, catalog string, stop index.TimeSeqID, data []byte) (string, error) {
	return c.snapFlight.Do(fmt.Sprintf("%s_%s", catalog, stop), func() (string, error) {
		key := c.storage.MakeSnapKey(catalog, stop)
		if err := c.storage.Put(ctx, key, data); err != nil {
			return "", fmt.Errorf("save snapshot: %w", err)
		}
		if err := c.writer.AddSnap(ctx, catalog, stop); err != nil {
			return "", fmt.Errorf("index snapshot: %w", err)
		}
		return key, nil
	})
}
