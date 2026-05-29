package lake

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/index"
)

// IterateSnaps streams every catalog's snap to fn via HSCAN — the single
// primitive for enumerating snap metadata (e.g. for backup tooling that
// feeds each (catalog, snap.StopTsSeq) into Storage.MakeSnapKey to locate
// every OSS snap object without an OSS LIST). Stops early when fn returns
// false; honours ctx cancellation. No Redis op blocks the server's main
// thread for more than a few hundred fields, so it scales to large fleets
// without materialising the full set in memory. See
// (*index.Reader).IterateSnaps for the concurrent-modification semantics
// inherited from HSCAN. Callers that want the whole set in a map can
// accumulate one inside fn.
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
