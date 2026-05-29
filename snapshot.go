package lake

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/index"
)

// AllSnaps returns the latest snap metadata for every catalog in this
// deployment in a single Redis HGETALL on "<prefix>:s". Backup
// tooling can feed each (catalog, info.StopTsSeq) into
// Storage.MakeSnapKey to enumerate every OSS snap key without a LIST.
func (c *Client) AllSnaps(ctx context.Context) (map[string]SnapInfo, error) {
	if err := c.ensureInitialized(ctx); err != nil {
		return nil, err
	}
	return c.reader.AllSnaps(ctx)
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
