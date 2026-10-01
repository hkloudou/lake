package lake

import (
	"context"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// Compact removes the index entries of every delta the catalog's current
// snapshot has absorbed and returns how many it removed. Redis only: the
// delta objects in storage stay (bucket lifecycle rules own deletion).
//
// Safe at any time from any process: reads observe the snap pointer and the
// deltas after it atomically, and the pointer is monotonic, so compaction
// can never remove a delta a concurrent read still needs. A catalog with no
// snapshot is left intact. There is no background reaper — sweep on your own
// schedule, e.g. via IterateSnaps.
func (c *Client) Compact(ctx context.Context, catalog string) (int64, error) {
	c.emitEvent(catalog, "Compact", nil)
	if err := utils.ValidateCatalog(catalog); err != nil {
		return 0, err
	}
	return c.idx.Compact(ctx, catalog)
}
