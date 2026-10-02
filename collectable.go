package lake

import (
	"context"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// Collectable reports how many delta entries the catalog's current snapshot
// has absorbed — everything at or before the snap stop, which no read fetches
// any more. 0 means nothing to collect (or no snapshot yet).
//
// Lake itself never deletes: not these index entries, not the delta objects
// they name, not superseded snapshot objects. Left alone, the index and the
// bucket simply grow with history, which is correct — just not free. A sweep,
// when you want one, is yours, and the rule is one comparison: every delta
// entry and every snapshot object sorting before the live snap stop is dead.
//
//   - Index: ZREMRANGEBYSCORE <prefix>:d:<catalog> -inf <snap stop score>.
//     Safe at any time — reads observe the pointer and the log atomically.
//   - Objects: delete them only once the snapshot has been published for a
//     few minutes (five is generous; use the snap object's Last-Modified). A
//     read that listed just before the pointer moved may still be fetching
//     the bodies it absorbed.
func (c *Client) Collectable(ctx context.Context, catalog string) (int64, error) {
	if err := utils.ValidateCatalog(catalog); err != nil {
		return 0, err
	}
	return c.idx.Absorbed(ctx, catalog)
}
