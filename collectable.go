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
//     Safe against concurrent reads and writes (reads observe the pointer
//     and the log atomically; the pointer only moves forward). Not against a
//     concurrent DeleteCatalog of the same catalog, which resets its
//     sequence — serialise the two; both are yours.
//   - Objects: delete them only after the pointer has been live for longer
//     than your longest read, measured from when you first observed it (a
//     read that listed the old pointer may still be fetching what it
//     absorbed). Compare snapshot names by ParseTimeSeqID(...).Score(), not
//     lexically.
func (c *Client) Collectable(ctx context.Context, catalog string) (int64, error) {
	if err := utils.ValidateCatalog(catalog); err != nil {
		return 0, err
	}
	return c.idx.Absorbed(ctx, catalog)
}
