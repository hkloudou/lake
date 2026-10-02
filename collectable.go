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
// when you want one, is yours; this is only the signal that there is work.
// The README ("Lake never deletes") lists what a correct sweep must handle.
func (c *Client) Collectable(ctx context.Context, catalog string) (int64, error) {
	if err := utils.ValidateCatalog(catalog); err != nil {
		return 0, err
	}
	return c.idx.Absorbed(ctx, catalog)
}
