package lake

import (
	"context"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// ClearHistory removes all delta entries (and their storage objects) at
// or before the catalog's latest snap. The previous OSS snap object
// from each save is left orphan in storage (V3 contract).
//
// SingleFlight de-duplicates concurrent clears on the same catalog.
func (c *Client) ClearHistory(ctx context.Context, catalog string) error {
	if err := utils.ValidateCatalog(catalog); err != nil {
		return err
	}
	_, err := c.clearFlight.Do(catalog, func() (struct{}, error) {
		return struct{}{}, c.doClearHistoryOptimized(ctx, catalog)
	})
	return err
}
