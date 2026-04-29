package lake

import (
	"context"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// ClearHistory removes all delta entries (and their OSS objects) at or
// before the catalog's latest snap, releasing storage that the snap has
// already absorbed.
//
// V3 keeps only one snap per catalog (overwritten on each save), so
// there is no historical-snap retention concept; ClearHistory is the
// single cleanup entry point. The previous OSS snap objects from
// overwrites are left in place — see clear_optimized.go's design note.
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
