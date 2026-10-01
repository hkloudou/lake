package index

import (
	"context"
	"fmt"
)

// DeleteCatalog removes the catalog's delta log, snap pointer and allocator
// and bumps its removal generation (see deleteCatalogScript). Objects in
// storage are untouched. Returns whether the catalog had any index state.
func (x *Index) DeleteCatalog(ctx context.Context, catalog string) (bool, error) {
	n, err := luaDeleteCatalog.Run(ctx, x.rdb,
		[]string{x.snapsKey(), x.deltaKey(catalog), x.allocKey(catalog)}, catalog,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("delete catalog: %w", err)
	}
	return n == 1, nil
}
