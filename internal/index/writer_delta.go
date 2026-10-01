package index

import (
	"context"
	"fmt"
)

// RemoveDelta deletes the delta entry with the given tsSeq and bumps the
// catalog's removal generation (see removeDeltaScript). The body object in
// storage is untouched. Returns whether an entry was removed.
func (x *Index) RemoveDelta(ctx context.Context, catalog string, tsSeq TimeSeqID) (bool, error) {
	n, err := luaRemoveDelta.Run(ctx, x.rdb,
		[]string{x.deltaKey(catalog), x.snapsKey()},
		tsSeq.Score(), tsSeq.String(), catalog,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("remove delta: %w", err)
	}
	return n == 1, nil
}
