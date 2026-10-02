package index

import "context"

// AddSnap publishes the catalog's snap pointer as [tsSeq, uri] — only
// monotonically, and only when removeGen still matches the catalog's removal
// generation (see addSnapScript). Refusals are silent no-ops; the freshly
// written snap object is left orphan in storage like any superseded snap.
func (x *Index) AddSnap(ctx context.Context, catalog string, stop TimeSeqID, uri, removeGen string) error {
	val, err := EncodeSnapValue(stop, uri)
	if err != nil {
		return err
	}
	if removeGen == "" {
		removeGen = "0"
	}
	return luaAddSnap.Run(ctx, x.rdb, []string{x.snapsKey()}, catalog, val, stop.Score(), removeGen).Err()
}
