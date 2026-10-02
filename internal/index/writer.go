package index

import (
	"context"
	"fmt"
	"time"
)

// notifyDedupTTL bounds how long a Notify stays idempotent for its uri. It
// covers every realistic retry loop; a later repeat is a new write.
const notifyDedupTTL = time.Hour

// Notify allocates a TimeSeqID for an already-uploaded delta and commits it
// (see notifyScript). A repeat call with the same uri within notifyDedupTTL
// returns the entry the first call committed.
func (x *Index) Notify(ctx context.Context, catalog, fieldPath string, mergeType MergeType, uri string) (TimeSeqID, error) {
	res, err := luaNotify.Run(ctx, x.rdb,
		[]string{x.deltaKey(catalog), x.snapsKey(), x.allocKey(catalog), x.dedupKey(uri)},
		fieldPath, int(mergeType), uri, catalog, int64(notifyDedupTTL/time.Second),
	).Slice()
	if err != nil {
		return TimeSeqID{}, fmt.Errorf("notify: %w", err)
	}
	if len(res) != 2 {
		return TimeSeqID{}, fmt.Errorf("notify: unexpected reply %v", res)
	}
	ts, ok1 := res[0].(int64)
	seq, ok2 := res[1].(int64)
	if !ok1 || !ok2 {
		return TimeSeqID{}, fmt.Errorf("notify: unexpected reply %v", res)
	}
	return TimeSeqID{Timestamp: ts, SeqID: seq}, nil
}

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
