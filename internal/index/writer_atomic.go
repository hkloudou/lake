package index

import (
	"context"
	"fmt"
	"time"
)

// notifyDedupTTL bounds how long a Notify stays idempotent for its uri. It
// covers every realistic retry loop (and, with handle signing, the whole
// window in which a handle is accepted at all); a later repeat is a new write.
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
