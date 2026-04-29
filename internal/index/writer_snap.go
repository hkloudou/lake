package index

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Writer handles writing to Redis ZADD index
type Writer struct {
	rdb *redis.Client
	indexIO
	// timeGen *TimeGenerator
}

// NewWriter creates a new index writer
func NewWriter(rdb *redis.Client) *Writer {
	return &Writer{
		rdb:     rdb,
		indexIO: indexIO{prefix: "lake"}, // Will be set later via SetPrefix
		// timeGen:  NewTimeGenerator(rdb),
	}
}

// AddSnap upserts the catalog's latest snapshot entry into the
// deployment-wide "<prefix>:snaps" Redis Hash.
//
//   - field = catalog name
//   - value = EncodeSnapValue(start, stop) i.e. "{start}|{stop}"
//
// V3 keeps only one snap per catalog (snap is idempotent and self-
// correcting, so historical snaps add no read-path value). HSet
// overwrites the previous entry; the previous OSS snap object is left
// orphan and reaped by future cleanup tooling — see Client.AllSnaps and
// the discussion in clear_optimized.go.
//
//   - startTsSeq: start of covered range; "0_0" for the first snap
//   - stopTsSeq:  stop of covered range
func (w *Writer) AddSnap(ctx context.Context, catalog string, startTsSeq, stopTsSeq TimeSeqID) error {
	return w.rdb.HSet(ctx, w.MakeSnapsHashKey(), catalog, EncodeSnapValue(startTsSeq, stopTsSeq)).Err()
}
