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
//   - value = EncodeSnapValue(stopTsSeq) i.e. "{stopTsSeq}"
//
// V3 keeps only one snap per catalog (snap is idempotent and self-
// correcting, so historical snaps add no read-path value). HSet
// overwrites the previous entry; the previous OSS snap object is left
// orphan and reaped by future cleanup tooling — see Client.AllSnaps and
// the discussion in clear_optimized.go.
//
// Only stopTsSeq is recorded — the snap's "start" had no read-path
// meaning (deltas are filtered by score > snap.stop) and storing it
// doubled both the Redis value size and OSS filename length for no
// benefit.
func (w *Writer) AddSnap(ctx context.Context, catalog string, stopTsSeq TimeSeqID) error {
	return w.rdb.HSet(ctx, w.MakeSnapsHashKey(), catalog, EncodeSnapValue(stopTsSeq)).Err()
}
