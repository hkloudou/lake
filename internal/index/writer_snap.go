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

// AddSnap adds a snapshot entry to the catalog snapshot index
// startTsSeq: start time sequence (e.g., "1700000000_1" or "0_0" for first snap)
// stopTsSeq: stop time sequence (e.g., "1700000100_500")
// score: the score for Redis ZADD (typically parsed from stopTsSeq)
func (w *Writer) AddSnap(ctx context.Context, catalog string, startTsSeq, stopTsSeq TimeSeqID) error {
	key := w.makeSnapKey(catalog)
	member := EncodeSnapMember(startTsSeq, stopTsSeq)

	return w.rdb.ZAdd(ctx, key, redis.Z{
		Score:  stopTsSeq.Score(),
		Member: member,
	}).Err()
}
