package index

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Writer handles writing to Redis ZADD index
type Writer struct {
	rdb     *redis.Client
	prefix  string
	timeGen *TimeGenerator
}

// NewWriter creates a new index writer
func NewWriter(rdb *redis.Client) *Writer {
	return &Writer{
		rdb:     rdb,
		prefix:  "lake", // Will be set later via SetPrefix
		timeGen: NewTimeGenerator(rdb),
	}
}

// SetPrefix sets the key prefix (e.g., "oss:my-lake")
func (w *Writer) SetPrefix(prefix string) {
	w.prefix = prefix
}

func (w *Writer) GetTimeSeqID(ctx context.Context, catalog string) (TimeSeqID, error) {
	return w.timeGen.Generate(ctx, catalog)
}

// AddWithTimeSeq adds an entry to the catalog index with auto-generated time+seqid
// Returns the generated TimeSeqID
func (w *Writer) AddWithTimeSeq(ctx context.Context, tsSeq TimeSeqID, catalog, field string, mergeType MergeType) error {
	// Generate timestamp + seqid from Redis

	key := w.makeCatalogKey(catalog)
	member := EncodeMember(field, tsSeq.String(), mergeType)

	return w.rdb.ZAdd(ctx, key, redis.Z{
		Score:  tsSeq.Score(),
		Member: member,
	}).Err()
}

// Add adds an entry to the catalog index (legacy - for backward compatibility)
// DEPRECATED: Use AddWithTimeSeq instead
func (w *Writer) Add(ctx context.Context, catalog, field, uuid string, timestamp int64, mergeType MergeType) error {
	key := w.makeCatalogKey(catalog)
	member := EncodeMember(field, uuid, mergeType)

	return w.rdb.ZAdd(ctx, key, redis.Z{
		Score:  float64(timestamp),
		Member: member,
	}).Err()
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

// BatchAddWithTimeSeq adds multiple entries with auto-generated time+seqid
// Returns the generated TimeSeqIDs in order
// func (w *Writer) BatchAddWithTimeSeq(ctx context.Context, catalog string, entries []BatchEntry) ([]TimeSeqID, error) {
// 	if len(entries) == 0 {
// 		return nil, nil
// 	}

// 	pipe := w.rdb.Pipeline()
// 	key := w.makeCatalogKey(catalog)
// 	timeSeqs := make([]TimeSeqID, len(entries))

// 	for i, e := range entries {
// 		// Generate time+seqid for each entry
// 		tsSeq, err := w.timeGen.Generate(ctx, catalog)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to generate time+seqid for entry %d: %w", i, err)
// 		}
// 		timeSeqs[i] = tsSeq

// 		member := EncodeMember(e.Field, tsSeq.String(), e.MergeType)
// 		pipe.ZAdd(ctx, key, redis.Z{
// 			Score:  tsSeq.Score(),
// 			Member: member,
// 		})
// 	}

// 	_, err := pipe.Exec(ctx)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return timeSeqs, nil
// }

// BatchEntry represents a batch entry for writing
// type BatchEntry struct {
// 	Field     string
// 	MergeType MergeType
// }

// Entry represents an index entry (legacy)
type Entry struct {
	Field     string
	UUID      string
	Timestamp int64
	MergeType MergeType
}

// BatchAdd adds multiple entries in a pipeline (legacy - for backward compatibility)
// DEPRECATED: Use BatchAddWithTimeSeq instead
// func (w *Writer) BatchAdd(ctx context.Context, catalog string, entries []Entry) error {
// 	if len(entries) == 0 {
// 		return nil
// 	}

// 	pipe := w.rdb.Pipeline()
// 	key := w.makeCatalogKey(catalog)

// 	for _, e := range entries {
// 		member := EncodeMember(e.Field, e.UUID, e.MergeType)
// 		pipe.ZAdd(ctx, key, redis.Z{
// 			Score:  float64(e.Timestamp),
// 			Member: member,
// 		})
// 	}

// 	_, err := pipe.Exec(ctx)
// 	return err
// }

// makeCatalogKey generates the Redis key for catalog data index
// Kept in sync with Reader.makeCatalogKey in keys.go
func (w *Writer) makeCatalogKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:data:%s", w.prefix, catalog)
}

// makeSnapKey generates the Redis key for catalog snapshot index
// Kept in sync with Reader.makeSnapKey in keys.go
func (w *Writer) makeSnapKey(catalog string) string {
	if w.prefix == "" {
		panic("prefix is not set")
	}
	return fmt.Sprintf("%s:snap:%s", w.prefix, catalog)
}
