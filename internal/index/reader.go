package index

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Reader handles reading from Redis ZADD index
type Reader struct {
	rdb    *redis.Client
	prefix string
}

// NewReader creates a new index reader
func NewReader(rdb *redis.Client) *Reader {
	return &Reader{
		rdb:    rdb,
		prefix: "", // Will be set later via SetPrefix
	}
}

// SetPrefix sets the key prefix (e.g., "oss:my-lake")
func (r *Reader) SetPrefix(prefix string) {
	r.prefix = prefix
}

// ReadResult represents a read result from the index
type ReadResult struct {
	Field     string
	TsSeq     TimeSeqID // Format: "ts_seqid"
	MergeType MergeType
	Timestamp int64 // Unix timestamp (from score)
}

// ReadAll reads all entries from the catalog
func (r *Reader) ReadAll(ctx context.Context, catalog string) ([]ReadResult, error) {
	key := r.makeCatalogKey(catalog)
	return r.readRange(ctx, key, "-inf", "+inf")
}

// ReadSince reads entries since the given timestamp (exclusive)
func (r *Reader) ReadSince(ctx context.Context, catalog string, sinceTimestamp int64) ([]ReadResult, error) {
	key := r.makeCatalogKey(catalog)
	// Use '(' to exclude the timestamp itself
	minScore := fmt.Sprintf("(%d", sinceTimestamp)
	return r.readRange(ctx, key, minScore, "+inf")
}

// ReadRange reads entries between timestamps
func (r *Reader) ReadRange(ctx context.Context, catalog string, minTimestamp, maxTimestamp int64) ([]ReadResult, error) {
	key := r.makeCatalogKey(catalog)
	return r.readRange(ctx, key, fmt.Sprintf("%d", minTimestamp), fmt.Sprintf("%d", maxTimestamp))
}

// GetLatestSnap returns the latest snapshot info
func (r *Reader) GetLatestSnap(ctx context.Context, catalog string) (*SnapInfo, error) {
	key := r.makeSnapKey(catalog)

	// ZREVRANGEBYSCORE to get the latest snapshot
	results, err := r.rdb.ZRevRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: 0,
		Count:  1,
	}).Result()

	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, nil // No snapshot found
	}

	startTsSeq, stopTsSeq, err := DecodeSnapMember(results[0].Member.(string))
	if err != nil {
		return nil, err
	}

	return &SnapInfo{
		StartTsSeq: startTsSeq,
		StopTsSeq:  stopTsSeq,
		Score:      results[0].Score,
	}, nil
}

// SnapInfo represents snapshot information
type SnapInfo struct {
	StartTsSeq TimeSeqID // Start time sequence (e.g., "1700000000_1" or "0_0" for first snap)
	StopTsSeq  TimeSeqID // Stop time sequence (e.g., "1700000100_500")
	Score      float64   // Score in Redis (stopTsSeq's timestamp)
}

func (r *Reader) readRange(ctx context.Context, key, min, max string) ([]ReadResult, error) {
	results, err := r.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()

	if err != nil {
		return nil, err
	}

	var entries []ReadResult
	for _, z := range results {
		member := z.Member.(string)

		// Skip snapshot members
		if IsSnapMember(member) {
			continue
		}

		// Skip non-data members
		if !IsDataMember(member) {
			continue
		}

		field, tsSeqString, mergeType, err := DecodeMember(member)
		if err != nil {
			continue // Skip invalid members
		}

		tsSeq, err := ParseTimeSeqID(tsSeqString)
		if err != nil {
			return nil, fmt.Errorf("failed to parse tsSeqID: %w", err)
			// continue // Skip invalid members
		}

		entries = append(entries, ReadResult{
			Field:     field,
			TsSeq:     tsSeq,
			MergeType: mergeType,
			Timestamp: int64(z.Score),
		})
	}

	return entries, nil
}
