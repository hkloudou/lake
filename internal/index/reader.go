package index

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Reader handles reading from Redis ZADD index
type Reader struct {
	rdb *redis.Client
	// prefix string
	indexKey
}

// NewReader creates a new index reader
func NewReader(rdb *redis.Client) *Reader {
	return &Reader{
		rdb: rdb,
		indexKey: indexKey{
			prefix: "lake",
		}, // Will be set later via SetPrefix
	}
}

// DeltaInfo represents delta information (with optional body data)
type DeltaInfo struct {
	Field     string
	TsSeq     TimeSeqID
	MergeType MergeType
	Score     float64
	Body      []byte // Optional: filled by fillDeltasBody
}

// ReadAll reads all entries from the catalog
func (r *Reader) ReadAll(ctx context.Context, catalog string) ([]DeltaInfo, error) {
	key := r.makeCatalogKey(catalog)
	return r.readRange(ctx, key, "-inf", "+inf")
}

// ReadSince reads entries since the given timestamp (exclusive)
func (r *Reader) ReadSince(ctx context.Context, catalog string, sinceTimestamp float64) ([]DeltaInfo, error) {
	key := r.makeCatalogKey(catalog)
	// Use '(' to exclude the timestamp itself
	// minScore := fmt.Sprintf("(%d", sinceTimestamp)
	return r.readRange(ctx, key, fmt.Sprintf("(%f", sinceTimestamp), "+inf")
}

// ReadRange reads entries between timestamps
func (r *Reader) ReadRange(ctx context.Context, catalog string, minTimestamp, maxTimestamp int64) ([]DeltaInfo, error) {
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
		// Score:      results[0].Score,
	}, nil
}

// SnapInfo represents snapshot information
type SnapInfo struct {
	StartTsSeq TimeSeqID // Start time sequence (e.g., "1700000000_1" or "0_0" for first snap)
	StopTsSeq  TimeSeqID // Stop time sequence (e.g., "1700000100_500")
	// Score      float64   // Score in Redis (stopTsSeq's timestamp)
}

func (m SnapInfo) Dump() string {
	// fmt.Println(fmt.Sprintf("Snapshot:\n"))
	var output strings.Builder
	output.WriteString(fmt.Sprintf("  Time Range: %s ~ %s\n", m.StartTsSeq, m.StopTsSeq))
	// output.WriteString(fmt.Sprintf("  Score: %.6f\n", m.Score))
	return output.String()
}

func (r *Reader) readRange(ctx context.Context, key, min, max string) ([]DeltaInfo, error) {
	results, err := r.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()

	if err != nil {
		return nil, err
	}

	var entries []DeltaInfo
	for _, z := range results {
		member := z.Member.(string)

		// Skip snapshot members
		if IsSnapMember(member) {
			continue
		}

		// Skip non-data members
		if !IsDeltaMember(member) {
			continue
		}

		field, tsSeqString, mergeType, err := DecodeDeltaMember(member)
		if err != nil {
			continue // Skip invalid members
		}

		tsSeq, err := ParseTimeSeqID(tsSeqString)
		if err != nil {
			return nil, fmt.Errorf("failed to parse tsSeqID: %w", err)
			// continue // Skip invalid members
		}
		if tsSeq.Score() != z.Score {
			return nil, fmt.Errorf("score mismatch: got %f, expected %f", tsSeq.Score(), z.Score)
		}

		entries = append(entries, DeltaInfo{
			Field:     field,
			TsSeq:     tsSeq,
			MergeType: mergeType,
			Score:     z.Score,
		})
	}

	return entries, nil
}
