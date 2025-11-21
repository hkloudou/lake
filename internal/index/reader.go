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
	UUID      string
	Timestamp int64
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

	uuid, err := DecodeSnapMember(results[0].Member.(string))
	if err != nil {
		return nil, err
	}

	return &SnapInfo{
		UUID:      uuid,
		Timestamp: int64(results[0].Score),
	}, nil
}

// SnapInfo represents snapshot information
type SnapInfo struct {
	UUID      string
	Timestamp int64
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

		field, uuid, err := DecodeMember(member)
		if err != nil {
			continue // Skip invalid members
		}

		entries = append(entries, ReadResult{
			Field:     field,
			UUID:      uuid,
			Timestamp: int64(z.Score),
		})
	}

	return entries, nil
}
