package index

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hkloudou/lake/v2/trace"
	"github.com/redis/go-redis/v9"
)

// Reader handles reading from Redis ZADD index
type Reader struct {
	rdb *redis.Client
	// prefix string
	redisTimeUnix int64
	indexIO
}

// NewReader creates a new index reader
func NewReader(rdb *redis.Client) *Reader {
	reader := &Reader{
		rdb: rdb,
		indexIO: indexIO{
			prefix: "lake",
		}, // Will be set later via SetPrefix
	}
	reader.startRedisTimeUnixUpdater()
	return reader
}

// DeltaInfo represents delta information (with optional body data)
type DeltaInfo struct {
	TsSeq     TimeSeqID
	MergeType MergeType
	Score     float64
	Path      string
	Body      []byte // Optional: filled by fillDeltasBody
}

// ReadAllResult holds read results with pending status
type ReadIndexResult struct {
	Deltas     []DeltaInfo
	HasPending bool
	Err        error
}

type SampleInfo struct {
	Indicator string
	Score     float64
}

// ReadAll reads all entries from the catalog
func (r *Reader) ReadAll(ctx context.Context, catalog string) *ReadIndexResult {
	tr := trace.FromContext(ctx)
	key := r.makeDeltaZsetKey(catalog)
	tr.RecordSpan("Read.ReadAll", map[string]interface{}{
		"key": key,
	})
	return r.readRange(ctx, key, "-inf", "+inf")
}

// ReadSince reads entries since the given timestamp (exclusive)
func (r *Reader) ReadSince(ctx context.Context, catalog string, sinceTimestamp float64) *ReadIndexResult {
	key := r.makeDeltaZsetKey(catalog)
	// Use '(' to exclude the timestamp itself
	return r.readRange(ctx, key, fmt.Sprintf("(%f", sinceTimestamp), "+inf")
}

// ReadRange reads entries between timestamps
func (r *Reader) ReadRange(ctx context.Context, catalog string, minTimestamp, maxTimestamp int64) *ReadIndexResult {
	key := r.makeDeltaZsetKey(catalog)
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

func (r *Reader) readRange(ctx context.Context, key, min, max string) *ReadIndexResult {
	tr := trace.FromContext(ctx)
	results, err := r.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()

	if err != nil {
		return &ReadIndexResult{Err: err}
	}

	var entries []DeltaInfo
	// var lastCommittedTimestamp float64
	// // First pass: collect committed entries and find latest timestamp
	var timeoutThreshold = int64(120) // 1 minute in seconds
	// ts := time.Now().Unix()
	var lastError error
	var hasPending bool
	for _, z := range results {
		member := z.Member.(string)

		// Skip snapshot members
		if IsSnapMember(member) {
			continue
		}

		// Check pending members
		if IsPendingMember(member) {
			ageSeconds := int64(r.redisTimeUnix) - int64(z.Score)
			if ageSeconds > timeoutThreshold {
				// Timeout > timeoutThreshold: ignore (abandoned write)
				continue
			}
			// Pending write in progress (age < timeout)
			lastError = fmt.Errorf("pending write detected: %s (age: %ds < %ds)", member, ageSeconds, timeoutThreshold)
			hasPending = true
			continue
		}

		// Only delta members should remain at this point
		if !IsDeltaMember(member) {
			// Unknown member type - data corruption
			return &ReadIndexResult{Err: fmt.Errorf("unknown member type (not snap/pending/delta): %q", member)}
		}

		deltaInfo, err := DecodeDeltaMember(member, z.Score)
		if err != nil {
			return &ReadIndexResult{Err: fmt.Errorf("failed to decode delta member: %w", err)}
		}

		entries = append(entries, *deltaInfo)
	}

	tr.RecordSpan("Read.ReadRange", map[string]interface{}{
		"count":      fmt.Sprintf("%d/%d", len(entries), len(results)),
		"hasPending": hasPending,
		"error":      lastError,
	})
	return &ReadIndexResult{
		Deltas:     entries,
		HasPending: hasPending,
		Err:        lastError,
	}
}

func (c *Reader) startRedisTimeUnixUpdater() {
	go func() {
		for {
			timestamp, err := c.getTimeUnix(context.Background())
			if err != nil {
				time.Sleep(5 * time.Second)
				return
			}
			c.redisTimeUnix = timestamp
			time.Sleep(5 * time.Second)
		}
	}()
}

func (w *Reader) getTimeUnix(ctx context.Context) (int64, error) {
	// encodedCatalog := encode.EncodeRedisCatalogName(catalog)
	// Execute Lua script
	result, err := w.rdb.Eval(ctx, `
local timeResult = redis.call("TIME")
local timestamp = timeResult[1]
return tonumber(timestamp)`,
		[]string{},
	).Result()

	if err != nil {
		return 0, fmt.Errorf("failed to get timeseq and precommit: %w", err)
	}

	// Parse result
	timestamp, ok := result.(int64)
	if !ok {
		return 0, fmt.Errorf("invalid timestamp type: %T", result)
	}
	return timestamp, nil
}

func (c *Reader) Meta(ctx context.Context, catalog string) (string, error) {
	tr := trace.FromContext(ctx)
	metaKey := c.makeMetaKey(catalog)
	tr.RecordSpan("Reader.Meta", map[string]interface{}{
		"catalog": catalog,
		"metaKey": metaKey,
	})
	val, err := c.rdb.Get(ctx, metaKey).Result()
	if err != nil && err != redis.Nil {
		return "", err
	}
	return val, nil
}
