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
	Member string
	Score  float64

	TsSeq     TimeSeqID
	MergeType MergeType
	Path      string
	Body      []byte // Optional: filled by fillDeltasBody
}

// ReadAllResult holds read results with pending status
type ReadIndexResult struct {
	Catalog    string
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
	tr.RecordSpan("Read.ReadAll", map[string]interface{}{
		"catalog": catalog,
	})
	return r.readRange(ctx, catalog, "-inf", "+inf")
}

// ReadSince reads entries since the given timestamp (exclusive)
func (r *Reader) ReadSince(ctx context.Context, catalog string, sinceTimestamp float64) *ReadIndexResult {
	// Use '(' to exclude the timestamp itself
	return r.readRange(ctx, catalog, fmt.Sprintf("(%.6f", sinceTimestamp), "+inf")
}

// ReadRange reads entries between timestamps
func (r *Reader) ReadRange(ctx context.Context, catalog string, minTimestamp, maxTimestamp float64) *ReadIndexResult {
	return r.readRange(ctx, catalog, fmt.Sprintf("%.6f", minTimestamp), fmt.Sprintf("%.6f", maxTimestamp))
}

func (r *Reader) readSnapBefore(ctx context.Context, catalog string, beforeTimestamp float64) ([]SnapInfo, error) {
	// return r.readRange(ctx, catalog, "-inf", fmt.Sprintf("%.6f", beforeTimestamp))
	key := r.MakeSnapZsetKey(catalog)
	results, err := r.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("(%.6f", beforeTimestamp), // exclude the timestamp itself
		Offset: 0,
		Count:  -1,
	}).Result()
	if err != nil {
		return nil, err
	}
	var snaps []SnapInfo
	for _, result := range results {
		member := result.Member.(string)
		if !IsSnapMember(member) {
			continue
		}
		startTsSeq, stopTsSeq, err := DecodeSnapMember(member)
		if err != nil {
			continue
		}
		snaps = append(snaps, SnapInfo{
			Member: member,
			Score:  result.Score,

			StartTsSeq: startTsSeq,
			StopTsSeq:  stopTsSeq,
		})
	}
	return snaps, nil
}

func (r *Reader) ReadSafeRemoveRange(ctx context.Context, catalog string) ([]SnapInfo, *ReadIndexResult) {
	return r.ReadSafeRemoveRangeWithRetention(ctx, catalog, 0)
}

// ReadSafeRemoveRangeWithRetention reads safe-to-remove deltas and snaps, while keeping historical snapshots
// keepSnaps: number of historical snapshots to keep (latest snap is always kept due to < filter)
//   - keepSnaps = 0: only keep the latest snap, remove all historical snaps
//   - keepSnaps = 1: keep the latest snap + 1 historical snap
//   - The latest snap is excluded by readSnapBefore using strict less-than (<)
func (r *Reader) ReadSafeRemoveRangeWithRetention(ctx context.Context, catalog string, keepSnaps int) ([]SnapInfo, *ReadIndexResult) {
	snap, err := r.GetLatestSnap(ctx, catalog)
	if err != nil {
		return nil, &ReadIndexResult{
			Err:        fmt.Errorf("failed to get latest snap: %w", err),
			Catalog:    catalog,
			HasPending: false,
			Deltas:     nil,
		}
	}

	if snap == nil {
		return nil, &ReadIndexResult{
			Err:        nil,
			Catalog:    catalog,
			HasPending: false,
			Deltas:     nil,
		}
	}
	age := int64(r.redisTimeUnix) - int64(snap.StopTsSeq.Score())

	// if snapshot is too new, return error
	if age < 60 {
		return nil, &ReadIndexResult{
			Err:        fmt.Errorf("snapshot is too new: %s", snap.StopTsSeq.String()),
			Catalog:    catalog,
			HasPending: false,
			Deltas:     nil,
		}
	}

	// Read all snaps before the latest one
	allSnaps, err := r.readSnapBefore(ctx, catalog, snap.StopTsSeq.Score())
	if err != nil {
		return nil, &ReadIndexResult{
			Err:        fmt.Errorf("failed to read snap before: %w", err),
			Catalog:    catalog,
			HasPending: false,
			Deltas:     nil,
		}
	}

	// Filter snaps based on retention policy
	snapsToRemove := r.filterSnapsForRemoval(allSnaps, keepSnaps)

	// Deltas can always be removed up to the latest snap
	result := r.ReadRange(ctx, catalog, 0, snap.StopTsSeq.Score())
	return snapsToRemove, result
}

// filterSnapsForRemoval filters snaps to keep the latest N historical snapshots
// snaps: all candidate historical snaps (sorted by score ascending, latest snap already excluded)
// keepCount: number of historical snaps to keep
// Returns: snaps to remove
func (r *Reader) filterSnapsForRemoval(snaps []SnapInfo, keepCount int) []SnapInfo {
	if keepCount <= 0 {
		// Keep 0 historical snaps, remove all (latest snap is already excluded)
		return snaps
	}

	if len(snaps) <= keepCount {
		// Not enough historical snaps to remove any
		return []SnapInfo{}
	}

	// snaps are already sorted by score ascending from readSnapBefore
	// We want to keep the latest N historical snaps, so remove the first (len - keepCount) items
	removeCount := len(snaps) - keepCount
	return snaps[:removeCount]
}

// GetLatestSnap returns the latest snapshot info
func (r *Reader) GetLatestSnap(ctx context.Context, catalog string) (*SnapInfo, error) {
	key := r.MakeSnapZsetKey(catalog)

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
		Member:     results[0].Member.(string),
		Score:      results[0].Score,
		StartTsSeq: startTsSeq,
		StopTsSeq:  stopTsSeq,
	}, nil
}

// SnapInfo represents snapshot information
type SnapInfo struct {
	Member string
	Score  float64

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

func (r *Reader) readRange(ctx context.Context, catalog string, min, max string) *ReadIndexResult {
	key := r.MakeDeltaZsetKey(catalog)
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
		"range":      fmt.Sprintf("%s ~ %s", min, max),
		"count":      fmt.Sprintf("%d/%d", len(entries), len(results)),
		"hasPending": hasPending,
		"error":      lastError,
	})
	return &ReadIndexResult{
		Catalog:    catalog,
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
	metaKey := c.MakeMetaKey(catalog)
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
