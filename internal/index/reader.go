package index

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Reader handles reading from Redis ZADD index.
//
// redisTimeUnix is updated periodically from Redis TIME and read concurrently
// from request goroutines (pending-write age, snapshot freshness checks). It
// must be accessed only via Load/Store to remain race-free.
//
// Close stops the background time-sync goroutine. It must be called when the
// owning Client is shut down; otherwise the goroutine leaks.
type Reader struct {
	rdb           *redis.Client
	redisTimeUnix atomic.Int64
	done          chan struct{}
	closeOnce     sync.Once
	indexIO
}

// NewReader creates a new index reader.
func NewReader(rdb *redis.Client) *Reader {
	reader := &Reader{
		rdb:     rdb,
		done:    make(chan struct{}),
		indexIO: indexIO{prefix: "lake"}, // overridden later via SetPrefix
	}
	reader.startRedisTimeUnixUpdater()
	return reader
}

// Close stops the background updater. Idempotent.
func (r *Reader) Close() {
	r.closeOnce.Do(func() {
		close(r.done)
	})
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
func (r *Reader) ReadAll(ctx context.Context, catalog string, strictPending bool) *ReadIndexResult {
	return r.readRange(ctx, catalog, "-inf", "+inf", strictPending)
}

// ReadSince reads entries since the given timestamp (exclusive)
func (r *Reader) ReadSince(ctx context.Context, catalog string, sinceTimestamp float64, strictPending bool) *ReadIndexResult {
	// Use '(' to exclude the timestamp itself
	return r.readRange(ctx, catalog, fmt.Sprintf("(%.6f", sinceTimestamp), "+inf", strictPending)
}

// ReadRange reads entries between timestamps
func (r *Reader) ReadRange(ctx context.Context, catalog string, minTimestamp, maxTimestamp float64) *ReadIndexResult {
	return r.readRange(ctx, catalog, fmt.Sprintf("%.6f", minTimestamp), fmt.Sprintf("%.6f", maxTimestamp), false)
}

// ReadSafeRemoveDeltas returns the deltas safely removable for a catalog
// (those at or before the catalog's latest snap), or an empty result if
// the catalog has no snap or the snap is too new (< 60s old, possibly
// still being filled).
//
// V3 keeps only one snap per catalog (snap is idempotent and self-
// correcting), so there are no historical snaps to enumerate or filter —
// callers only need to worry about removing old delta entries.
func (r *Reader) ReadSafeRemoveDeltas(ctx context.Context, catalog string) *ReadIndexResult {
	snap, err := r.GetLatestSnap(ctx, catalog)
	if err != nil {
		return &ReadIndexResult{
			Err:     fmt.Errorf("failed to get latest snap: %w", err),
			Catalog: catalog,
		}
	}
	if snap == nil {
		// No snap → no safe removal point.
		return &ReadIndexResult{Catalog: catalog}
	}
	age := r.redisTimeUnix.Load() - int64(snap.StopTsSeq.Score())
	if age < 60 {
		return &ReadIndexResult{
			Err:     fmt.Errorf("snapshot is too new: %s", snap.StopTsSeq.String()),
			Catalog: catalog,
		}
	}
	return r.ReadRange(ctx, catalog, 0, snap.StopTsSeq.Score())
}

// GetLatestSnap returns the catalog's snapshot metadata, or nil if the
// catalog has no snap yet. Reads a single field from the deployment-wide
// "<prefix>:snaps" hash.
func (r *Reader) GetLatestSnap(ctx context.Context, catalog string) (*SnapInfo, error) {
	val, err := r.rdb.HGet(ctx, r.MakeSnapsHashKey(), catalog).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	startTsSeq, stopTsSeq, err := DecodeSnapValue(val)
	if err != nil {
		return nil, err
	}
	return &SnapInfo{StartTsSeq: startTsSeq, StopTsSeq: stopTsSeq}, nil
}

// AllSnaps returns the latest snap metadata for every catalog in this
// deployment via a single HGETALL on "<prefix>:snaps". This is the
// canonical entry point for whole-deployment backup tooling that wants
// to enumerate every snap file in OSS without doing an OSS LIST.
//
// Catalogs whose snap value fails to decode are skipped silently; if you
// need to detect corruption, parse the hash yourself via HGETALL.
func (r *Reader) AllSnaps(ctx context.Context) (map[string]SnapInfo, error) {
	all, err := r.rdb.HGetAll(ctx, r.MakeSnapsHashKey()).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]SnapInfo, len(all))
	for catalog, val := range all {
		startTsSeq, stopTsSeq, err := DecodeSnapValue(val)
		if err != nil {
			continue
		}
		out[catalog] = SnapInfo{StartTsSeq: startTsSeq, StopTsSeq: stopTsSeq}
	}
	return out, nil
}

// SnapInfo represents the time range of a catalog's snapshot. The Redis
// score is derived from StopTsSeq, and the OSS object key is derived
// from (catalog, StartTsSeq, StopTsSeq); neither is held on the struct
// directly so there is one source of truth per dimension.
type SnapInfo struct {
	StartTsSeq TimeSeqID // start of the snap's covered range; "0_0" for the first snap
	StopTsSeq  TimeSeqID // stop of the snap's covered range
}

// Score returns the Redis score for this snap (== StopTsSeq.Score()).
func (m SnapInfo) Score() float64 { return m.StopTsSeq.Score() }

// Dump renders a human-readable single-line description.
func (m SnapInfo) Dump() string {
	var output strings.Builder
	output.WriteString(fmt.Sprintf("  Time Range: %s ~ %s\n", m.StartTsSeq, m.StopTsSeq))
	return output.String()
}

func (r *Reader) readRange(ctx context.Context, catalog string, min, max string, strictPending bool) *ReadIndexResult {
	key := r.MakeDeltaZsetKey(catalog)
	results, err := r.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()

	if err != nil {
		return &ReadIndexResult{Err: err}
	}

	result := r.processZMembers(catalog, results, strictPending)

	return result
}

// processZMembers processes raw Redis Z members into ReadIndexResult.
// Shared by readRange and BatchList.
func (r *Reader) processZMembers(catalog string, results []redis.Z, strictPending bool) *ReadIndexResult {
	var entries []DeltaInfo
	var timeoutThreshold = int64(120) // 2 minutes in seconds
	var hasPending bool
	var hasUnresolvedPending bool
	for _, z := range results {
		member := z.Member.(string)

		// Snap members never appear in the delta zset under v3 (snaps live
		// in their own "<prefix>:snaps" hash), so no IsSnapMember filter is
		// needed here.

		// Check pending members
		if IsPendingMember(member) {
			ageSeconds := r.redisTimeUnix.Load() - int64(z.Score)
			if ageSeconds > timeoutThreshold {
				// Timeout > timeoutThreshold: ignore (abandoned write)
				continue
			}
			// Pending write in progress (age < timeout)
			hasUnresolvedPending = true
			if strictPending {
				hasPending = true
			}
			continue
		}
		// A delta after a pending means the read may have incomplete data
		if hasUnresolvedPending {
			hasPending = true
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

	return &ReadIndexResult{
		Catalog:    catalog,
		Deltas:     entries,
		HasPending: hasPending,
	}
}

// BatchListResult holds the combined snap + delta results for one catalog
type BatchListResult struct {
	Snap       *SnapInfo
	ReadResult *ReadIndexResult
}

// BatchList performs List operations for multiple catalogs using Redis Pipeline.
// Phase 1: pipeline all snap queries (1 round-trip)
// Phase 2: pipeline all delta queries using snap results (1 round-trip)
// Total: 2 round-trips regardless of catalog count.
func (r *Reader) BatchList(ctx context.Context, catalogs []string, strictPending bool) map[string]*BatchListResult {
	results := make(map[string]*BatchListResult, len(catalogs))
	if len(catalogs) == 0 {
		return results
	}

	// Initialize results
	for _, catalog := range catalogs {
		results[catalog] = &BatchListResult{}
	}

	// Phase 1: a single HMGet pulls every catalog's snap metadata from
	// the deployment-wide "<prefix>:snaps" hash in one round-trip.
	snapVals, err := r.rdb.HMGet(ctx, r.MakeSnapsHashKey(), catalogs...).Result()
	if err != nil && err != redis.Nil {
		for _, catalog := range catalogs {
			results[catalog].ReadResult = &ReadIndexResult{
				Catalog: catalog,
				Err:     fmt.Errorf("failed to get snapshots: %w", err),
			}
		}
		return results
	}
	for i, raw := range snapVals {
		catalog := catalogs[i]
		if raw == nil {
			continue // catalog has no snap yet
		}
		val, ok := raw.(string)
		if !ok {
			continue
		}
		startTsSeq, stopTsSeq, err := DecodeSnapValue(val)
		if err != nil {
			results[catalog].ReadResult = &ReadIndexResult{
				Catalog: catalog,
				Err:     fmt.Errorf("failed to decode snapshot: %w", err),
			}
			continue
		}
		results[catalog].Snap = &SnapInfo{StartTsSeq: startTsSeq, StopTsSeq: stopTsSeq}
	}

	// Phase 2: Pipeline all delta queries
	deltaPipe := r.rdb.Pipeline()
	deltaCmds := make(map[string]*redis.ZSliceCmd, len(catalogs))
	for _, catalog := range catalogs {
		// Skip catalogs that already failed in phase 1
		if results[catalog].ReadResult != nil && results[catalog].ReadResult.Err != nil {
			continue
		}
		key := r.MakeDeltaZsetKey(catalog)
		min := "-inf"
		if snap := results[catalog].Snap; snap != nil {
			min = fmt.Sprintf("(%.6f", snap.StopTsSeq.Score())
		}
		deltaCmds[catalog] = deltaPipe.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
			Min: min,
			Max: "+inf",
		})
	}
	deltaPipe.Exec(ctx)

	// Process delta results
	for catalog, cmd := range deltaCmds {
		zs, err := cmd.Result()
		if err != nil && err != redis.Nil {
			results[catalog].ReadResult = &ReadIndexResult{
				Catalog: catalog,
				Err:     fmt.Errorf("failed to read deltas: %w", err),
			}
			continue
		}
		results[catalog].ReadResult = r.processZMembers(catalog, zs, strictPending)
	}

	return results
}

func (c *Reader) startRedisTimeUnixUpdater() {
	go func() {
		// Initial fetch so the first read sees a real value rather than 0.
		if ts, err := c.getTimeUnix(context.Background()); err == nil {
			c.redisTimeUnix.Store(ts)
		}
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-c.done:
				return
			case <-ticker.C:
				if ts, err := c.getTimeUnix(context.Background()); err == nil {
					c.redisTimeUnix.Store(ts)
				}
			}
		}
	}()
}

func (w *Reader) getTimeUnix(ctx context.Context) (int64, error) {
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

