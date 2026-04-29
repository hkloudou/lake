package index

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Reader reads Lake's Redis index. Maintains a 5-second-resolution clock
// (redisTimeUnix) used for pending-write age and snapshot freshness;
// callers must Close the Reader to stop the background ticker.
type Reader struct {
	rdb           *redis.Client
	redisTimeUnix atomic.Int64
	done          chan struct{}
	closeOnce     sync.Once
	indexIO
}

// NewReader returns a Reader that has begun ticking. SetPrefix must be
// called before any read method.
func NewReader(rdb *redis.Client) *Reader {
	r := &Reader{rdb: rdb, done: make(chan struct{})}
	go r.timeUpdater()
	return r
}

// Close stops the background time-sync goroutine. Idempotent.
func (r *Reader) Close() {
	r.closeOnce.Do(func() { close(r.done) })
}

// SnapInfo records that a catalog has been snapshotted up to StopTsSeq.
// Reads merge any deltas with score > StopTsSeq.Score() on top.
type SnapInfo struct {
	StopTsSeq TimeSeqID
}

func (s SnapInfo) Score() float64 { return s.StopTsSeq.Score() }
func (s SnapInfo) Dump() string   { return fmt.Sprintf("  Stop: %s\n", s.StopTsSeq) }

// DeltaInfo is one parsed entry from the catalog's delta zset.
type DeltaInfo struct {
	Member    string
	Score     float64
	TsSeq     TimeSeqID
	MergeType MergeType
	Path      string
	Body      []byte // populated lazily by readers
}

// ReadIndexResult is the parsed view of a delta zset slice.
type ReadIndexResult struct {
	Catalog    string
	Deltas     []DeltaInfo
	HasPending bool
	Err        error
}

// BatchListResult combines the snap + delta read for one catalog.
type BatchListResult struct {
	Snap       *SnapInfo
	ReadResult *ReadIndexResult
}

// ReadAll returns every delta for the catalog.
func (r *Reader) ReadAll(ctx context.Context, catalog string, strictPending bool) *ReadIndexResult {
	return r.readRange(ctx, catalog, "-inf", "+inf", strictPending)
}

// ReadSince returns deltas with score > sinceTimestamp.
func (r *Reader) ReadSince(ctx context.Context, catalog string, sinceTimestamp float64, strictPending bool) *ReadIndexResult {
	return r.readRange(ctx, catalog, fmt.Sprintf("(%.6f", sinceTimestamp), "+inf", strictPending)
}

// ReadRange returns deltas in [minTimestamp, maxTimestamp].
func (r *Reader) ReadRange(ctx context.Context, catalog string, minTimestamp, maxTimestamp float64) *ReadIndexResult {
	return r.readRange(ctx, catalog, fmt.Sprintf("%.6f", minTimestamp), fmt.Sprintf("%.6f", maxTimestamp), false)
}

// ReadSafeRemoveDeltas returns deltas safely removable for a catalog
// (those at or before the latest snap). Empty if no snap exists or the
// snap is younger than 60s.
func (r *Reader) ReadSafeRemoveDeltas(ctx context.Context, catalog string) *ReadIndexResult {
	snap, err := r.GetLatestSnap(ctx, catalog)
	if err != nil {
		return &ReadIndexResult{Catalog: catalog, Err: fmt.Errorf("get latest snap: %w", err)}
	}
	if snap == nil {
		return &ReadIndexResult{Catalog: catalog}
	}
	if r.redisTimeUnix.Load()-int64(snap.StopTsSeq.Score()) < 60 {
		return &ReadIndexResult{Catalog: catalog, Err: fmt.Errorf("snapshot is too new: %s", snap.StopTsSeq)}
	}
	return r.ReadRange(ctx, catalog, 0, snap.StopTsSeq.Score())
}

// GetLatestSnap returns the catalog's snap metadata (nil if none).
func (r *Reader) GetLatestSnap(ctx context.Context, catalog string) (*SnapInfo, error) {
	val, err := r.rdb.HGet(ctx, r.MakeSnapsHashKey(), catalog).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	stop, err := DecodeSnapValue(val)
	if err != nil {
		return nil, err
	}
	return &SnapInfo{StopTsSeq: stop}, nil
}

// AllSnaps returns every catalog's snap metadata via a single HGETALL.
// Catalogs with undecodable values are skipped.
func (r *Reader) AllSnaps(ctx context.Context) (map[string]SnapInfo, error) {
	all, err := r.rdb.HGetAll(ctx, r.MakeSnapsHashKey()).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]SnapInfo, len(all))
	for catalog, val := range all {
		if stop, err := DecodeSnapValue(val); err == nil {
			out[catalog] = SnapInfo{StopTsSeq: stop}
		}
	}
	return out, nil
}

// BatchList runs a snap HMGet plus a pipelined delta ZRange for every
// catalog — 2 round-trips total regardless of catalog count.
func (r *Reader) BatchList(ctx context.Context, catalogs []string, strictPending bool) map[string]*BatchListResult {
	out := make(map[string]*BatchListResult, len(catalogs))
	if len(catalogs) == 0 {
		return out
	}
	for _, c := range catalogs {
		out[c] = &BatchListResult{}
	}

	// Phase 1: HMGet on the global snaps hash.
	snapVals, err := r.rdb.HMGet(ctx, r.MakeSnapsHashKey(), catalogs...).Result()
	if err != nil && err != redis.Nil {
		for _, c := range catalogs {
			out[c].ReadResult = &ReadIndexResult{Catalog: c, Err: fmt.Errorf("hmget snaps: %w", err)}
		}
		return out
	}
	for i, raw := range snapVals {
		c := catalogs[i]
		s, ok := raw.(string)
		if !ok {
			continue
		}
		stop, err := DecodeSnapValue(s)
		if err != nil {
			out[c].ReadResult = &ReadIndexResult{Catalog: c, Err: fmt.Errorf("decode snap: %w", err)}
			continue
		}
		out[c].Snap = &SnapInfo{StopTsSeq: stop}
	}

	// Phase 2: pipelined delta ZRange per catalog.
	pipe := r.rdb.Pipeline()
	cmds := make(map[string]*redis.ZSliceCmd, len(catalogs))
	for _, c := range catalogs {
		if out[c].ReadResult != nil && out[c].ReadResult.Err != nil {
			continue
		}
		min := "-inf"
		if out[c].Snap != nil {
			min = fmt.Sprintf("(%.6f", out[c].Snap.StopTsSeq.Score())
		}
		cmds[c] = pipe.ZRangeByScoreWithScores(ctx, r.MakeDeltaZsetKey(c), &redis.ZRangeBy{Min: min, Max: "+inf"})
	}
	pipe.Exec(ctx)
	for c, cmd := range cmds {
		zs, err := cmd.Result()
		if err != nil && err != redis.Nil {
			out[c].ReadResult = &ReadIndexResult{Catalog: c, Err: fmt.Errorf("zrange: %w", err)}
			continue
		}
		out[c].ReadResult = r.processZMembers(c, zs, strictPending)
	}
	return out
}

func (r *Reader) readRange(ctx context.Context, catalog, min, max string, strictPending bool) *ReadIndexResult {
	zs, err := r.rdb.ZRangeByScoreWithScores(ctx, r.MakeDeltaZsetKey(catalog), &redis.ZRangeBy{Min: min, Max: max}).Result()
	if err != nil {
		return &ReadIndexResult{Catalog: catalog, Err: err}
	}
	return r.processZMembers(catalog, zs, strictPending)
}

// processZMembers parses zset entries into deltas and detects pending
// writes. A pending member is "unresolved" if its age < 120s; if a
// committed delta follows an unresolved pending the read is flagged
// HasPending. With strictPending=true, any unresolved pending sets the
// flag regardless of position.
func (r *Reader) processZMembers(catalog string, zs []redis.Z, strictPending bool) *ReadIndexResult {
	const timeoutSec = 120

	var entries []DeltaInfo
	var hasPending, sawUnresolvedPending bool
	now := r.redisTimeUnix.Load()

	for _, z := range zs {
		member := z.Member.(string)
		switch {
		case IsPendingMember(member):
			if now-int64(z.Score) > timeoutSec {
				continue // abandoned
			}
			sawUnresolvedPending = true
			if strictPending {
				hasPending = true
			}
		case IsDeltaMember(member):
			if sawUnresolvedPending {
				hasPending = true
			}
			d, err := DecodeDeltaMember(member, z.Score)
			if err != nil {
				return &ReadIndexResult{Catalog: catalog, Err: fmt.Errorf("decode delta: %w", err)}
			}
			entries = append(entries, *d)
		default:
			return &ReadIndexResult{Catalog: catalog, Err: fmt.Errorf("unknown member %q", member)}
		}
	}
	return &ReadIndexResult{Catalog: catalog, Deltas: entries, HasPending: hasPending}
}

// timeUpdater pulls Redis TIME every 5s into redisTimeUnix.
func (r *Reader) timeUpdater() {
	if ts, err := r.serverUnix(context.Background()); err == nil {
		r.redisTimeUnix.Store(ts)
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			if ts, err := r.serverUnix(context.Background()); err == nil {
				r.redisTimeUnix.Store(ts)
			}
		}
	}
}

func (r *Reader) serverUnix(ctx context.Context) (int64, error) {
	res, err := r.rdb.Eval(ctx, `return tonumber(redis.call("TIME")[1])`, nil).Result()
	if err != nil {
		return 0, err
	}
	ts, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("redis TIME returned %T, want int64", res)
	}
	return ts, nil
}

