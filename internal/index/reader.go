package index

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Reader reads Lake's Redis index. Maintains a 5-second-resolution
// clock (redisTimeUnix) for snapshot freshness checks. The background
// ticker runs for the process lifetime; Lake is intended to be used
// with one Client per process, so OS reclaim on exit is sufficient.
type Reader struct {
	rdb           *redis.Client
	redisTimeUnix atomic.Int64
	indexIO
}

func NewReader(rdb *redis.Client) *Reader {
	r := &Reader{rdb: rdb}
	go r.timeUpdater()
	return r
}

// SnapInfo records that a catalog has been snapshotted up to StopTsSeq.
type SnapInfo struct{ StopTsSeq TimeSeqID }

func (s SnapInfo) Score() float64 { return s.StopTsSeq.Score() }
func (s SnapInfo) Dump() string   { return fmt.Sprintf("  Stop: %s\n", s.StopTsSeq) }

// DeltaInfo is one parsed entry from the catalog's delta zset.
type DeltaInfo struct {
	Member    string
	Score     float64
	TsSeq     TimeSeqID
	MergeType MergeType
	Path      string
	UUID      string // OSS object identifier (allocated by WriteBegin)
	Body      []byte // populated lazily by readers
}

type ReadIndexResult struct {
	Catalog string
	Deltas  []DeltaInfo
	Err     error
}

type BatchListResult struct {
	Snap       *SnapInfo
	ReadResult *ReadIndexResult
}

func (r *Reader) ReadAll(ctx context.Context, catalog string) *ReadIndexResult {
	return r.readRange(ctx, catalog, "-inf", "+inf")
}

func (r *Reader) ReadSince(ctx context.Context, catalog string, sinceTimestamp float64) *ReadIndexResult {
	return r.readRange(ctx, catalog, fmt.Sprintf("(%.6f", sinceTimestamp), "+inf")
}

func (r *Reader) ReadRange(ctx context.Context, catalog string, minTimestamp, maxTimestamp float64) *ReadIndexResult {
	return r.readRange(ctx, catalog, fmt.Sprintf("%.6f", minTimestamp), fmt.Sprintf("%.6f", maxTimestamp))
}

// ReadSafeRemoveDeltas returns deltas safely removable for a catalog
// (those at or before the latest snap, snap older than 60s).
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

// BatchList runs an HMGet on snaps + a pipelined ZRange for every
// catalog — 2 round-trips total regardless of catalog count.
func (r *Reader) BatchList(ctx context.Context, catalogs []string) map[string]*BatchListResult {
	out := make(map[string]*BatchListResult, len(catalogs))
	if len(catalogs) == 0 {
		return out
	}
	for _, c := range catalogs {
		out[c] = &BatchListResult{}
	}

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
		out[c].ReadResult = r.processZMembers(c, zs)
	}
	return out
}

func (r *Reader) readRange(ctx context.Context, catalog, min, max string) *ReadIndexResult {
	zs, err := r.rdb.ZRangeByScoreWithScores(ctx, r.MakeDeltaZsetKey(catalog), &redis.ZRangeBy{Min: min, Max: max}).Result()
	if err != nil {
		return &ReadIndexResult{Catalog: catalog, Err: err}
	}
	return r.processZMembers(catalog, zs)
}

// processZMembers parses zset entries into deltas. V3 has no pending
// state — every member must be a delta member.
func (r *Reader) processZMembers(catalog string, zs []redis.Z) *ReadIndexResult {
	var entries []DeltaInfo
	for _, z := range zs {
		member := z.Member.(string)
		if !IsDeltaMember(member) {
			return &ReadIndexResult{Catalog: catalog, Err: fmt.Errorf("unknown member %q", member)}
		}
		d, err := DecodeDeltaMember(member, z.Score)
		if err != nil {
			return &ReadIndexResult{Catalog: catalog, Err: fmt.Errorf("decode delta: %w", err)}
		}
		entries = append(entries, *d)
	}
	return &ReadIndexResult{Catalog: catalog, Deltas: entries}
}

func (r *Reader) timeUpdater() {
	if ts, err := r.serverUnix(context.Background()); err == nil {
		r.redisTimeUnix.Store(ts)
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if ts, err := r.serverUnix(context.Background()); err == nil {
			r.redisTimeUnix.Store(ts)
		}
	}
}

// NowUnix returns Lake's notion of the current time in unix seconds,
// taken from the Redis server clock (refreshed every 5s). During the
// brief window before the first sync it falls back to the local clock.
func (r *Reader) NowUnix() int64 {
	if t := r.redisTimeUnix.Load(); t > 0 {
		return t
	}
	return time.Now().Unix()
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
