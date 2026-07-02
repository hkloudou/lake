package index

import (
	"context"
	"fmt"
	"strconv"
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
type SnapInfo struct {
	StopTsSeq TimeSeqID
	URI       string // storage locator provider://bucket/path of the snap object
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
	URI       string // storage locator provider://bucket/path (carried in the member)
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

// listScript reads the catalog's snap pointer and the deltas past it in ONE
// atomic step. Atomicity is a correctness requirement, not an optimization:
// with Compact in the picture, a non-atomic HGET→ZRANGE pair could observe an
// old snap pointer, then a delta range already compacted up to a newer snap —
// silently dropping the deltas in between from the merged document. Inside a
// script nothing interleaves, and the snap pointer is monotonic (AddSnap), so
// every read observes a consistent (snap, deltas-after-it) pair.
//
// A snap value snap_score rejects falls back to the full range; the Go side
// then surfaces the decode error (parseListReply). KEYS[1] = snaps hash,
// KEYS[2] = delta zset; ARGV[1] = catalog. Returns {snapValue|false, flat
// [member, score, ...]} — scores pass through as Redis reply strings, never
// via Lua numbers (tostring would mangle the float).
const listScript = snapScoreLua + `
local snap = redis.call("HGET", KEYS[1], ARGV[1])
local min = "-inf"
if snap then
  local score = snap_score(snap)
  if score then
    min = "(" .. string.format("%.6f", score)
  end
end
return {snap or false, redis.call("ZRANGEBYSCORE", KEYS[2], min, "+inf", "WITHSCORES")}
`

// ListCatalog atomically reads the snap pointer and the deltas past it —
// the single read primitive behind Client.List / Client.BatchList.
func (r *Reader) ListCatalog(ctx context.Context, catalog string) (*SnapInfo, *ReadIndexResult) {
	res, err := r.rdb.Eval(ctx, listScript,
		[]string{r.MakeSnapsHashKey(), r.MakeDeltaZsetKey(catalog)},
		catalog,
	).Result()
	if err != nil {
		return nil, &ReadIndexResult{Catalog: catalog, Err: fmt.Errorf("list eval: %w", err)}
	}
	return r.parseListResult(catalog, res)
}

// parseListResult converts one listScript reply into (snap, deltas). An
// undecodable snap value is an error, not a silent nil: the read path must
// not fall back to replaying all deltas as if no snapshot existed (the full
// log may long predate what the snapshot absorbed — or be compacted away).
func (r *Reader) parseListResult(catalog string, res any) (*SnapInfo, *ReadIndexResult) {
	rawSnap, zs, err := parseListReply(res)
	if err != nil {
		return nil, &ReadIndexResult{Catalog: catalog, Err: err}
	}
	var snap *SnapInfo
	if rawSnap != "" {
		stop, uri, derr := DecodeSnapValue(rawSnap)
		if derr != nil {
			return nil, &ReadIndexResult{Catalog: catalog, Err: fmt.Errorf("decode snap: %w", derr)}
		}
		snap = &SnapInfo{StopTsSeq: stop, URI: uri}
	}
	return snap, r.processZMembers(catalog, zs)
}

// parseListReply unpacks the raw {snapValue|false, [member, score, ...]}
// script reply. Scores arrive as Redis reply strings ("%.17g"), which
// strconv.ParseFloat round-trips to the exact stored double — the same
// fidelity go-redis gives ZRangeByScoreWithScores, so DecodeDeltaMember's
// score-lockstep check keeps holding.
func parseListReply(res any) (string, []redis.Z, error) {
	arr, ok := res.([]any)
	if !ok || len(arr) != 2 {
		return "", nil, fmt.Errorf("unexpected list reply: %T", res)
	}
	rawSnap, _ := arr[0].(string) // Lua false → nil → not a string → ""
	flat, ok := arr[1].([]any)
	if !ok {
		return "", nil, fmt.Errorf("unexpected list deltas reply: %T", arr[1])
	}
	if len(flat)%2 != 0 {
		return "", nil, fmt.Errorf("odd WITHSCORES reply length: %d", len(flat))
	}
	zs := make([]redis.Z, 0, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		member, ok1 := flat[i].(string)
		scoreStr, ok2 := flat[i+1].(string)
		if !ok1 || !ok2 {
			return "", nil, fmt.Errorf("unexpected member/score types: %T/%T", flat[i], flat[i+1])
		}
		score, perr := strconv.ParseFloat(scoreStr, 64)
		if perr != nil {
			return "", nil, fmt.Errorf("invalid score %q: %w", scoreStr, perr)
		}
		zs = append(zs, redis.Z{Member: member, Score: score})
	}
	return rawSnap, zs, nil
}

func (r *Reader) GetLatestSnap(ctx context.Context, catalog string) (*SnapInfo, error) {
	val, err := r.rdb.HGet(ctx, r.MakeSnapsHashKey(), catalog).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	stop, uri, err := DecodeSnapValue(val)
	if err != nil {
		return nil, err
	}
	return &SnapInfo{StopTsSeq: stop, URI: uri}, nil
}

// snapScanBatch is the HSCAN page size. Tuned so each Redis call returns
// in well under a millisecond on the server (single-threaded event loop
// tolerates ~10⁴ small fields per op without noticeable jitter); larger
// values trade pipeline depth for hash-table coverage per page.
const snapScanBatch = 500

// IterateSnaps streams every catalog's snap to fn via HSCAN — no single
// Redis op holds the server's main thread for more than ~snapScanBatch
// fields, so this scales to multi-million-catalog deployments without
// stalling concurrent reads/writes. Iteration stops when fn returns false
// or the hash is exhausted; ctx cancellation is honoured between pages.
//
// Each (catalog, snap) is yielded at most once for snap values that
// existed throughout the scan; a catalog that is added or removed *during*
// the scan may be observed once, twice, or not at all (HSCAN's standard
// concurrent-modification semantics). Values that fail to decode are
// skipped silently.
func (r *Reader) IterateSnaps(ctx context.Context, fn func(catalog string, snap SnapInfo) bool) error {
	key := r.MakeSnapsHashKey()
	var cursor uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pairs, next, err := r.rdb.HScan(ctx, key, cursor, "", snapScanBatch).Result()
		if err != nil {
			return err
		}
		for i := 0; i+1 < len(pairs); i += 2 {
			stop, uri, derr := DecodeSnapValue(pairs[i+1])
			if derr != nil {
				continue
			}
			if !fn(pairs[i], SnapInfo{StopTsSeq: stop, URI: uri}) {
				return nil
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

// BatchList runs one pipelined listScript per catalog — a single round-trip
// regardless of catalog count, and each catalog's (snap, deltas) pair is
// atomic on the server (per-catalog atomicity is all a reader needs; no
// cross-catalog consistency is promised).
func (r *Reader) BatchList(ctx context.Context, catalogs []string) map[string]*BatchListResult {
	out := make(map[string]*BatchListResult, len(catalogs))
	if len(catalogs) == 0 {
		return out
	}

	pipe := r.rdb.Pipeline()
	cmds := make(map[string]*redis.Cmd, len(catalogs))
	for _, c := range catalogs {
		out[c] = &BatchListResult{}
		cmds[c] = pipe.Eval(ctx, listScript,
			[]string{r.MakeSnapsHashKey(), r.MakeDeltaZsetKey(c)}, c)
	}
	pipe.Exec(ctx)
	for c, cmd := range cmds {
		res, err := cmd.Result()
		if err != nil {
			out[c].ReadResult = &ReadIndexResult{Catalog: c, Err: fmt.Errorf("list eval: %w", err)}
			continue
		}
		out[c].Snap, out[c].ReadResult = r.parseListResult(c, res)
	}
	return out
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
