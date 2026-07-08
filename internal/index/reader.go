package index

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Reader reads Lake's Redis index. Maintains a Redis-synced clock (see
// NowUnix) for snapshot freshness and handle-expiry checks. The background
// ticker runs until Close; a Reader that is never closed is reclaimed
// by the OS at process exit (the intended single-Client-per-process use).
type Reader struct {
	rdb       *redis.Client
	clock     atomic.Pointer[clockSync]
	done      chan struct{}
	closeOnce sync.Once
	indexIO
}

// clockSync is one observation of the Redis server clock, paired with the
// local monotonic instant it was taken at. NowUnix extrapolates from it, so
// the clock keeps ADVANCING between syncs, through sync failures, and after
// Close — a frozen clock would let signed handles outlive their expiry and
// pin WithMaxAge staleness forever.
type clockSync struct {
	redisUnix int64
	at        time.Time // monotonic anchor
}

func NewReader(rdb *redis.Client) *Reader {
	r := &Reader{rdb: rdb, done: make(chan struct{})}
	go r.timeUpdater()
	return r
}

// Close stops the background clock ticker. Idempotent. Reads keep working
// after Close: NowUnix extrapolates from the last sync with the local
// monotonic clock (or uses the local clock outright if no sync ever landed).
func (r *Reader) Close() {
	r.closeOnce.Do(func() { close(r.done) })
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
	// RemoveGen is the catalog's removal generation observed atomically with
	// this read ("0" until the first RemoveDelta). AddSnap refuses a snapshot
	// carrying a stale generation, so a read that listed a delta which was
	// removed while the read was in flight can never persist that delta's
	// effect into a snapshot.
	RemoveGen string
	Err       error
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
// then surfaces the decode error (parseListReply). The removal generation
// ("<catalog>:rg" field of the snaps hash, absent = "0") is read in the same
// atomic step so AddSnap can later tell whether a RemoveDelta interleaved.
// KEYS[1] = snaps hash, KEYS[2] = delta zset; ARGV[1] = catalog. Returns
// {snapValue|false, removeGen, flat [member, score, ...]} — scores pass
// through as Redis reply strings, never via Lua numbers (tostring would
// mangle the float).
const listScript = snapScoreLua + `
local snap = redis.call("HGET", KEYS[1], ARGV[1])
local rg = redis.call("HGET", KEYS[1], ARGV[1] .. ":rg") or "0"
local min = "-inf"
if snap then
  local score = snap_score(snap)
  if score then
    min = "(" .. string.format("%.6f", score)
  end
end
return {snap or false, rg, redis.call("ZRANGEBYSCORE", KEYS[2], min, "+inf", "WITHSCORES")}
`

// luaList dispatches listScript by SHA (EVALSHA, falling back to EVAL on a
// cold script cache) so the ~1 KB script body is not re-sent on every List.
var luaList = NewScript(listScript)

// ListCatalog atomically reads the snap pointer and the deltas past it —
// the single read primitive behind Client.List / Client.BatchList.
func (r *Reader) ListCatalog(ctx context.Context, catalog string) (*SnapInfo, *ReadIndexResult) {
	res, err := RunScript(ctx, r.rdb, luaList,
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
	rawSnap, removeGen, zs, err := parseListReply(res)
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
	rr := r.processZMembers(catalog, zs)
	rr.RemoveGen = removeGen
	return snap, rr
}

// parseListReply unpacks the raw {snapValue|false, removeGen, [member,
// score, ...]} script reply. Scores arrive as Redis reply strings ("%.17g"),
// which strconv.ParseFloat round-trips to the exact stored double — the same
// fidelity go-redis gives ZRangeByScoreWithScores, so DecodeDeltaMember's
// score-lockstep check keeps holding.
func parseListReply(res any) (string, string, []redis.Z, error) {
	arr, ok := res.([]any)
	if !ok || len(arr) != 3 {
		return "", "", nil, fmt.Errorf("unexpected list reply: %T", res)
	}
	rawSnap, _ := arr[0].(string) // Lua false → nil → not a string → ""
	removeGen, ok := arr[1].(string)
	if !ok {
		return "", "", nil, fmt.Errorf("unexpected remove-gen reply: %T", arr[1])
	}
	flat, ok := arr[2].([]any)
	if !ok {
		return "", "", nil, fmt.Errorf("unexpected list deltas reply: %T", arr[2])
	}
	if len(flat)%2 != 0 {
		return "", "", nil, fmt.Errorf("odd WITHSCORES reply length: %d", len(flat))
	}
	zs := make([]redis.Z, 0, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		member, ok1 := flat[i].(string)
		scoreStr, ok2 := flat[i+1].(string)
		if !ok1 || !ok2 {
			return "", "", nil, fmt.Errorf("unexpected member/score types: %T/%T", flat[i], flat[i+1])
		}
		score, perr := strconv.ParseFloat(scoreStr, 64)
		if perr != nil {
			return "", "", nil, fmt.Errorf("invalid score %q: %w", scoreStr, perr)
		}
		zs = append(zs, redis.Z{Member: member, Score: score})
	}
	return rawSnap, removeGen, zs, nil
}

// RemoveGen returns the catalog's current removal generation ("0" if no
// delta was ever removed). Sample write-backs recheck it against the
// generation captured with their ListResult: a loader computing from a list
// taken BEFORE a removal must not cache its result after it.
func (r *Reader) RemoveGen(ctx context.Context, catalog string) (string, error) {
	gen, err := r.rdb.HGet(ctx, r.MakeSnapsHashKey(), catalog+":rg").Result()
	if err == redis.Nil {
		return "0", nil
	}
	if err != nil {
		return "", err
	}
	return gen, nil
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

	cmds := r.evalListPipelined(ctx, catalogs, false)
	// RunScript's fallback cannot fire inside a pipeline (command errors
	// surface only at Exec), so it is mirrored here with the same predicate:
	// re-run the whole pipeline with full-body EVAL, which executes AND
	// re-caches the script in one step — no SCRIPT LOAD (or even EVALSHA)
	// permission required. Rare — a cold script cache, or an ACL that denies
	// EVALSHA; listScript is read-only, so re-running is always safe.
	for _, cmd := range cmds {
		if needsEvalFallback(cmd.Err()) {
			cmds = r.evalListPipelined(ctx, catalogs, true)
			break
		}
	}

	for c, cmd := range cmds {
		out[c] = &BatchListResult{}
		res, err := cmd.Result()
		if err != nil {
			out[c].ReadResult = &ReadIndexResult{Catalog: c, Err: fmt.Errorf("list eval: %w", err)}
			continue
		}
		out[c].Snap, out[c].ReadResult = r.parseListResult(c, res)
	}
	return out
}

// evalListPipelined queues one listScript call per catalog on a single
// pipeline and executes it. With fullBody false it uses EVALSHA (only the
// 40-byte SHA travels per catalog); with fullBody true it uses EVAL — the
// cold-cache retry path, which also re-caches the script server-side. When
// SHA-1 is unavailable (fips140=only) it is EVAL-only, mirroring RunScript.
func (r *Reader) evalListPipelined(ctx context.Context, catalogs []string, fullBody bool) map[string]*redis.Cmd {
	compiled := luaList.compiled()
	if compiled == nil {
		fullBody = true
	}
	pipe := r.rdb.Pipeline()
	cmds := make(map[string]*redis.Cmd, len(catalogs))
	for _, c := range catalogs {
		keys := []string{r.MakeSnapsHashKey(), r.MakeDeltaZsetKey(c)}
		if fullBody {
			cmds[c] = pipe.Eval(ctx, luaList.src, keys, c)
		} else {
			cmds[c] = compiled.EvalSha(ctx, pipe, keys, c)
		}
	}
	pipe.Exec(ctx)
	return cmds
}

// processZMembers parses zset entries into deltas. V3 has no pending
// state — every member must be a delta member.
func (r *Reader) processZMembers(catalog string, zs []redis.Z) *ReadIndexResult {
	entries := make([]DeltaInfo, 0, len(zs))
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
	r.syncClock()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			r.syncClock()
		}
	}
}

func (r *Reader) syncClock() {
	if ts, err := r.serverUnix(context.Background()); err == nil {
		r.clock.Store(&clockSync{redisUnix: ts, at: time.Now()})
	}
}

// NowUnix returns Lake's notion of the current time in unix seconds: the
// Redis server clock, re-anchored every 5s and extrapolated with the local
// MONOTONIC clock in between. It therefore always advances — across sync
// failures and after Close — while staying pinned to the Redis clock's
// offset. Before the first successful sync it is the local clock.
func (r *Reader) NowUnix() int64 {
	if c := r.clock.Load(); c != nil {
		return c.redisUnix + int64(time.Since(c.at)/time.Second)
	}
	return time.Now().Unix()
}

func (r *Reader) serverUnix(ctx context.Context) (int64, error) {
	t, err := r.rdb.Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	return t.Unix(), nil
}
