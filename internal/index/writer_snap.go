package index

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Writer writes Redis index entries (delta zset, snap hash, seqid).
type Writer struct {
	rdb *redis.Client
	indexIO
}

// NewWriter returns a Writer; SetPrefix must be called before use.
func NewWriter(rdb *redis.Client) *Writer {
	return &Writer{rdb: rdb}
}

// addSnapScript upserts the catalog's snap entry only when the new stop is
// strictly newer than the stored one AND the snapshot was computed from the
// catalog's current removal generation.
//
// Monotonicity: snapshot saves are async and may race across processes;
// without the guard, a slow save computed at an older stop could land after
// a newer one and regress the snap pointer (correct but wasteful). A stored
// value snap_score rejects (see snapScoreLua, encoding.go) is treated as
// absent and overwritten (self-heal).
//
// Removal barrier: a snapshot bakes in every delta the read listed. If a
// RemoveDelta interleaved between that read and this save, the snapshot
// would resurrect the removed write — permanently, since later reads prefer
// the snap pointer. RemoveDelta bumps "<catalog>:rg"; a save whose
// generation (captured atomically by listScript) no longer matches is
// dropped. The next read starts from post-removal state and snapshots fine.
//
// Returns 1 when the entry was written, 0 when it was dropped/kept.
const addSnapScript = snapScoreLua + `
local cur = redis.call("HGET", KEYS[1], ARGV[1])
if cur then
  local score = snap_score(cur)
  if score and score >= tonumber(ARGV[3]) then
    return 0
  end
end
if (redis.call("HGET", KEYS[1], ARGV[1] .. ":rg") or "0") ~= ARGV[4] then
  return 0
end
redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
return 1
`

// luaAddSnap / luaCompactDeltas dispatch their scripts by SHA (EVALSHA with
// EVAL fallback), so the shared snapScoreLua prelude is not re-sent per call.
var (
	luaAddSnap       = redis.NewScript(addSnapScript)
	luaCompactDeltas = redis.NewScript(compactDeltasScript)
)

// AddSnap upserts the catalog's snap entry in "<prefix>:s" as [tsSeq, uri],
// but only monotonically, and only when removeGen still matches the
// catalog's removal generation (see addSnapScript). Refusals are silent
// no-ops; the freshly written snap object is left orphan in storage, like
// any superseded snap — V3 contract.
func (w *Writer) AddSnap(ctx context.Context, catalog string, stopTsSeq TimeSeqID, uri, removeGen string) error {
	val, err := EncodeSnapValue(stopTsSeq, uri)
	if err != nil {
		return err
	}
	if removeGen == "" {
		removeGen = "0"
	}
	return RunScript(ctx, w.rdb, luaAddSnap,
		[]string{w.MakeSnapsHashKey()},
		catalog, val, stopTsSeq.Score(), removeGen,
	).Err()
}

// compactDeltasScript removes every delta zset entry the catalog's current
// snapshot has absorbed: score ≤ the snap's stop score, inclusive — the read
// path fetches deltas strictly AFTER the stop (listScript, reader.go), so an
// absorbed delta can never be read again. Missing or undecodable snap → 0
// (never trim on a pointer the Go reader would reject). Returns the number
// of entries removed.
const compactDeltasScript = snapScoreLua + `
local cur = redis.call("HGET", KEYS[1], ARGV[1])
if not cur then
  return 0
end
local score = snap_score(cur)
if not score then
  return 0
end
return redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", string.format("%.6f", score))
`

// CompactDeltas trims the catalog's delta zset up to (and including) the
// current snap stop, atomically with reading the snap pointer. Only index
// entries are removed — delta objects in storage are untouched.
func (w *Writer) CompactDeltas(ctx context.Context, catalog string) (int64, error) {
	res, err := RunScript(ctx, w.rdb, luaCompactDeltas,
		[]string{w.MakeSnapsHashKey(), w.MakeDeltaZsetKey(catalog)},
		catalog,
	).Result()
	if err != nil {
		return 0, fmt.Errorf("compact eval: %w", err)
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected compact result: %v", res)
	}
	return n, nil
}
