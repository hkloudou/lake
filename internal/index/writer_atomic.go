package index

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// notifyLua atomically allocates a TimeSeqID and adds the committed delta
// member in a single ZADD. tsSeq is allocated only when notify fires (after
// the client's upload has succeeded), so a slow / aborted upload never appears
// in the index — no pending phase, no rollback.
//
// Allocation is MONOTONIC per catalog, not just clock-driven. Redis TIME is
// gettimeofday-based and can step backwards (NTP step, failover to a replica
// whose host clock lags, VM restore). A naive per-second counter would then
// mint duplicate tsSeqs — or worse, scores at-or-below the snap stop, which
// listScript's exclusive-min range hides from every read and Compact then
// deletes: acknowledged writes silently lost. So the issued (ts, seq) is
// floored by three sources, and the write always sorts strictly after all of
// them:
//
//   - the allocator key (last issued pair, 7-day TTL — survives any
//     realistic clock step while the catalog is active);
//   - the catalog's snap stop (never issue at-or-below the snapshot bound);
//   - the newest existing delta (keeps order when the allocator expired
//     while old deltas remain).
//
// Within one second the seq budget is 999,999; when exhausted (or pinned by
// a backwards clock) allocation spills into the next second instead of
// failing — order is preserved and the wall clock catches up.
//
// The member is the JSON array [mergeType, fieldPath, tsSeq, uri], assembled
// here via cjson — this script is the single authoritative encoder. The uri
// (provider://bucket/path) fully locates the body, so reads need no
// key-derivation knowledge.
//
// KEYS[1] = delta zset, KEYS[2] = snaps hash, KEYS[3] = allocator key;
// ARGV[1] = fieldPath, ARGV[2] = mergeType, ARGV[3] = uri, ARGV[4] = catalog.
const notifyLua = `
local zsetKey, snapsKey, allocKey = KEYS[1], KEYS[2], KEYS[3]
local fieldPath, mergeType, uri, catalog = ARGV[1], ARGV[2], ARGV[3], ARGV[4]

-- (ts, seq) floor: the pair the new allocation must sort strictly after.
local ts = tonumber(redis.call("TIME")[1])
local seq = 0

local function bump(bts, bseq)
  if bts and (bts > ts or (bts == ts and bseq > seq)) then
    ts, seq = bts, bseq
  end
end

-- Mirror of ParseTimeSeqID (timeseqid.go): "ts_seq", no leading zeros,
-- ts within the score-safe cap, seq 1..999999. Returns nil on anything else
-- (including the "0_0" sentinel, which floors nothing).
local function parse_tsseq(s)
  local a, b = string.match(s, "^([1-9]%d*)_([1-9]%d?%d?%d?%d?%d?)$")
  if a and tonumber(a) <= 8589934591 then
    return tonumber(a), tonumber(b)
  end
  return nil
end

local floored = false
local last = redis.call("GET", allocKey)
if last then
  local lts, lseq = parse_tsseq(last)
  if lts then
    bump(lts, lseq)
    floored = true
  end
end
if not floored then
  -- Cold allocator (expired / first write) or an UNPARSEABLE one (corrupt /
  -- foreign value — it must degrade to the probes, not to bare TIME): every
  -- snap stop and delta was itself minted here and written back to the
  -- allocator, so a healthy allocator dominates both and the ~100% warm
  -- path skips the probes.
  local snap = redis.call("HGET", snapsKey, catalog)
  if snap then
    local ok, arr = pcall(cjson.decode, snap)
    if ok and type(arr) == "table" and type(arr[1]) == "string" then
      bump(parse_tsseq(arr[1]))
    end
  end
  local top = redis.call("ZREVRANGE", zsetKey, 0, 0)
  if top[1] then
    local ok, arr = pcall(cjson.decode, top[1])
    if ok and type(arr) == "table" and type(arr[3]) == "string" then
      bump(parse_tsseq(arr[3]))
    end
  end
end

seq = seq + 1
if seq > 999999 then
  ts, seq = ts + 1, 1
end
if ts > 8589934591 then
  -- Past MaxTimestamp the reader rejects the member (and the score cannot
  -- carry the seqid); minting it would wedge every read of the catalog.
  -- Reachable only via an absurdly future server clock.
  return redis.error_reply("timestamp " .. ts .. " beyond score-safe cap (server clock misconfigured?)")
end
local tsSeq = ts .. "_" .. seq
redis.call("SET", allocKey, tsSeq, "EX", 604800)

local member = cjson.encode({tonumber(mergeType), fieldPath, tsSeq, uri})
-- score MUST stay bit-identical to TimeSeqID.Score() in timeseqid.go: the read
-- path recomputes it and DecodeDeltaMember rejects a mismatch.
local score = ts + (seq / 1000000.0)

redis.call("ZADD", zsetKey, score, member)
return {ts, seq, member}
`

var notifyScript = redis.NewScript(notifyLua)

// Notify allocates a TimeSeqID for an already-uploaded delta and commits it to
// the Redis index. uri is the storage locator (provider://bucket/path) the
// client uploaded to; it is embedded in the member so reads resolve the body
// without any storage-key knowledge.
func (w *Writer) Notify(ctx context.Context, catalog, fieldPath string, mergeType MergeType, uri string) (TimeSeqID, string, error) {
	if w.prefix == "" {
		return TimeSeqID{}, "", fmt.Errorf("writer prefix not set; call SetPrefix")
	}
	res, err := notifyScript.Run(ctx, w.rdb,
		[]string{w.MakeDeltaZsetKey(catalog), w.MakeSnapsHashKey(), w.MakeSeqAllocKey(catalog)},
		fieldPath, int(mergeType), uri, catalog,
	).Result()
	if err != nil {
		return TimeSeqID{}, "", fmt.Errorf("notify eval: %w", err)
	}
	arr, ok := res.([]any)
	if !ok || len(arr) != 3 {
		return TimeSeqID{}, "", fmt.Errorf("unexpected notify result: %v", res)
	}
	ts, ok1 := arr[0].(int64)
	seq, ok2 := arr[1].(int64)
	member, ok3 := arr[2].(string)
	if !ok1 || !ok2 || !ok3 {
		return TimeSeqID{}, "", fmt.Errorf("unexpected notify types: %T,%T,%T", arr[0], arr[1], arr[2])
	}
	return TimeSeqID{Timestamp: ts, SeqID: seq}, member, nil
}
