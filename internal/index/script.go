package index

import "github.com/redis/go-redis/v9"

// Every index mutation and the atomic read are Lua scripts, dispatched by
// go-redis's Script.Run (EVALSHA with EVAL fallback on a cold script cache).
// They share snapScoreLua, the Lua mirror of DecodeSnapValue (encoding.go).

// notifyScript allocates a TimeSeqID and appends the delta member in one step.
//
// Allocation is monotonic per catalog: the issued (ts, seq) is floored by the
// allocator key (last issued pair), the snap stop and the newest delta, so a
// backwards Redis clock (NTP step, failover) can never mint a duplicate or a
// write that sorts at-or-below the snapshot bound, which reads would skip
// (and an operator's index trim would delete). When the 999,999 seq budget of a second is exhausted
// allocation spills into the next second.
//
// Notify is idempotent per uri for the dedup TTL: the committed member is
// remembered under KEYS[4] and a repeat call returns it without allocating
// again. Handles travel through clients that retry on timeouts; a naive
// re-append would give the retried body a NEWER tsSeq than writes that landed
// in between and silently overwrite them. The record outlives RemoveDelta, so
// a retry of a removed write cannot resurrect it.
//
// KEYS[1] = delta zset, KEYS[2] = snaps hash, KEYS[3] = allocator key,
// KEYS[4] = dedup key; ARGV[1] = fieldPath, ARGV[2] = mergeType, ARGV[3] = uri,
// ARGV[4] = catalog, ARGV[5] = dedup TTL seconds. Returns {ts, seq}.
const notifyScript = snapScoreLua + `
local zsetKey, snapsKey, allocKey, dedupKey = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local fieldPath, mergeType, uri, catalog, dedupTTL = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5]

local prev = redis.call("GET", dedupKey)
if prev then
  local pts, pseq = parse_tsseq(cjson.decode(prev)[3])
  return {pts, pseq}
end

local ts = tonumber(redis.call("TIME")[1])
local seq = 0
local function bump(bts, bseq)
  if bts and (bts > ts or (bts == ts and bseq > seq)) then
    ts, seq = bts, bseq
  end
end

local last = redis.call("GET", allocKey)
if last then
  bump(parse_tsseq(last))
end
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

seq = seq + 1
if seq > 999999 then
  ts, seq = ts + 1, 1
end
if ts > 8589934591 then
  return redis.error_reply("timestamp " .. ts .. " beyond score-safe cap (server clock misconfigured?)")
end
local tsSeq = ts .. "_" .. seq
redis.call("SET", allocKey, tsSeq, "EX", 604800)

-- score MUST stay bit-identical to TimeSeqID.Score(): the read path
-- recomputes it and DecodeDeltaMember rejects a mismatch.
local member = cjson.encode({tonumber(mergeType), fieldPath, tsSeq, uri})
redis.call("ZADD", zsetKey, ts + (seq / 1000000.0), member)
redis.call("SET", dedupKey, member, "EX", dedupTTL)
return {ts, seq}
`

// listScript reads the snap pointer, the removal generation and the deltas
// past the snap in ONE atomic step — an operator trimming absorbed entries
// (ZREMRANGEBYSCORE up to the snap stop) could otherwise pair a non-atomic
// HGET→ZRANGE's old pointer with an already-trimmed log.
// KEYS[1] = snaps hash, KEYS[2] = delta zset; ARGV[1] = catalog.
// Returns {snapValue|false, removeGen, [member, score, ...]}; scores travel as
// Redis reply strings so no Lua number formatting touches them.
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

// addSnapScript upserts the snap entry only when the new stop is strictly
// newer than the stored one (async saves may race across processes) AND the
// snapshot was computed under the catalog's current removal generation — a
// RemoveDelta that interleaved between the read and this save would otherwise
// be undone by a snapshot that still contains the removed write.
// KEYS[1] = snaps hash; ARGV[1] = catalog, ARGV[2] = value, ARGV[3] = stop
// score, ARGV[4] = removal generation.
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

// removeDeltaScript removes the one entry whose embedded tsSeq matches, and
// bumps the catalog's removal generation first — Lua does not roll back, so
// a failing HINCRBY (hand-corrupted field) must fail while the delta is still
// present. KEYS[1] = delta zset, KEYS[2] = snaps hash; ARGV[1] = score,
// ARGV[2] = tsSeq, ARGV[3] = catalog. Returns 1 if removed.
const removeDeltaScript = `
for _, m in ipairs(redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[1])) do
  local ok, arr = pcall(cjson.decode, m)
  if ok and type(arr) == "table" and arr[3] == ARGV[2] then
    redis.call("HINCRBY", KEYS[2], ARGV[3] .. ":rg", 1)
    redis.call("ZREM", KEYS[1], m)
    return 1
  end
end
return 0
`

// deleteCatalogScript drops a catalog's delta log, snap pointer and allocator
// atomically, bumping the removal generation first (same reasoning as
// removeDeltaScript; and bumped rather than deleted so a read in flight
// cannot AddSnap its pre-delete snapshot back). A catalog with no state
// returns 0 and mints nothing. KEYS[1] = snaps hash, KEYS[2] = delta zset,
// KEYS[3] = allocator key; ARGV[1] = catalog.
const deleteCatalogScript = `
if redis.call("EXISTS", KEYS[2], KEYS[3]) == 0 and redis.call("HEXISTS", KEYS[1], ARGV[1]) == 0 then
  return 0
end
redis.call("HINCRBY", KEYS[1], ARGV[1] .. ":rg", 1)
redis.call("DEL", KEYS[2], KEYS[3])
redis.call("HDEL", KEYS[1], ARGV[1])
return 1
`

var (
	luaNotify        = redis.NewScript(notifyScript)
	luaList          = redis.NewScript(listScript)
	luaAddSnap       = redis.NewScript(addSnapScript)
	luaRemoveDelta   = redis.NewScript(removeDeltaScript)
	luaDeleteCatalog = redis.NewScript(deleteCatalogScript)
)
