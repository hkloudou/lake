package index

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/encode"
	"github.com/redis/go-redis/v9"
)

// notifyScript atomically allocates a TimeSeqID and adds the committed delta
// member in a single ZADD. tsSeq is allocated only when notify fires (after
// the client's upload has succeeded), so a slow / aborted upload never appears
// in the index — no pending phase, no rollback.
//
// The member is the JSON array [mergeType, fieldPath, tsSeq, uri], assembled
// here via cjson — this script is the single authoritative encoder. The uri
// (provider://bucket/path) fully locates the body, so reads need no
// key-derivation knowledge.
//
// The seqid counter is namespaced by prefix (deployment Name) + catalog + ts,
// giving each catalog an independent 999,999/sec budget that resets each second.
const notifyScript = `
local catalog, zaddKey = KEYS[1], KEYS[2]
local fieldPath, mergeType, prefix, uri = ARGV[1], ARGV[2], ARGV[3], ARGV[4]

local ts = redis.call("TIME")[1]
local seqKey = prefix .. ":seqid:" .. catalog .. ":" .. ts
if redis.call("SETNX", seqKey, "0") == 1 then
  redis.call("EXPIRE", seqKey, 5)
end
local seqid = redis.call("INCR", seqKey)
if seqid > 999999 then
  return redis.error_reply("seqid overflow: " .. seqid .. " > 999999 (max writes/sec)")
end

local tsSeq  = ts .. "_" .. seqid
local member = cjson.encode({tonumber(mergeType), fieldPath, tsSeq, uri})
-- score MUST stay bit-identical to TimeSeqID.Score() in timeseqid.go: the read
-- path recomputes it and DecodeDeltaMember rejects a mismatch.
local score  = tonumber(ts) + (tonumber(seqid) / 1000000.0)

redis.call("ZADD", zaddKey, score, member)
return {tonumber(ts), seqid, member}
`

// luaNotify dispatches notifyScript by SHA — the hottest write-path script,
// so its body travels once per server, not once per write.
var luaNotify = redis.NewScript(notifyScript)

// Notify allocates a TimeSeqID for an already-uploaded delta and commits it to
// the Redis index. uri is the storage locator (provider://bucket/path) the
// client uploaded to; it is embedded in the member so reads resolve the body
// without any storage-key knowledge.
func (w *Writer) Notify(ctx context.Context, catalog, fieldPath string, mergeType MergeType, uri string) (TimeSeqID, string, error) {
	if w.prefix == "" {
		return TimeSeqID{}, "", fmt.Errorf("writer prefix not set; call SetPrefix")
	}
	res, err := luaNotify.Run(ctx, w.rdb,
		[]string{encode.EncodeRedisCatalogName(catalog), w.MakeDeltaZsetKey(catalog)},
		fieldPath, int(mergeType), w.prefix, uri,
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
