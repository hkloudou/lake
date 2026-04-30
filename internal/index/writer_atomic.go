package index

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/encode"
)

// notifyScript atomically allocates a TimeSeqID and adds the committed
// delta member in a single ZADD. V3 dispenses with the v2 pending →
// committed swap entirely: tsSeq is allocated only when notify fires
// (after the OSS upload has succeeded), so a slow / aborted upload
// never appears in the index — no pending phase, no 120s timeout, no
// rollback API.
//
// The seqid counter is namespaced by ARGV[3] (deployment Name) so that
// multiple deployments sharing one Redis under different Names get
// independent 999,999/sec budgets.
const notifyScript = `
local catalog, zaddKey = KEYS[1], KEYS[2]
local fieldPath, mergeType, prefix, uuid = ARGV[1], ARGV[2], ARGV[3], ARGV[4]

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
local member = "delta|" .. mergeType .. "|" .. fieldPath .. "|" .. tsSeq .. "|" .. uuid
local score  = tonumber(ts) + (tonumber(seqid) / 1000000.0)

redis.call("ZADD", zaddKey, score, member)
return {tonumber(ts), seqid, member}
`

// Notify allocates a TimeSeqID for an already-uploaded delta and
// commits it to the Redis index. The uuid is the OSS object identifier
// the client used at upload time; it is embedded in the delta member
// so reads can resolve the storage key.
func (w *Writer) Notify(ctx context.Context, catalog, fieldPath string, mergeType MergeType, uuid string) (TimeSeqID, string, error) {
	if w.prefix == "" {
		return TimeSeqID{}, "", fmt.Errorf("writer prefix not set; call SetPrefix")
	}
	res, err := w.rdb.Eval(ctx, notifyScript,
		[]string{encode.EncodeRedisCatalogName(catalog), w.MakeDeltaZsetKey(catalog)},
		fieldPath, int(mergeType), w.prefix, uuid,
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
