package index

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/encode"
)

// getTimeSeqIDAndPreCommitScript atomically allocates a TimeSeqID,
// pre-commits a pending delta member, and returns
// {timestamp, seqid, member}.
//
// The seqid counter is namespaced by ARGV[3] (deployment prefix /
// lake.setting Name) so multiple deployments sharing one Redis under
// different Names get independent 999,999/sec budgets.
const getTimeSeqIDAndPreCommitScript = `
local catalog, zaddKey = KEYS[1], KEYS[2]
local fieldPath, mergeType, prefix = ARGV[1], ARGV[2], ARGV[3]

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
local member = "pending|delta|" .. mergeType .. "|" .. fieldPath .. "|" .. tsSeq
local score  = tonumber(ts) + (tonumber(seqid) / 1000000.0)

redis.call("ZADD", zaddKey, score, member)
return {tonumber(ts), seqid, member}
`

// commitScript atomically swaps a pending delta member for its committed form.
const commitScript = `
local key = KEYS[1]
redis.call("ZADD", key, tonumber(ARGV[3]), ARGV[2])
redis.call("ZREM", key, ARGV[1])
return "OK"
`

// GetTimeSeqIDAndPreCommit allocates a TimeSeqID and writes the
// pending delta member. Returns the assigned tsSeq and the pending
// member string used by Commit / Rollback.
func (w *Writer) GetTimeSeqIDAndPreCommit(ctx context.Context, catalog, fieldPath string, mergeType MergeType) (TimeSeqID, string, error) {
	if w.prefix == "" {
		return TimeSeqID{}, "", fmt.Errorf("writer prefix not set; call SetPrefix")
	}
	res, err := w.rdb.Eval(ctx, getTimeSeqIDAndPreCommitScript,
		[]string{encode.EncodeRedisCatalogName(catalog), w.MakeDeltaZsetKey(catalog)},
		fieldPath, int(mergeType), w.prefix,
	).Result()
	if err != nil {
		return TimeSeqID{}, "", fmt.Errorf("precommit eval: %w", err)
	}
	arr, ok := res.([]any)
	if !ok || len(arr) != 3 {
		return TimeSeqID{}, "", fmt.Errorf("unexpected precommit result: %v", res)
	}
	ts, ok1 := arr[0].(int64)
	seq, ok2 := arr[1].(int64)
	member, ok3 := arr[2].(string)
	if !ok1 || !ok2 || !ok3 {
		return TimeSeqID{}, "", fmt.Errorf("unexpected precommit types: %T,%T,%T", arr[0], arr[1], arr[2])
	}
	return TimeSeqID{Timestamp: ts, SeqID: seq}, member, nil
}

// Rollback removes a pending member. Used when storage Put fails.
func (w *Writer) Rollback(ctx context.Context, catalog, pendingMember string) error {
	return w.rdb.ZRem(ctx, w.MakeDeltaZsetKey(catalog), pendingMember).Err()
}

// Commit atomically swaps pending → committed.
func (w *Writer) Commit(ctx context.Context, catalog, pendingMember, committedMember string, score float64) error {
	_, err := w.rdb.Eval(ctx, commitScript,
		[]string{w.MakeDeltaZsetKey(catalog)},
		pendingMember, committedMember, score,
	).Result()
	return err
}
