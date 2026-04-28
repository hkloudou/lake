package index

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v3/internal/encode"
)

// getTimeSeqIDAndPreCommitScript atomically generates a (timestamp, seqid)
// pair, ZADDs a pending delta member, and returns both the pair and the
// pending member string.
//
// The seqid counter is namespaced by ARGV[3] (the deployment's Redis
// prefix, i.e. lake.setting Name). This makes the seqid space tenant-
// scoped: two Lake deployments sharing the same Redis but using different
// Names get independent 999,999/sec budgets per (catalog, second).
const getTimeSeqIDAndPreCommitScript = `
-- KEYS[1]: encoded catalog name
-- KEYS[2]: delta zset key
-- ARGV[1]: fieldPath
-- ARGV[2]: mergeType
-- ARGV[3]: prefix (deployment-level namespace)

local catalog = KEYS[1]
local zaddKey = KEYS[2]
local fieldPath = ARGV[1]
local mergeType = ARGV[2]
local prefix = ARGV[3]

local timestamp = redis.call("TIME")[1]
local seqKey = prefix .. ":seqid:" .. catalog .. ":" .. timestamp

local setResult = redis.call("SETNX", seqKey, "0")
if setResult == 1 then
    redis.call("EXPIRE", seqKey, 5)
end

local seqid = redis.call("INCR", seqKey)

if seqid > 999999 then
    return redis.error_reply("seqid overflow: " .. seqid .. " > 999999 (max writes per second reached)")
end

local tsSeq = timestamp .. "_" .. seqid
local member = "pending|delta|" .. mergeType .. "|" .. fieldPath .. "|" .. tsSeq
local score = tonumber(timestamp) + (tonumber(seqid) / 1000000.0)

redis.call("ZADD", zaddKey, score, member)

return {tonumber(timestamp), seqid, member}
`

// commitScript atomically removes the pending member and adds the committed one.
const commitScript = `
-- KEYS[1]: delta zset key
-- ARGV[1]: pending member
-- ARGV[2]: committed member
-- ARGV[3]: score

local key = KEYS[1]
local pendingMember = ARGV[1]
local committedMember = ARGV[2]
local score = tonumber(ARGV[3])

redis.call("ZADD", key, score, committedMember)
redis.call("ZREM", key, pendingMember)

return "OK"
`

// GetTimeSeqIDAndPreCommit atomically generates TimeSeqID and pre-commits to Redis.
// Returns TimeSeqID and pending member string.
//
// The seqid counter is namespaced by w.prefix (deployment Name) so that
// multiple Lake deployments sharing one Redis do not contend for the same
// per-second seqid budget.
func (w *Writer) GetTimeSeqIDAndPreCommit(ctx context.Context, catalog, fieldPath string, mergeType MergeType) (TimeSeqID, string, error) {
	if w.prefix == "" {
		return TimeSeqID{}, "", fmt.Errorf("writer prefix not set; call SetPrefix before write operations")
	}
	encodedCatalog := encode.EncodeRedisCatalogName(catalog)
	zaddKey := w.MakeDeltaZsetKey(catalog)

	result, err := w.rdb.Eval(ctx, getTimeSeqIDAndPreCommitScript,
		[]string{encodedCatalog, zaddKey},
		fieldPath, int(mergeType), w.prefix).Result()

	if err != nil {
		return TimeSeqID{}, "", fmt.Errorf("failed to get timeseq and precommit: %w", err)
	}

	arr, ok := result.([]interface{})
	if !ok || len(arr) != 3 {
		return TimeSeqID{}, "", fmt.Errorf("unexpected result format: %v", result)
	}

	timestamp, ok := arr[0].(int64)
	if !ok {
		return TimeSeqID{}, "", fmt.Errorf("invalid timestamp type: %T", arr[0])
	}
	seqid, ok := arr[1].(int64)
	if !ok {
		return TimeSeqID{}, "", fmt.Errorf("invalid seqid type: %T", arr[1])
	}
	pendingMember, ok := arr[2].(string)
	if !ok {
		return TimeSeqID{}, "", fmt.Errorf("invalid pending member type: %T", arr[2])
	}

	return TimeSeqID{Timestamp: timestamp, SeqID: seqid}, pendingMember, nil
}

// Rollback removes a pending member from Redis (used when storage write fails)
func (w *Writer) Rollback(ctx context.Context, catalog, pendingMember string) error {
	zaddKey := w.MakeDeltaZsetKey(catalog)
	return w.rdb.ZRem(ctx, zaddKey, pendingMember).Err()
}

// Commit atomically commits a pending write.
func (w *Writer) Commit(ctx context.Context, catalog, pendingMember, committedMember string, score float64) error {
	zaddKey := w.MakeDeltaZsetKey(catalog)
	_, err := w.rdb.Eval(ctx, commitScript,
		[]string{zaddKey},
		pendingMember, committedMember, score).Result()
	return err
}
