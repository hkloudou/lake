package index

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v2/internal/encode"
)

// Lua script to atomically generate TimeSeqID and pre-commit to Redis
// Returns: {timestamp, seqid}
// Side effect: ZADD with pending member
const getTimeSeqIDAndPreCommitScript = `
-- KEYS[1]: base64 encoded catalog name (for seqid isolation)
-- KEYS[2]: Redis ZADD key (e.g., "oss:mylake:delta:users")
-- ARGV[1]: encoded field
-- ARGV[2]: mergeType

local catalog = KEYS[1]
local zaddKey = KEYS[2]
local field = ARGV[1]
local mergeType = ARGV[2]

-- Generate timestamp + seqid
local timeResult = redis.call("TIME")
local timestamp = timeResult[1]
local seqKey = "lake:seqid:" .. catalog .. ":" .. timestamp

local setResult = redis.call("SETNX", seqKey, "0")
if setResult == 1 then
    redis.call("EXPIRE", seqKey, 5)
end

local seqid = redis.call("INCR", seqKey)

-- Check seqid limit (max 999,999 for %.6f precision)
if seqid > 999999 then
    return redis.error_reply("seqid overflow: " .. seqid .. " > 999999 (max writes per second reached)")
end

-- Build pending member
local tsSeq = timestamp .. "_" .. seqid
local member = "pending|delta|" .. field .. "|" .. tsSeq .. "|" .. mergeType
local score = tonumber(timestamp) + (tonumber(seqid) / 1000000.0)

-- Pre-commit to Redis (pending state)
redis.call("ZADD", zaddKey, score, member)

return {timestamp, seqid}
`

// Lua script to commit: remove pending, add committed
// This is atomic
const commitScript = `
-- KEYS[1]: Redis ZADD key
-- ARGV[1]: pending member
-- ARGV[2]: committed member  
-- ARGV[3]: score

local key = KEYS[1]
local pendingMember = ARGV[1]
local committedMember = ARGV[2]
local score = tonumber(ARGV[3])

-- Atomic: remove pending, add committed
redis.call("ZREM", key, pendingMember)
redis.call("ZADD", key, score, committedMember)

return "OK"
`

// GetTimeSeqIDAndPreCommit atomically generates TimeSeqID and pre-commits to Redis
// Returns TimeSeqID and pending member string
func (w *Writer) GetTimeSeqIDAndPreCommit(ctx context.Context, catalog, field string, mergeType MergeType) (TimeSeqID, string, error) {
	encodedCatalog := encodeCatalog(catalog)
	zaddKey := w.makeCatalogKey(catalog)

	// Encode field for member
	encodedField := encodeField(field)

	// Execute Lua script
	result, err := w.rdb.Eval(ctx, getTimeSeqIDAndPreCommitScript,
		[]string{encodedCatalog, zaddKey},
		encodedField, int(mergeType)).Result()

	if err != nil {
		return TimeSeqID{}, "", fmt.Errorf("failed to get timeseq and precommit: %w", err)
	}

	// Parse result
	arr, ok := result.([]interface{})
	if !ok || len(arr) != 2 {
		return TimeSeqID{}, "", fmt.Errorf("unexpected result format: %v", result)
	}

	tsStr := arr[0].(string)
	seqid := arr[1].(int64)

	var timestamp int64
	fmt.Sscanf(tsStr, "%d", &timestamp)

	tsSeq := TimeSeqID{
		Timestamp: timestamp,
		SeqID:     seqid,
	}

	// Build pending member string
	pendingMember := fmt.Sprintf("pending|delta|%s|%s|%d", encodedField, tsSeq.String(), mergeType)

	return tsSeq, pendingMember, nil
}

// Commit atomically commits a pending write
func (w *Writer) Commit(ctx context.Context, catalog, pendingMember, committedMember string, score float64) error {
	zaddKey := w.makeCatalogKey(catalog)

	_, err := w.rdb.Eval(ctx, commitScript,
		[]string{zaddKey},
		pendingMember, committedMember, score).Result()

	return err
}

// Helper functions
func encodeCatalog(catalog string) string {
	return encode.EncodeRedisCatalogName(catalog)
}

func encodeField(field string) string {
	return encode.EncodeRedisCatalogName(field)
}
