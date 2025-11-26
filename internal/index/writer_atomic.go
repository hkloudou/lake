package index

import (
	"context"
	"fmt"

	"github.com/hkloudou/lake/v2/internal/encode"
	"github.com/hkloudou/lake/v2/internal/trace"
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
local member = "pending|delta|" .. mergeType .. "|" .. field
local score = tonumber(timestamp) + (tonumber(seqid) / 1000000.0)

-- Pre-commit to Redis (pending state)
redis.call("ZADD", zaddKey, score, member)

return {tonumber(timestamp), seqid, member}
`

// Lua script to commit: remove pending, add committed
// This is atomic
const commitScript = `
-- KEYS[1]: Redis ZADD key
-- KEYS[2]: Redis meta key

-- ARGV[1]: pending member
-- ARGV[2]: committed member  
-- ARGV[3]: score

local key = KEYS[1]
local metaKey = KEYS[2]
local pendingMember = ARGV[1]
local committedMember = ARGV[2]
local score = tonumber(ARGV[3])

-- Atomic: remove pending, add committed
redis.call("ZADD", key, score, committedMember)
redis.call("ZREM", key, pendingMember)

return "OK"
`

/*
DELETED CODE:


-- ARGV[4]: updatedMap
local updatedMap = cjson.decode(ARGV[4])

--------------------------------
-- Update meta map with updatedMap
local meta = redis.call("GET", metaKey)
local metaMap = {}
if meta then
    metaMap = cjson.decode(meta)
end

-- Merge updatedMap into metaMap
for key, value in pairs(updatedMap) do
    metaMap[key] = value
end

-- Save updated meta
redis.call("SET", metaKey, cjson.encode(metaMap))

return "OK"
*/

// func (w *Writer) GetTimeUnix(ctx context.Context) (int64, error) {
// 	// encodedCatalog := encode.EncodeRedisCatalogName(catalog)
// 	// Execute Lua script
// 	result, err := w.rdb.Eval(ctx, `
// local timeResult = redis.call("TIME")
// local timestamp = timeResult[1]
// return tonumber(timestamp)`,
// 		[]string{},
// 	).Result()

// 	if err != nil {
// 		return 0, fmt.Errorf("failed to get timeseq and precommit: %w", err)
// 	}

// 	// Parse result
// 	timestamp, ok := result.(int64)
// 	if !ok {
// 		return 0, fmt.Errorf("invalid timestamp type: %T", result)
// 	}
// 	return timestamp, nil
// }

// GetTimeSeqIDAndPreCommit atomically generates TimeSeqID and pre-commits to Redis
// Returns TimeSeqID and pending member string
func (w *Writer) GetTimeSeqIDAndPreCommit(ctx context.Context, catalog, field string, mergeType MergeType) (TimeSeqID, string, error) {
	encodedCatalog := encode.EncodeRedisCatalogName(catalog)
	zaddKey := w.makeDeltaZsetKey(catalog)

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

	// var timestamp int64
	// fmt.Sscanf(tsStr, "%d", &timestamp)

	tsSeq := TimeSeqID{
		Timestamp: timestamp,
		SeqID:     seqid,
	}

	return tsSeq, pendingMember, nil
}

// Commit atomically commits a pending write
func (w *Writer) Commit(ctx context.Context, catalog, pendingMember, committedMember string, score float64) error {
	tr := trace.FromContext(ctx)
	tr.RecordSpan("Commit.Start")
	zaddKey := w.makeDeltaZsetKey(catalog)
	metaKey := w.makeMetaKey(catalog)
	// updatedMapJSON, err := json.Marshal(updatedMap)
	// if err != nil {
	// 	return fmt.Errorf("failed to marshal updated map: %w", err)
	// }
	tr.RecordSpan("Commit.MarshalUpdatedMap", map[string]any{
		// "updatedMap":      updatedMap,
		// "size":            len(updatedMapJSON),
		"zaddKey":         zaddKey,
		"metaKey":         metaKey,
		"pendingMember":   pendingMember,
		"committedMember": committedMember,
		"score":           fmt.Sprintf("%.6f", score),
		// "updatedMapJSON":  string(updatedMapJSON),
	})
	_, err := w.rdb.Eval(ctx, commitScript,
		[]string{zaddKey, metaKey},
		pendingMember, committedMember, score).Result()

	return err
}

// Helper functions
// func encodeCatalog(catalog string) string {
// 	return encode.EncodeRedisCatalogName(catalog)
// }

func encodeField(field string) string {
	return encode.EncodeRedisCatalogName(field)
}
