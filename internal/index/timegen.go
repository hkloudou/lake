package index

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// TimeSeqID represents a timestamp + sequence ID pair
type TimeSeqID struct {
	Timestamp int64 // Unix timestamp in seconds
	SeqID     int64 // Sequence ID within that second
}

// Score returns the score value for Redis ZADD
// Uses float64: timestamp as integer part, seqid as fractional part
// Example: ts=1700000000, seqid=123 -> 1700000000.000123
func (t TimeSeqID) Score() float64 {
	// Scale seqid to fractional part (supports up to 999999 ops/sec)
	return float64(t.Timestamp) + float64(t.SeqID)/1000000.0
}

// String returns the string representation used in filenames
// Format: {timestamp}_{seqid}
func (t TimeSeqID) String() string {
	return fmt.Sprintf("%d_%d", t.Timestamp, t.SeqID)
}

// ParseTimeSeqID parses a TimeSeqID from string format
func ParseTimeSeqID(s string) (TimeSeqID, error) {
	var ts, seqid int64
	_, err := fmt.Sscanf(s, "%d_%d", &ts, &seqid)
	if err != nil {
		return TimeSeqID{}, fmt.Errorf("invalid TimeSeqID format: %s", s)
	}
	return TimeSeqID{Timestamp: ts, SeqID: seqid}, nil
}

// TimeGenerator generates unique timestamp + sequence ID pairs using Redis
type TimeGenerator struct {
	rdb *redis.Client
}

// NewTimeGenerator creates a new time generator
func NewTimeGenerator(rdb *redis.Client) *TimeGenerator {
	return &TimeGenerator{rdb: rdb}
}

// Lua script to generate timestamp + seqid atomically with catalog isolation
// KEYS[1]: base64 encoded catalog name
// Returns: {timestamp, seqid}
const timeGenScript = `
local catalog = KEYS[1]
local timeResult = redis.call("TIME")
local timestamp = timeResult[1]

-- Sequence key includes catalog for isolation
local seqKey = "lake:seqid:" .. catalog .. ":" .. timestamp

-- Initialize sequence counter if not exists (expires in 5 seconds)
local setResult = redis.call("SETNX", seqKey, "0")
if setResult == 1 then
    redis.call("EXPIRE", seqKey, 5)
end

-- Increment and return
local seqid = redis.call("INCR", seqKey)

return {timestamp, seqid}
`

// Generate generates a unique TimeSeqID using Redis TIME + INCR
// catalog: catalog name for seqid isolation (will be base64 URL encoded)
func (g *TimeGenerator) Generate(ctx context.Context, catalog string) (TimeSeqID, error) {
	// Base64 URL encode the catalog name to avoid special characters
	encodedCatalog := base64.URLEncoding.EncodeToString([]byte(catalog))

	// Pass encoded catalog as KEYS[1]
	result, err := g.rdb.Eval(ctx, timeGenScript, []string{encodedCatalog}).Result()
	if err != nil {
		return TimeSeqID{}, fmt.Errorf("failed to generate time+seqid: %w", err)
	}

	// Parse result
	arr, ok := result.([]interface{})
	if !ok || len(arr) != 2 {
		return TimeSeqID{}, fmt.Errorf("unexpected result format: %v", result)
	}

	// Redis TIME returns strings
	tsStr, ok := arr[0].(string)
	if !ok {
		return TimeSeqID{}, fmt.Errorf("invalid timestamp type: %T", arr[0])
	}

	seqid, ok := arr[1].(int64)
	if !ok {
		return TimeSeqID{}, fmt.Errorf("invalid seqid type: %T", arr[1])
	}

	var timestamp int64
	_, err = fmt.Sscanf(tsStr, "%d", &timestamp)
	if err != nil {
		return TimeSeqID{}, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	return TimeSeqID{
		Timestamp: timestamp,
		SeqID:     seqid,
	}, nil
}
