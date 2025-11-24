package index

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/hkloudou/lake/v2/internal/encode"
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
func ParseTimeSeqID(s any) (TimeSeqID, error) {
	switch v := s.(type) {
	case string:
		// Try format: "timestamp_seqid"
		var ts, seqid int64
		_, err := fmt.Sscanf(v, "%d_%d", &ts, &seqid)
		if err == nil {
			return TimeSeqID{Timestamp: ts, SeqID: seqid}, nil
		}

		// Try format: "timestamp.seqid" (decimal with 1-6 digits, auto-padded to 6)
		// Check decimal point exists
		dotIndex := strings.Index(v, ".")
		if dotIndex == -1 {
			return TimeSeqID{}, fmt.Errorf("invalid format: score must have decimal point (format: timestamp.x to timestamp.xxxxxx)")
		}

		// Get decimal part and check length (1-6 digits allowed)
		decimalPart := v[dotIndex+1:]
		if len(decimalPart) < 1 || len(decimalPart) > 6 {
			return TimeSeqID{}, fmt.Errorf("invalid precision: score must have 1-6 decimal places, got %d decimal places", len(decimalPart))
		}

		// Parse the float
		var tsFloat float64
		_, err = fmt.Sscanf(v, "%f", &tsFloat)
		if err != nil {
			return TimeSeqID{}, fmt.Errorf("invalid TimeSeqID format: %s", v)
		}

		// Extract timestamp and seqid from float
		ts = int64(tsFloat)
		fractional := tsFloat - float64(ts)
		seqid = int64(math.Round(fractional * 1000000))

		// Validate: seqid cannot be 0 (fractional part must be non-zero)
		if seqid == 0 {
			return TimeSeqID{}, fmt.Errorf("invalid score: seqid cannot be 0 (fractional part must be non-zero)")
		}

		return TimeSeqID{Timestamp: ts, SeqID: seqid}, nil

	case float64:
		// Extract timestamp and seqid from float
		ts := int64(v)
		fractional := v - float64(ts)
		seqid := int64(math.Round(fractional * 1000000))

		// Validate: seqid cannot be 0 (fractional part must be non-zero)
		if seqid == 0 {
			return TimeSeqID{}, fmt.Errorf("invalid score: seqid cannot be 0 (fractional part must be non-zero)")
		}

		// Validate: seqid must be valid (0 < seqid < 1000000)
		if seqid < 0 || seqid >= 1000000 {
			return TimeSeqID{}, fmt.Errorf("invalid score: seqid out of range (must be 0 < seqid < 1000000), got %d", seqid)
		}

		// Validate: score must be exactly representable in 6 decimal places
		// Reconstruct the score and check if it matches the original
		reconstructed := float64(ts) + float64(seqid)/1000000.0
		diff := math.Abs(v - reconstructed)
		// Allow tiny floating point error (< 1e-10), but reject anything larger
		// which indicates more than 6 decimal places
		if diff > 1e-10 {
			return TimeSeqID{}, fmt.Errorf("invalid precision: score has more than 6 decimal places (got %f, reconstructed %f, diff %e)", v, reconstructed, diff)
		}

		return TimeSeqID{Timestamp: ts, SeqID: seqid}, nil

	default:
		return TimeSeqID{}, fmt.Errorf("unsupported type for TimeSeqID: %T", s)
	}
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
	encodedCatalog := encode.EncodeRedisCatalogName(catalog)

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
