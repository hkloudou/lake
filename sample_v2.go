package lake

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// sampleCache wraps the cached sample result with its score for atomic consistency.
// Score and Data are stored together in a single Redis key,
// so there's no window for inconsistency between score check and data read.
type sampleCache[T any] struct {
	Score float64 `json:"score"`
	Data  T       `json:"data"`
}

// Sample reads cached sampling result of type T for the given catalog and indicator.
// If the catalog data has changed since last sample (based on ListResult.LastUpdated),
// loader is called to produce a new T, which is cached in Redis for future calls.
//
// indicator distinguishes different sampling dimensions for the same catalog
// (e.g., "report", "summary", "stats").
//
// loader is only called when data has changed — similar to a cache-miss callback.
//
// Usage:
//
//	list := client.List(ctx, "users")
//	report, err := lake.Sample[Report](ctx, list, "daily", func(list *ListResult) (Report, error) {
//	    data, err := lake.ReadMap(ctx, list)
//	    if err != nil { return Report{}, err }
//	    return buildReport(data), nil
//	})
func Sample[T any](ctx context.Context, list *ListResult, indicator string, loader func(*ListResult) (T, error)) (T, error) {
	var zero T
	if list.Err != nil {
		return zero, list.Err
	}
	if list.HasPending {
		return zero, fmt.Errorf("pending writes detected")
	}

	c := list.client
	if err := c.ensureInitialized(ctx); err != nil {
		return zero, err
	}

	c.emitEvent(list.catalog, "Sample", map[string]any{"indicator": indicator})

	lastUpdated := list.LastUpdated()
	cacheKey := c.makeSampleCacheKey(list.catalog, indicator)

	// Single GET: score + data are atomic
	cached, err := c.rdb.Get(ctx, cacheKey).Bytes()
	if err == nil {
		var entry sampleCache[T]
		if json.Unmarshal(cached, &entry) == nil && entry.Score >= lastUpdated && entry.Score > 0 {
			return entry.Data, nil
		}
		// Score stale or unmarshal failed, fall through to reload
	} else if err != redis.Nil {
		return zero, err
	}

	// Data changed or no cache — call loader with SingleFlight
	singleFlightKey := fmt.Sprintf("sample:%s:%s:%.6f", list.catalog, indicator, lastUpdated)
	resultBytes, err := c.snapFlight.Do(singleFlightKey, func() (string, error) {
		result, err := loader(list)
		if err != nil {
			return "", err
		}

		entry := sampleCache[T]{Score: lastUpdated, Data: result}
		data, err := json.Marshal(entry)
		if err != nil {
			return "", fmt.Errorf("failed to marshal sample result: %w", err)
		}

		// Single SET: score + data atomic
		c.rdb.Set(ctx, cacheKey, data, 0)

		return string(data), nil
	})
	if err != nil {
		return zero, err
	}

	var entry sampleCache[T]
	if err := json.Unmarshal([]byte(resultBytes), &entry); err != nil {
		return zero, err
	}
	return entry.Data, nil
}

func (c *Client) makeSampleCacheKey(catalog, indicator string) string {
	return fmt.Sprintf("%s:%s:sample_cache:%s", c.reader.Prefix(), catalog, indicator)
}
