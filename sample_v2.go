package lake

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// marshalSampleCache serializes [score, data] as a JSON array.
func marshalSampleCache[T any](score float64, data T) ([]byte, error) {
	return json.Marshal([2]any{score, data})
}

// unmarshalSampleCache deserializes [score, data] from a JSON array.
func unmarshalSampleCache[T any](raw []byte) (float64, T, error) {
	var arr [2]json.RawMessage
	var zero T
	if err := json.Unmarshal(raw, &arr); err != nil {
		return 0, zero, err
	}
	var score float64
	if err := json.Unmarshal(arr[0], &score); err != nil {
		return 0, zero, err
	}
	var data T
	if err := json.Unmarshal(arr[1], &data); err != nil {
		return 0, zero, err
	}
	return score, data, nil
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
	hashKey := c.makeSampleHashKey(list.catalog)

	// Single HGET: [score, data] atomic per indicator
	cached, err := c.rdb.HGet(ctx, hashKey, indicator).Bytes()
	if err == nil {
		score, data, unmarshalErr := unmarshalSampleCache[T](cached)
		if unmarshalErr == nil && score >= lastUpdated && score > 0 {
			return data, nil
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

		data, err := marshalSampleCache(lastUpdated, result)
		if err != nil {
			return "", fmt.Errorf("failed to marshal sample result: %w", err)
		}

		// Single HSET: [score, data] atomic per indicator
		c.rdb.HSet(ctx, hashKey, indicator, data)

		return string(data), nil
	})
	if err != nil {
		return zero, err
	}

	_, result, err := unmarshalSampleCache[T]([]byte(resultBytes))
	if err != nil {
		return zero, err
	}
	return result, nil
}

func (c *Client) makeSampleHashKey(catalog string) string {
	return fmt.Sprintf("%s:%s:sample", c.reader.Prefix(), catalog)
}
