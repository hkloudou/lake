package lake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// ErrPendingWrites is returned when a Sample/Read is attempted on a list whose
// index contains a pending write that may affect the result.
var ErrPendingWrites = errors.New("pending writes detected")

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

// Sample returns the cached sampling result of type T for the given indicator
// applied to the catalog represented by list. If the catalog has changed since
// the last sample (compared by ListResult.LastUpdated), loader is invoked to
// produce a fresh value, which is then cached for future calls.
//
// indicator distinguishes different sampling dimensions for the same catalog
// (e.g. "report", "summary", "stats"). All catalogs sharing an indicator live
// inside the same Redis Hash "<prefix>:samples:<indicator>", with the catalog
// name as the field, making indicator-wide operations cheap.
//
// Usage:
//
//	list := client.List(ctx, "users")
//	report, err := lake.Sample[Report](ctx, list, "daily", func(l *ListResult) (Report, error) {
//	    data, err := lake.ReadMap(ctx, l)
//	    if err != nil { return Report{}, err }
//	    return buildReport(data), nil
//	})
func Sample[T any](ctx context.Context, list *ListResult, indicator string, loader func(*ListResult) (T, error)) (T, error) {
	var zero T
	if list.Err != nil {
		return zero, list.Err
	}
	if list.HasPending {
		return zero, ErrPendingWrites
	}

	c := list.client
	if err := c.ensureInitialized(ctx); err != nil {
		return zero, err
	}

	c.emitEvent(list.catalog, "Sample", map[string]any{"indicator": indicator})

	lastUpdated := list.LastUpdated()
	hashKey := c.reader.MakeSampleIndicatorKey(indicator)
	field := list.catalog

	// Single HGET: [score, data] atomic per (indicator, catalog).
	cached, err := c.rdb.HGet(ctx, hashKey, field).Bytes()
	if err == nil {
		score, data, unmarshalErr := unmarshalSampleCache[T](cached)
		if unmarshalErr == nil && score >= lastUpdated && score > 0 {
			return data, nil
		}
		// Score stale or unmarshal failed — fall through to reload.
	} else if err != redis.Nil {
		return zero, err
	}

	// Data changed or no cache — invoke loader under SingleFlight to dedupe
	// concurrent loaders for the same (catalog, indicator, score).
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

		if err := c.rdb.HSet(ctx, hashKey, field, data).Err(); err != nil {
			return "", fmt.Errorf("failed to write sample cache: %w", err)
		}

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
