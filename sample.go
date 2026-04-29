package lake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// ErrPendingWrites is returned when a Sample/Read sees a list whose
// index contains a pending write that may affect the result.
var ErrPendingWrites = errors.New("pending writes detected")

// Sample returns the cached sample of type T for the given indicator
// applied to list's catalog. If the catalog changed since the last
// sample (compared by ListResult.LastUpdated), loader is invoked and
// the new value is cached for future calls.
//
// Storage layout: all catalogs sharing one indicator live inside
// "<prefix>:samples:<indicator>" Hash, with catalog as field and
// "[score, data]" JSON as value. Indicator-wide bulk operations stay
// single-key.
func Sample[T any](ctx context.Context, list *ListResult, indicator string, loader func(*ListResult) (T, error)) (T, error) {
	var zero T

	c := list.client
	c.emitEvent(list.catalog, "Sample", map[string]any{"indicator": indicator})

	if list.Err != nil {
		return zero, list.Err
	}
	if list.HasPending {
		return zero, ErrPendingWrites
	}
	if err := c.ensureInitialized(ctx); err != nil {
		return zero, err
	}

	lastUpdated := list.LastUpdated()
	hashKey := c.reader.MakeSampleIndicatorKey(indicator)
	field := list.catalog

	// Cache hit fast-path: one HGET; values are "[score, data]" JSON arrays.
	if cached, err := c.rdb.HGet(ctx, hashKey, field).Bytes(); err == nil {
		if score, data, derr := unmarshalSampleCache[T](cached); derr == nil && score >= lastUpdated && score > 0 {
			return data, nil
		}
	} else if err != redis.Nil {
		return zero, err
	}

	// Cache miss / stale: SingleFlight-deduped loader on (catalog, indicator, score).
	flightKey := fmt.Sprintf("%s:%s:%.6f", list.catalog, indicator, lastUpdated)
	resultBytes, err := c.sampleFlight.Do(flightKey, func() (string, error) {
		result, err := loader(list)
		if err != nil {
			return "", err
		}
		data, err := marshalSampleCache(lastUpdated, result)
		if err != nil {
			return "", fmt.Errorf("marshal sample: %w", err)
		}
		if err := c.rdb.HSet(ctx, hashKey, field, data).Err(); err != nil {
			return "", fmt.Errorf("hset sample: %w", err)
		}
		return string(data), nil
	})
	if err != nil {
		return zero, err
	}
	_, result, err := unmarshalSampleCache[T]([]byte(resultBytes))
	return result, err
}

func marshalSampleCache[T any](score float64, data T) ([]byte, error) {
	return json.Marshal([2]any{score, data})
}

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
