package lake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

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

	// Cache hit fast-path: one HGET; values are "[score, data]" JSON arrays.
	if cached, err := c.rdb.HGet(ctx, hashKey, list.catalog).Bytes(); err == nil {
		if score, data, derr := unmarshalSampleCache[T](cached); derr == nil && score >= lastUpdated && score > 0 {
			return data, nil
		}
	} else if err != redis.Nil {
		return zero, err
	}

	return loadAndCacheSample(ctx, c, list, indicator, hashKey, lastUpdated, loader)
}

// BatchSampleResult is one entry in BatchSample's output: either a
// loaded/cached value or an error scoped to that catalog.
type BatchSampleResult[T any] struct {
	Value T
	Err   error
}

// BatchSample fetches the same indicator for many catalogs in one
// HMGet of "<prefix>:samples:<indicator>". Cache hits return immediately;
// misses run loader concurrently (10 workers) and write back via HSet
// inside the same per-(catalog, indicator, score) SingleFlight that
// Sample uses, so concurrent Sample + BatchSample calls dedupe.
//
// Errors are isolated per catalog: a list with Err / HasPending / a
// failing loader only poisons its own BatchSampleResult.
//
// Designed to be piped from BatchList:
//
//	lists := client.BatchList(ctx, catalogs)
//	results := lake.BatchSample[Report](ctx, lists, "daily", loader)
func BatchSample[T any](
	ctx context.Context,
	lists map[string]*ListResult,
	indicator string,
	loader func(*ListResult) (T, error),
) map[string]*BatchSampleResult[T] {
	out := make(map[string]*BatchSampleResult[T], len(lists))
	if len(lists) == 0 {
		return out
	}

	// Pick any client; ListResults from BatchList all share one.
	var c *Client
	for _, l := range lists {
		if l != nil && l.client != nil {
			c = l.client
			break
		}
	}
	if c == nil {
		err := errors.New("BatchSample: no client (empty or invalid lists)")
		for cat := range lists {
			out[cat] = &BatchSampleResult[T]{Err: err}
		}
		return out
	}

	// Emit per catalog before any early return (mirrors BatchList).
	for cat := range lists {
		c.emitEvent(cat, "BatchSample", map[string]any{"indicator": indicator})
	}

	if err := c.ensureInitialized(ctx); err != nil {
		for cat := range lists {
			out[cat] = &BatchSampleResult[T]{Err: err}
		}
		return out
	}

	// Partition: drop catalogs that already have list-level errors;
	// remaining go into the HMGet probe.
	probe := make([]string, 0, len(lists))
	for cat, l := range lists {
		switch {
		case l == nil:
			out[cat] = &BatchSampleResult[T]{Err: errors.New("nil list")}
		case l.Err != nil:
			out[cat] = &BatchSampleResult[T]{Err: l.Err}
		case l.HasPending:
			out[cat] = &BatchSampleResult[T]{Err: ErrPendingWrites}
		default:
			probe = append(probe, cat)
		}
	}
	if len(probe) == 0 {
		return out
	}

	hashKey := c.reader.MakeSampleIndicatorKey(indicator)

	// One HMGet to surface every cache hit at once.
	cached, err := c.rdb.HMGet(ctx, hashKey, probe...).Result()
	if err != nil && err != redis.Nil {
		for _, cat := range probe {
			out[cat] = &BatchSampleResult[T]{Err: fmt.Errorf("hmget samples: %w", err)}
		}
		return out
	}

	// Classify: hit serves immediately, miss queues for the worker pool.
	var (
		mu     sync.Mutex
		misses []string
	)
	for i, raw := range cached {
		cat := probe[i]
		l := lists[cat]
		lastUpdated := l.LastUpdated()
		if s, ok := raw.(string); ok {
			if score, data, derr := unmarshalSampleCache[T]([]byte(s)); derr == nil && score >= lastUpdated && score > 0 {
				out[cat] = &BatchSampleResult[T]{Value: data}
				continue
			}
		}
		misses = append(misses, cat)
	}
	if len(misses) == 0 {
		return out
	}

	// Worker pool: 10 concurrent loaders, dedupe via sampleFlight.
	workers := 10
	if len(misses) < workers {
		workers = len(misses)
	}
	jobs := make(chan string, len(misses))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			for cat := range jobs {
				l := lists[cat]
				value, err := loadAndCacheSample(ctx, c, l, indicator, hashKey, l.LastUpdated(), loader)
				mu.Lock()
				out[cat] = &BatchSampleResult[T]{Value: value, Err: err}
				mu.Unlock()
			}
		})
	}
	for _, cat := range misses {
		jobs <- cat
	}
	close(jobs)
	wg.Wait()

	return out
}

// loadAndCacheSample is the shared cache-fill path for Sample and
// BatchSample. Wraps loader in the (catalog, indicator, score)
// SingleFlight, encodes as "[score, data]", and HSets the result.
//
// Generic free function (not a method) because Go forbids generic
// methods on a non-generic receiver.
func loadAndCacheSample[T any](
	ctx context.Context,
	c *Client,
	list *ListResult,
	indicator, hashKey string,
	lastUpdated float64,
	loader func(*ListResult) (T, error),
) (T, error) {
	var zero T
	flightKey := fmt.Sprintf("%s:%s:%.6f", list.catalog, indicator, lastUpdated)
	raw, err := c.sampleFlight.Do(flightKey, func() (string, error) {
		result, err := loader(list)
		if err != nil {
			return "", err
		}
		data, err := marshalSampleCache(lastUpdated, result)
		if err != nil {
			return "", fmt.Errorf("marshal sample: %w", err)
		}
		if err := c.rdb.HSet(ctx, hashKey, list.catalog, data).Err(); err != nil {
			return "", fmt.Errorf("hset sample: %w", err)
		}
		return string(data), nil
	})
	if err != nil {
		return zero, err
	}
	_, value, err := unmarshalSampleCache[T]([]byte(raw))
	return value, err
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
