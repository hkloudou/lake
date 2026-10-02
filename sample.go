package lake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/hkloudou/lake/v3/internal/utils"
)

// Sampler[T] derives a cached value of type T from a catalog's raw state.
// The loader runs on a cache miss; the result is memoised in the
// "<prefix>:m:<indicator>" Redis hash (field = catalog) together with the
// data version and removal generation it was computed under, and is served
// only while both still match the caller's ListResult. WithMaxAge and
// WithShouldRefresh can only ADD recomputes. Loader errors and their
// fallbacks are never cached. The memo is a cache: a Redis outage degrades
// to recompute, never to failure.
type Sampler[T any] struct {
	indicator     string
	loader        func(*ListResult) (T, error)
	maxAge        time.Duration
	shouldRefresh func(SampleMeta, *ListResult, map[string]*ListResult) bool
	onLoaderErr   func(error) (T, bool)
}

// SampleMeta is stored alongside each cached sample and handed to a
// WithShouldRefresh predicate.
type SampleMeta struct {
	Score     float64 // ListResult.LastUpdated() when the sample was computed
	UpdatedAt int64   // unix seconds when it was computed; basis for WithMaxAge
	RemoveGen string  // ListResult.RemoveGen() when it was computed
}

// SampleResult is one entry of Batch's output.
type SampleResult[T any] struct {
	Value T
	Err   error
}

type SamplerOption[T any] func(*Sampler[T])

// NewSampler builds a reusable Sampler for one indicator (same naming rules
// as a catalog — it is embedded in a Redis key). Change the indicator name
// when the loader's logic changes; that is the cache-busting mechanism.
// Panics on an invalid indicator or nil loader.
func NewSampler[T any](indicator string, loader func(*ListResult) (T, error), opts ...SamplerOption[T]) *Sampler[T] {
	if err := utils.ValidateCatalog(indicator); err != nil {
		panic(fmt.Sprintf("lake: NewSampler invalid indicator: %v", err))
	}
	if loader == nil {
		panic("lake: NewSampler loader must be non-nil")
	}
	s := &Sampler[T]{indicator: indicator, loader: loader}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// WithMaxAge forces a recompute when the cached sample is older than d,
// regardless of data version. d <= 0 disables the check.
func WithMaxAge[T any](d time.Duration) SamplerOption[T] {
	return func(s *Sampler[T]) { s.maxAge = d }
}

// WithShouldRefresh installs a custom staleness predicate. peers is every
// ListResult of the current call (Batch: the whole map; Sample: just self),
// so a cross-catalog dependency can compare peers["B"].LastUpdated() and
// peers["B"].RemoveGen() against a baseline recorded inside T. Runs on every
// hit, so it must be pure and cheap.
func WithShouldRefresh[T any](fn func(meta SampleMeta, self *ListResult, peers map[string]*ListResult) bool) SamplerOption[T] {
	return func(s *Sampler[T]) { s.shouldRefresh = fn }
}

// WithLoaderErrorFallback substitutes a value when the loader fails: fn
// returns (value, true) to serve it for this call only (never cached), or
// (_, false) to propagate the error.
func WithLoaderErrorFallback[T any](fn func(error) (T, bool)) SamplerOption[T] {
	return func(s *Sampler[T]) { s.onLoaderErr = fn }
}

// WithLoaderErrorDefault always serves v on a loader error.
func WithLoaderErrorDefault[T any](v T) SamplerOption[T] {
	return WithLoaderErrorFallback(func(error) (T, bool) { return v, true })
}

// Sample returns the sample for list's catalog: one HGET on a hit, loader +
// HSET on a miss.
func (s *Sampler[T]) Sample(ctx context.Context, list *ListResult) (T, error) {
	var zero T
	if list == nil || list.client == nil {
		return zero, errors.New("lake: Sample requires a ListResult from List/BatchList")
	}
	c := list.client
	if c.hasHandlers() {
		c.emitEvent(list.catalog, "Sample", map[string]any{"indicator": s.indicator})
	}
	if list.Err != nil {
		return zero, list.Err
	}
	peers := map[string]*ListResult{list.catalog: list}
	raw, err := c.sampleRdb.HGet(ctx, c.idx.SampleKey(s.indicator), list.catalog).Result()
	if v, ok := s.hit(raw, err, list, peers); ok {
		return v, nil
	}
	return s.finalize(s.loadAndCache(ctx, list))
}

// Batch samples many catalogs: one HMGET, then the loader concurrently (10
// at a time) for the misses only. Errors are per catalog. Pipe it from
// BatchList.
func (s *Sampler[T]) Batch(ctx context.Context, lists map[string]*ListResult) map[string]*SampleResult[T] {
	out := make(map[string]*SampleResult[T], len(lists))
	var c *Client
	probe := make([]string, 0, len(lists))
	for cat, l := range lists {
		switch {
		case l == nil || l.client == nil:
			out[cat] = &SampleResult[T]{Err: errors.New("lake: Batch requires ListResults from BatchList")}
		case l.Err != nil:
			out[cat] = &SampleResult[T]{Err: l.Err}
		default:
			c = l.client
			probe = append(probe, cat)
		}
	}
	if len(probe) == 0 {
		return out
	}
	if c.hasHandlers() {
		for _, cat := range probe {
			c.emitEvent(cat, "BatchSample", map[string]any{"indicator": s.indicator})
		}
	}

	vals, err := c.sampleRdb.HMGet(ctx, c.idx.SampleKey(s.indicator), probe...).Result()
	if len(vals) != len(probe) {
		vals = make([]any, len(probe)) // cache-read failure: everything is a miss
	}
	var (
		mu     sync.Mutex
		wg     sync.WaitGroup
		sem    = make(chan struct{}, 10)
		misses []string
	)
	for i, cat := range probe {
		raw, _ := vals[i].(string)
		if v, ok := s.hit(raw, err, lists[cat], lists); ok {
			out[cat] = &SampleResult[T]{Value: v}
			continue
		}
		misses = append(misses, cat)
	}
	for _, cat := range misses {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			v, e := s.finalize(s.loadAndCache(ctx, lists[cat]))
			mu.Lock()
			out[cat] = &SampleResult[T]{Value: v, Err: e}
			mu.Unlock()
		})
	}
	wg.Wait()
	return out
}

// hit decodes a cached value and reports whether it may be served to list.
func (s *Sampler[T]) hit(raw string, err error, list *ListResult, peers map[string]*ListResult) (T, bool) {
	var zero T
	if err != nil || raw == "" {
		return zero, false
	}
	meta, data, derr := unmarshalSampleCache[T]([]byte(raw))
	if derr != nil || s.isStale(meta, list, peers) {
		return zero, false
	}
	return data, true
}

// isStale: the data-version floor and the removal-generation match are
// mandatory; maxAge and shouldRefresh only add triggers. Score 0 with an
// empty catalog is a valid hit (pre-provisioned tenants must not recompute on
// every call); the first write raises LastUpdated and invalidates it.
func (s *Sampler[T]) isStale(meta SampleMeta, list *ListResult, peers map[string]*ListResult) bool {
	if meta.Score < list.LastUpdated() || meta.RemoveGen != list.RemoveGen() {
		return true
	}
	if s.maxAge > 0 && time.Duration(time.Now().Unix()-meta.UpdatedAt)*time.Second >= s.maxAge {
		return true
	}
	return s.shouldRefresh != nil && s.shouldRefresh(meta, list, peers)
}

// loadAndCache runs the loader once per (catalog, indicator, version,
// generation) across concurrent callers, writes the result back best-effort,
// and hands every caller the same JSON-round-tripped value. The entry
// carries list's version and generation, so one computed from a ListResult
// taken before a RemoveDelta is rejected by isStale afterwards — the write
// needs no further barrier.
func (s *Sampler[T]) loadAndCache(ctx context.Context, list *ListResult) (T, error) {
	var zero T
	c := list.client
	meta := SampleMeta{Score: list.LastUpdated(), RemoveGen: list.RemoveGen()}
	key := list.catalog + ":" + s.indicator + ":" + strconv.FormatFloat(meta.Score, 'f', 6, 64) + ":" + meta.RemoveGen
	raw, err, _ := c.sampleFlight.Do(key, func() (any, error) {
		v, err := s.loader(list)
		if err != nil {
			return "", &loaderError{err}
		}
		meta.UpdatedAt = time.Now().Unix()
		data, err := json.Marshal([4]any{meta.Score, meta.UpdatedAt, meta.RemoveGen, v})
		if err != nil {
			return "", fmt.Errorf("marshal sample: %w", err)
		}
		if err := c.sampleRdb.HSet(ctx, c.idx.SampleKey(s.indicator), list.catalog, data).Err(); err != nil {
			c.emitEvent(list.catalog, "SampleCacheError", map[string]any{"op": "hset", "err": err.Error()})
		}
		return string(data), nil
	})
	if err != nil {
		return zero, err
	}
	_, v, err := unmarshalSampleCache[T]([]byte(raw.(string)))
	return v, err
}

// finalize applies the loader-error fallback; the caller always sees their
// original loader error (unwrapped) so errors.Is still works.
func (s *Sampler[T]) finalize(v T, err error) (T, error) {
	var zero T
	var le *loaderError
	if err == nil {
		return v, nil
	}
	if errors.As(err, &le) {
		if s.onLoaderErr != nil {
			if d, ok := s.onLoaderErr(le.err); ok {
				return d, nil
			}
		}
		return zero, le.err
	}
	return zero, err
}

type loaderError struct{ err error }

func (e *loaderError) Error() string { return e.err.Error() }
func (e *loaderError) Unwrap() error { return e.err }

// Cache value: the JSON array [score, updatedAt, removeGen, data]. Anything
// else (older formats, foreign values) reads as a miss.
func unmarshalSampleCache[T any](raw []byte) (SampleMeta, T, error) {
	var (
		arr  [4]json.RawMessage
		meta SampleMeta
		data T
	)
	if err := json.Unmarshal(raw, &arr); err != nil {
		return meta, data, err
	}
	for i, dst := range []any{&meta.Score, &meta.UpdatedAt, &meta.RemoveGen, &data} {
		if err := json.Unmarshal(arr[i], dst); err != nil {
			return meta, data, err
		}
	}
	if meta.UpdatedAt <= 0 || meta.RemoveGen == "" {
		return meta, data, errors.New("sample cache entry missing metadata")
	}
	return meta, data, nil
}
