package lake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hkloudou/lake/v3/internal/utils"
	"github.com/redis/go-redis/v9"
)

// Sampler[T] is the single entry point for derived, cached sampling.
//
// A "sample" is a value of type T computed from a catalog's raw state
// (snap + deltas) by a loader, then memoised in the
// "<prefix>:m:<indicator>" Redis Hash (catalog = field). Construct
// one Sampler per (indicator, T, loader) and reuse it: the staleness
// policy and error fallback are bound at construction so every call site
// stays uniform.
//
// Validity is layered. The data-version floor is mandatory — if the
// catalog advanced past the version the sample was computed at, it is
// recomputed. WithMaxAge and WithShouldRefresh add further triggers (the
// analog of React's shouldComponentUpdate: force a "re-render" even when
// the data version is unchanged); they can only cause MORE recomputes,
// never serve a value staler than the floor allows.
//
// The cache only ever holds genuinely computed values. A loader error,
// and any fallback substituted for it, is returned to the caller but
// never written back — so a transient failure cannot poison the cache
// until the catalog's next write.
//
// Physically the memo Hash may live on a dedicated cache-tier Redis
// (Client option WithSampleCacheRedis / WithSampleCacheURL); a cache
// Redis outage degrades gracefully — reads recompute, writes are
// best-effort — so the authoritative store never depends on cache health.
type Sampler[T any] struct {
	indicator     string
	loader        func(*ListResult) (T, error)
	maxAge        time.Duration
	shouldRefresh func(SampleMeta, *ListResult, map[string]*ListResult) bool
	onLoaderErr   func(error) (T, bool)
}

// SampleMeta is the metadata stored alongside each cached sample, and the
// input a WithShouldRefresh predicate compares against.
type SampleMeta struct {
	// Score is the catalog's data version (ListResult.LastUpdated) at the
	// moment the sample was computed.
	Score float64
	// UpdatedAt is the Redis-server wall clock (unix seconds) when the
	// sample was computed; the basis for WithMaxAge.
	UpdatedAt int64
}

// SampleResult is one entry of Batch's output: a value, or an error
// scoped to that single catalog.
type SampleResult[T any] struct {
	Value T
	Err   error
}

// SamplerOption configures a Sampler at construction.
type SamplerOption[T any] func(*Sampler[T])

// NewSampler builds a reusable Sampler for one indicator. loader computes
// the sample from a catalog's ListResult on a cache miss. It panics if
// indicator is empty or invalid, or loader is nil (programmer error —
// fail-fast, per package policy). The indicator is embedded verbatim in the
// Redis memo key "<prefix>:m:<indicator>", so it follows the same charset
// rules as catalog names (no ":" "|" "(" ")", ASCII only).
func NewSampler[T any](indicator string, loader func(*ListResult) (T, error), opts ...SamplerOption[T]) *Sampler[T] {
	if indicator == "" {
		panic("lake: NewSampler indicator must be non-empty")
	}
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
// measured by the Redis server clock (~5s resolution), regardless of data
// version. Use it for time-sensitive derivations whose correctness depends
// on wall-clock and not just catalog data (e.g. a "today" report). d <= 0
// disables the check.
func WithMaxAge[T any](d time.Duration) SamplerOption[T] {
	return func(s *Sampler[T]) { s.maxAge = d }
}

// WithShouldRefresh installs a custom staleness predicate — the analog of
// React's shouldComponentUpdate. It receives:
//
//   - meta:  the cached entry's metadata (its data version and compute time)
//   - self:  this catalog's current ListResult
//   - peers: every ListResult available in the current call's context. For
//     Sampler.Batch this is the full lists map (including self under its
//     own key); for Sampler.Sample it is a singleton {self.catalog: self}.
//     Never nil.
//
// Returns true to force a recompute even when the data version is unchanged
// (cross-catalog dependencies, external inputs). Runs on every cache hit,
// so it MUST be pure and cheap: do no I/O.
//
// To act on a cross-catalog dependency, fan out a single BatchList covering
// the catalogs you care about, then compare peers["B"].LastUpdated() against
// whatever version of B the cached T recorded at compute time (T is the
// natural place to carry "what I depended on" — Sampler does not track it).
// If the relevant peer is absent from peers, the dependency cannot be
// evaluated this call; either return false (accept the stale value this
// time) or arrange for BatchList to include it next time.
func WithShouldRefresh[T any](fn func(meta SampleMeta, self *ListResult, peers map[string]*ListResult) bool) SamplerOption[T] {
	return func(s *Sampler[T]) { s.shouldRefresh = fn }
}

// WithLoaderErrorFallback substitutes a value when the loader returns an
// error. fn inspects that error and returns (value, true) to serve the
// value for this call only, or (_, false) to propagate the error. The
// substitute is transient — it is NEVER written to the cache, so the next
// call retries the loader. fn may run concurrently (Batch) and should be
// safe to call from multiple goroutines.
func WithLoaderErrorFallback[T any](fn func(error) (T, bool)) SamplerOption[T] {
	return func(s *Sampler[T]) { s.onLoaderErr = fn }
}

// WithLoaderErrorDefault is WithLoaderErrorFallback that always serves v on
// any loader error.
func WithLoaderErrorDefault[T any](v T) SamplerOption[T] {
	return func(s *Sampler[T]) { s.onLoaderErr = func(error) (T, bool) { return v, true } }
}

// Sample returns the sample for list's catalog, computing and caching it
// on a miss or when the staleness policy fires.
func (s *Sampler[T]) Sample(ctx context.Context, list *ListResult) (T, error) {
	var zero T
	if list == nil || list.client == nil {
		return zero, errors.New("lake: Sample requires a ListResult from List/BatchList")
	}
	c := list.client
	c.emitEvent(list.catalog, "Sample", map[string]any{"indicator": s.indicator})

	if list.Err != nil {
		return zero, list.Err
	}
	// Single-catalog context: the only "peer" available is self.
	peers := map[string]*ListResult{list.catalog: list}
	return s.finalize(s.sampleCore(ctx, c, list, peers))
}

// Batch fetches the same indicator for many catalogs in one HMGet, running
// the loader concurrently (10 workers) only for misses; cache hits return
// immediately. Errors are isolated per catalog. Misses dedupe against the
// same SingleFlight as Sample, so concurrent Sample + Batch calls share a
// loader run. Designed to be piped from BatchList:
//
//	lists := client.BatchList(ctx, catalogs)
//	results := sampler.Batch(ctx, lists)
func (s *Sampler[T]) Batch(ctx context.Context, lists map[string]*ListResult) map[string]*SampleResult[T] {
	out := make(map[string]*SampleResult[T], len(lists))
	if len(lists) == 0 {
		return out
	}

	// Any ListResult from BatchList carries the same client.
	var c *Client
	for _, l := range lists {
		if l != nil && l.client != nil {
			c = l.client
			break
		}
	}
	if c == nil {
		err := errors.New("lake: Batch has no client (empty or invalid lists)")
		for cat := range lists {
			out[cat] = &SampleResult[T]{Err: err}
		}
		return out
	}

	for cat := range lists {
		c.emitEvent(cat, "BatchSample", map[string]any{"indicator": s.indicator})
	}

	// Partition: drop catalogs that already have list-level errors; the
	// rest go into the HMGet probe.
	probe := make([]string, 0, len(lists))
	for cat, l := range lists {
		switch {
		case l == nil:
			out[cat] = &SampleResult[T]{Err: errors.New("nil list")}
		case l.Err != nil:
			out[cat] = &SampleResult[T]{Err: l.Err}
		default:
			probe = append(probe, cat)
		}
	}
	if len(probe) == 0 {
		return out
	}

	now := c.reader.NowUnix()
	hashKey := c.reader.MakeSampleIndicatorKey(s.indicator)

	// One HMGet probes every catalog's cached value plus the epoch field —
	// captured before any loader runs, so a mid-batch invalidation voids the
	// batch's write-backs.
	fields := append(append(make([]string, 0, len(probe)+1), probe...), sampleEpochField)
	epoch := "0"
	cached, err := c.sampleRdb.HMGet(ctx, hashKey, fields...).Result()
	if err != nil && err != redis.Nil {
		// Cache-read failure: degrade to recompute-all rather than fail
		// the batch (the cache is an optimization, not the truth).
		for _, cat := range probe {
			c.emitEvent(cat, "SampleCacheError", map[string]any{"op": "hmget", "err": err.Error()})
		}
		cached = make([]any, len(fields))
	}
	if e, ok := cached[len(probe)].(string); ok {
		epoch = e
	}

	var (
		mu     sync.Mutex
		misses []string
	)
	for i, raw := range cached[:len(probe)] {
		cat := probe[i]
		l := lists[cat]
		if str, ok := raw.(string); ok {
			if meta, data, derr := unmarshalSampleCache[T]([]byte(str)); derr == nil && !s.isStale(meta, l, lists, now) {
				out[cat] = &SampleResult[T]{Value: data}
				continue
			}
		}
		misses = append(misses, cat)
	}
	if len(misses) == 0 {
		return out
	}

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
				// Re-read the clock per loader run: with many misses ahead in
				// the queue, the batch-start `now` could stamp an UpdatedAt
				// already a whole maxAge in the past.
				v, e := s.finalize(s.loadAndCache(ctx, c, l, hashKey, l.LastUpdated(), c.reader.NowUnix(), epoch))
				mu.Lock()
				out[cat] = &SampleResult[T]{Value: v, Err: e}
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

// sampleEpochField is the memo hash's reserved epoch field. Catalog names
// cannot be ":" (ValidateCatalog forbids the character entirely), so it can
// never collide with a real catalog field. The epoch is bumped by every
// invalidation; loadAndCache captures it at probe time and its write-back is
// discarded if the epoch moved — otherwise an in-flight HSET landing after
// an HDEL would silently reinstate the just-invalidated value, and with the
// data version unchanged nothing would ever evict it again.
const sampleEpochField = ":"

// sampleWriteScript writes a computed sample only if the memo epoch still
// matches what the writer observed before running its loader.
// KEYS[1] = memo hash; ARGV[1] = epoch, ARGV[2] = catalog, ARGV[3] = value.
const sampleWriteScript = `
if (redis.call("HGET", KEYS[1], ":") or "0") == ARGV[1] then
  redis.call("HSET", KEYS[1], ARGV[2], ARGV[3])
  return 1
end
return 0
`

// sampleInvalidateScript bumps the epoch and deletes the catalogs' fields in
// one atomic step. KEYS[1] = memo hash; ARGV = catalogs. Returns the number
// of fields deleted.
const sampleInvalidateScript = `
redis.call("HINCRBY", KEYS[1], ":", 1)
return redis.call("HDEL", KEYS[1], unpack(ARGV))
`

// InvalidateSamples deletes the cached samples of one indicator for the given
// catalogs and returns how many entries existed. The next Sample/Batch
// recomputes them. Deletion and the epoch bump are atomic, so a compute
// already in flight when the invalidation lands cannot write its (stale)
// result back — its epoch no longer matches (see sampleEpochField).
//
// The memo hash has no TTL of its own, so this is the hook for the two cases
// staleness policies cannot see: a catalog that was deleted outright (its
// memo field would otherwise linger forever), and a loader whose CODE changed
// (the data version didn't move, but the cached value is now computed wrong).
// Works on any Client with the same prefix — no Sampler[T] value needed, so
// ops tooling can sweep without knowing T. RemoveDelta calls this for every
// indicator automatically.
func (c *Client) InvalidateSamples(ctx context.Context, indicator string, catalogs ...string) (int64, error) {
	for _, cat := range catalogs {
		c.emitEvent(cat, "InvalidateSample", map[string]any{"indicator": indicator})
	}
	if err := utils.ValidateCatalog(indicator); err != nil {
		return 0, fmt.Errorf("invalid indicator: %w", err)
	}
	for _, cat := range catalogs {
		if err := utils.ValidateCatalog(cat); err != nil {
			return 0, err
		}
	}
	if len(catalogs) == 0 {
		return 0, nil
	}
	args := make([]any, len(catalogs))
	for i, cat := range catalogs {
		args[i] = cat
	}
	res, err := c.sampleRdb.Eval(ctx, sampleInvalidateScript,
		[]string{c.reader.MakeSampleIndicatorKey(indicator)}, args...).Result()
	if err != nil {
		return 0, err
	}
	n, _ := res.(int64)
	return n, nil
}

// isStale decides whether a cached entry must be recomputed. The data-
// version floor is mandatory; maxAge and shouldRefresh only ADD triggers
// (they can force a refresh, never suppress one the data version requires).
func (s *Sampler[T]) isStale(meta SampleMeta, list *ListResult, peers map[string]*ListResult, now int64) bool {
	lastUpdated := list.LastUpdated()
	if !(meta.Score >= lastUpdated && meta.Score > 0) {
		return true // data advanced past the cached version (or none) → refresh
	}
	// Compare in time.Duration so a sub-second maxAge keeps its value instead
	// of truncating to 0 (which would mark every hit stale). The clock itself
	// still ticks in whole seconds (~5s resolution).
	if s.maxAge > 0 && now > 0 && meta.UpdatedAt > 0 && time.Duration(now-meta.UpdatedAt)*time.Second >= s.maxAge {
		return true
	}
	if s.shouldRefresh != nil && s.shouldRefresh(meta, list, peers) {
		return true
	}
	return false
}

// sampleCore is the cache-or-compute path for a single catalog: one HMGET
// (value + epoch), then load on miss/stale. A cache-READ failure degrades to
// recompute — it never fails the call. The epoch is captured here, BEFORE the
// loader runs, so an invalidation that lands during the compute makes the
// write-back a no-op.
func (s *Sampler[T]) sampleCore(ctx context.Context, c *Client, list *ListResult, peers map[string]*ListResult) (T, error) {
	lastUpdated := list.LastUpdated()
	now := c.reader.NowUnix()
	hashKey := c.reader.MakeSampleIndicatorKey(s.indicator)

	epoch := "0"
	if vals, err := c.sampleRdb.HMGet(ctx, hashKey, list.catalog, sampleEpochField).Result(); err != nil {
		c.emitEvent(list.catalog, "SampleCacheError", map[string]any{"op": "hmget", "err": err.Error()})
	} else {
		if e, ok := vals[1].(string); ok {
			epoch = e
		}
		if cached, ok := vals[0].(string); ok {
			if meta, data, derr := unmarshalSampleCache[T]([]byte(cached)); derr == nil && !s.isStale(meta, list, peers, now) {
				return data, nil
			}
		}
	}
	return s.loadAndCache(ctx, c, list, hashKey, lastUpdated, now, epoch)
}

// loadAndCache runs the loader under the (catalog, indicator, version)
// SingleFlight, stamps [score, updatedAt, data], and writes it back
// best-effort — and conditionally: the write lands only if the memo epoch
// still matches what the caller observed before the loader ran, so an
// invalidation cannot be undone by an in-flight compute (the value is still
// returned to this caller; it just isn't cached). A loader error is wrapped
// (loaderError) so finalize can distinguish it from an internal encode
// error; a cache-WRITE failure is swallowed — the computed value is already
// correct and is returned anyway.
func (s *Sampler[T]) loadAndCache(ctx context.Context, c *Client, list *ListResult, hashKey string, lastUpdated float64, now int64, epoch string) (T, error) {
	var zero T
	flightKey := fmt.Sprintf("%s:%s:%.6f", list.catalog, s.indicator, lastUpdated)
	raw, err := c.sampleFlight.Do(flightKey, func() (string, error) {
		result, lerr := s.loader(list)
		if lerr != nil {
			return "", &loaderError{err: lerr}
		}
		data, merr := marshalSampleCache(SampleMeta{Score: lastUpdated, UpdatedAt: now}, result)
		if merr != nil {
			return "", fmt.Errorf("marshal sample: %w", merr)
		}
		if werr := c.sampleRdb.Eval(ctx, sampleWriteScript, []string{hashKey}, epoch, list.catalog, data).Err(); werr != nil {
			c.emitEvent(list.catalog, "SampleCacheError", map[string]any{"op": "hset", "err": werr.Error()})
		}
		return string(data), nil
	})
	if err != nil {
		return zero, err
	}
	_, value, derr := unmarshalSampleCache[T]([]byte(raw))
	return value, derr
}

// finalize applies the loader-error fallback. A substituted value is
// returned to the caller only — never cached. Internal (non-loader) errors
// and a declined fallback propagate; the caller always sees their original
// loader error (unwrapped) so errors.Is on their own sentinels still works.
func (s *Sampler[T]) finalize(v T, err error) (T, error) {
	var zero T
	if err == nil {
		return v, nil
	}
	var le *loaderError
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

// loaderError tags an error as originating from the caller's loader (vs an
// internal encode failure), so finalize can scope the fallback correctly.
type loaderError struct{ err error }

func (e *loaderError) Error() string { return e.err.Error() }
func (e *loaderError) Unwrap() error { return e.err }

// Cache value format: a JSON array "[score, updatedAt, data]". score is the
// data version and updatedAt the compute wall-clock; data is the sample.
func marshalSampleCache[T any](meta SampleMeta, data T) ([]byte, error) {
	return json.Marshal([3]any{meta.Score, meta.UpdatedAt, data})
}

func unmarshalSampleCache[T any](raw []byte) (SampleMeta, T, error) {
	var (
		arr  [3]json.RawMessage
		meta SampleMeta
		zero T
	)
	if err := json.Unmarshal(raw, &arr); err != nil {
		return meta, zero, err
	}
	if err := json.Unmarshal(arr[0], &meta.Score); err != nil {
		return meta, zero, err
	}
	if err := json.Unmarshal(arr[1], &meta.UpdatedAt); err != nil {
		return meta, zero, err
	}
	var data T
	if err := json.Unmarshal(arr[2], &data); err != nil {
		return meta, zero, err
	}
	return meta, data, nil
}
