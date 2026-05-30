package lake

import (
	"fmt"
	"sync"
	"time"

	"github.com/hkloudou/lake/v3/internal/cache"
	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/xsync"
	"github.com/hkloudou/lake/v3/storage"
	"github.com/redis/go-redis/v9"
)

// Client is the main entry point for Lake v3.
//
// Everything is wired explicitly at New: prefix (namespaces every Redis key
// and the seqid counter), the authoritative index Redis, and a storage
// Resolver. There is no lake.setting bootstrap and no global state — Lake core
// never imports a cloud SDK; it only ever calls the Storage the Resolver
// returns.
//
// A Client is intended to live for the process lifetime. Background goroutines
// (Redis time updater, cache cleanup) tick forever and are reclaimed by the OS
// at exit; in-flight async snapshot saves are fire-and-forget.
type Client struct {
	rdb        *redis.Client // authoritative: snap hash, delta zset, seqid
	sampleRdb  *redis.Client // sample (memo) hash; defaults to rdb
	writer     *index.Writer
	reader     *index.Reader
	snapCache  cache.Cache
	deltaCache cache.Cache

	resolve      storage.Resolver
	snapProvider string // WithSnapTarget; "" disables auto-snapshotting
	snapBucket   string

	storMu sync.RWMutex // guards stores
	stores map[string]storage.Storage

	snapFlight   xsync.SingleFlight[string] // dedupe concurrent snapshot saves on (catalog, stop)
	sampleFlight xsync.SingleFlight[string] // dedupe concurrent Sampler[T] loaders on (catalog, indicator, score)

	eventHandlers []EventHandler
}

type option struct {
	snapCache    cache.Cache
	deltaCache   cache.Cache
	sampleRdb    *redis.Client
	snapProvider string
	snapBucket   string
}

// New creates a Lake client.
//
//   - prefix   namespaces every Redis key (and the seqid counter).
//   - rdb      the authoritative index Redis (durable).
//   - resolve  the single storage-injection point: maps (provider, bucket) to
//     a bucket-scoped storage.Storage. Lake memoises the result per pair.
//
// Panics on a nil/empty required argument (programmer error, per package policy).
func New(prefix string, rdb *redis.Client, resolve storage.Resolver, opts ...func(*option)) *Client {
	if prefix == "" {
		panic("lake: New requires a non-empty prefix")
	}
	if rdb == nil {
		panic("lake: New requires an index *redis.Client")
	}
	if resolve == nil {
		panic("lake: New requires a storage.Resolver")
	}
	o := &option{}
	for _, fn := range opts {
		fn(o)
	}
	if o.snapCache == nil {
		o.snapCache = cache.NewRedisCache(rdb, 2*time.Hour)
	}
	if o.deltaCache == nil {
		o.deltaCache = cache.NewMemoryCache(1 * time.Minute)
	}
	if o.sampleRdb == nil {
		o.sampleRdb = rdb
	}
	c := &Client{
		rdb:          rdb,
		sampleRdb:    o.sampleRdb,
		writer:       index.NewWriter(rdb),
		reader:       index.NewReader(rdb),
		snapCache:    o.snapCache,
		deltaCache:   o.deltaCache,
		resolve:      resolve,
		snapProvider: o.snapProvider,
		snapBucket:   o.snapBucket,
		stores:       make(map[string]storage.Storage),
		snapFlight:   xsync.NewSingleFlight[string](),
		sampleFlight: xsync.NewSingleFlight[string](),
	}
	c.writer.SetPrefix(prefix)
	c.reader.SetPrefix(prefix)
	return c
}

func WithSnapCache(c cache.Cache) func(*option)  { return func(o *option) { o.snapCache = c } }
func WithDeltaCache(c cache.Cache) func(*option) { return func(o *option) { o.deltaCache = c } }

// WithSnapCacheMetaURL builds a Redis-backed snapshot cache from a URL.
func WithSnapCacheMetaURL(metaUrl string, ttl time.Duration) func(*option) {
	c, err := cache.NewRedisCacheWithURL(metaUrl, ttl)
	if err != nil {
		panic(err)
	}
	return WithSnapCache(c)
}

// WithDeltaCacheMetaURL builds a Redis-backed delta cache from a URL.
func WithDeltaCacheMetaURL(metaUrl string, ttl time.Duration) func(*option) {
	c, err := cache.NewRedisCacheWithURL(metaUrl, ttl)
	if err != nil {
		panic(err)
	}
	return WithDeltaCache(c)
}

// WithSnapTarget sets where Lake writes the snapshots it auto-generates on the
// read path (resolved via the client's Resolver). Omit it to disable
// auto-snapshotting: reads then replay all deltas from "{}" — correct, just
// slower for long histories.
func WithSnapTarget(provider, bucket string) func(*option) {
	return func(o *option) { o.snapProvider, o.snapBucket = provider, bucket }
}

// WithSampleCacheRedis routes the Sampler memo hash ("<prefix>:m:*") to a
// separate Redis instance. Defaults to the authoritative rdb.
func WithSampleCacheRedis(rdb *redis.Client) func(*option) {
	return func(o *option) { o.sampleRdb = rdb }
}

// WithSampleCacheURL is the URL form of WithSampleCacheRedis. Panics on an
// invalid URL (programmer error at construction time).
func WithSampleCacheURL(url string) func(*option) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		panic(fmt.Errorf("lake: invalid sample-cache URL: %w", err))
	}
	return WithSampleCacheRedis(redis.NewClient(opt))
}

// storageFor resolves and memoises a bucket-scoped Storage for (provider,
// bucket). The Resolver is invoked at most once per distinct pair.
func (c *Client) storageFor(provider, bucket string) (storage.Storage, error) {
	if provider == "" || bucket == "" {
		return nil, fmt.Errorf("lake: empty provider/bucket (%q/%q)", provider, bucket)
	}
	key := provider + "|" + bucket
	c.storMu.RLock()
	s := c.stores[key]
	c.storMu.RUnlock()
	if s != nil {
		return s, nil
	}
	c.storMu.Lock()
	defer c.storMu.Unlock()
	if s = c.stores[key]; s != nil {
		return s, nil
	}
	s, err := c.resolve(provider, bucket)
	if err != nil {
		return nil, fmt.Errorf("lake: resolve %s://%s: %w", provider, bucket, err)
	}
	if s == nil {
		return nil, fmt.Errorf("lake: resolver returned nil storage for %s://%s", provider, bucket)
	}
	c.stores[key] = s
	return s, nil
}
