package lake

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hkloudou/lake/v3/internal/cache"
	"github.com/hkloudou/lake/v3/internal/config"
	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/storage"
	"github.com/hkloudou/lake/v3/internal/xsync"
	"github.com/redis/go-redis/v9"
)

// Client is the main entry point for Lake v3.
//
// A Client is intended to live for the process lifetime. Background
// goroutines (Redis time updater, cache cleanup / stat) tick forever
// and are reclaimed by the OS at process exit; in-flight async
// snapshot saves are fire-and-forget (an interrupted save leaves at
// most one orphan OSS object, reaped by the next sweep).
type Client struct {
	rdb        *redis.Client // authoritative: snap hash, delta zset, seqid
	sampleRdb  *redis.Client // sample (memo) hash; defaults to rdb. Pluggable so cache-tier failures cannot threaten the authoritative store.
	writer     *index.Writer
	reader     *index.Reader
	configMgr  *config.Manager
	snapCache  cache.Cache
	deltaCache cache.Cache

	mu      sync.RWMutex
	storage storage.Storage
	config  *config.Config

	snapFlight   xsync.SingleFlight[string]   // dedupe concurrent snapshot saves on (catalog, stop)
	sampleFlight xsync.SingleFlight[string]   // dedupe concurrent Sampler[T] loaders on (catalog, indicator, score)
	clearFlight  xsync.SingleFlight[struct{}] // dedupe concurrent ClearHistory on (catalog)

	eventHandlers []EventHandler
}

type option struct {
	Storage            storage.Storage
	SnapCacheProvider  cache.Cache
	DeltaCacheProvider cache.Cache
	SampleRdb          *redis.Client // nil → use the authoritative rdb
}

// NewLake creates a Lake client. Config (storage backend, bucket, etc.)
// is loaded lazily on first operation from the Redis key "lake.setting".
func NewLake(metaUrl string, opts ...func(*option)) *Client {
	redisOpt, err := redis.ParseURL(metaUrl)
	if err != nil {
		redisOpt = &redis.Options{Addr: metaUrl}
	}
	rdb := redis.NewClient(redisOpt)

	o := &option{}
	for _, opt := range opts {
		opt(o)
	}
	if o.SnapCacheProvider == nil {
		o.SnapCacheProvider = cache.NewRedisCache(rdb, 2*time.Hour)
	}
	if o.DeltaCacheProvider == nil {
		o.DeltaCacheProvider = cache.NewMemoryCache(1 * time.Minute)
	}
	if o.SampleRdb == nil {
		o.SampleRdb = rdb // colocated by default
	}

	return &Client{
		rdb:          rdb,
		sampleRdb:    o.SampleRdb,
		writer:       index.NewWriter(rdb),
		reader:       index.NewReader(rdb),
		configMgr:    config.NewManager(rdb),
		storage:      o.Storage, // nil → loaded lazily
		snapCache:    o.SnapCacheProvider,
		deltaCache:   o.DeltaCacheProvider,
		snapFlight:   xsync.NewSingleFlight[string](),
		sampleFlight: xsync.NewSingleFlight[string](),
		clearFlight:  xsync.NewSingleFlight[struct{}](),
	}
}

func WithSnapCache(c cache.Cache) func(*option)   { return func(o *option) { o.SnapCacheProvider = c } }
func WithDeltaCache(c cache.Cache) func(*option)  { return func(o *option) { o.DeltaCacheProvider = c } }
func WithStorage(s storage.Storage) func(*option) { return func(o *option) { o.Storage = s } }

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

// WithSampleCacheRedis routes the Sampler memo hash ("<prefix>:m:*") to a
// separate Redis instance instead of the authoritative one. Sample is a
// derived cache — a separate hot tier can absorb scan / flush / restart
// without threatening the authoritative store. Defaults to the
// authoritative rdb when omitted.
func WithSampleCacheRedis(rdb *redis.Client) func(*option) {
	return func(o *option) { o.SampleRdb = rdb }
}

// WithSampleCacheURL is the URL form of WithSampleCacheRedis. Panics on
// an invalid URL (programmer error at construction time, per package policy).
func WithSampleCacheURL(url string) func(*option) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		panic(fmt.Errorf("lake: invalid sample-cache URL: %w", err))
	}
	return WithSampleCacheRedis(redis.NewClient(opt))
}

// ensureInitialized loads lake.setting on first use and wires the
// storage prefix into the index reader/writer. Idempotent and
// concurrent-safe.
func (c *Client) ensureInitialized(ctx context.Context) error {
	c.mu.RLock()
	if c.storage != nil {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.storage != nil {
		return nil
	}

	if c.config == nil {
		cfg, err := c.configMgr.Load(ctx)
		if err != nil {
			return fmt.Errorf("load lake.setting: %w", err)
		}
		c.config = cfg
	}
	stor, err := c.config.CreateStorage()
	if err != nil {
		return fmt.Errorf("create %s storage: %w", c.config.Storage, err)
	}
	c.storage = stor

	prefix := stor.RedisPrefix()
	c.writer.SetPrefix(prefix)
	c.reader.SetPrefix(prefix)
	return nil
}
