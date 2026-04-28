package lake

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hkloudou/lake/v3/internal/cache"
	"github.com/hkloudou/lake/v3/internal/config"
	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/storage"
	"github.com/hkloudou/lake/v3/internal/xsync"
	"github.com/redis/go-redis/v9"
)

// Client is the main interface for Lake v3.
//
// A Client owns several long-lived background goroutines (Redis time
// updater, cache cleanup, cache stat logger) plus an unbounded number of
// short-lived async snapshot writers. Call Close to drain them on shutdown.
// After Close the Client must not be used again.
type Client struct {
	rdb    *redis.Client
	writer *index.Writer
	reader *index.Reader

	configMgr  *config.Manager
	snapCache  cache.Cache // Snapshot cache (Redis or NoOp)
	deltaCache cache.Cache // Delta file cache (Memory)

	mu      sync.RWMutex
	storage storage.Storage
	config  *config.Config

	snapFlight  xsync.SingleFlight[string]
	clearFlight xsync.SingleFlight[struct{}]

	// snapWG accounts for in-flight async snapshot saves spawned by readData
	// so Close can wait for them to finish.
	snapWG sync.WaitGroup

	closeOnce sync.Once

	eventHandlers []EventHandler
}

// Option is a function that configures the client
type option struct {
	Storage            storage.Storage
	SnapCacheProvider  cache.Cache
	DeltaCacheProvider cache.Cache
}

// NewLake creates a new Lake client with the given Redis URL.
// Config (storage backend, bucket, AES key, etc.) is loaded lazily on the
// first operation from the Redis key "lake.setting".
//
// The returned Client must be closed with Close to release background
// goroutines.
func NewLake(metaUrl string, opts ...func(*option)) *Client {
	redisOpt, err := redis.ParseURL(metaUrl)
	if err != nil {
		// Fallback: treat the input as a plain address.
		redisOpt = &redis.Options{Addr: metaUrl}
	}
	rdb := redis.NewClient(redisOpt)

	option := &option{}
	for _, opt := range opts {
		opt(option)
	}

	writer := index.NewWriter(rdb)
	reader := index.NewReader(rdb)
	configMgr := config.NewManager(rdb)

	snapCache := option.SnapCacheProvider
	if snapCache == nil {
		snapCache = cache.NewRedisCache(rdb, 2*time.Hour)
	}

	deltaCache := option.DeltaCacheProvider
	if deltaCache == nil {
		deltaCache = cache.NewMemoryCache(1 * time.Minute)
	}

	return &Client{
		rdb:         rdb,
		writer:      writer,
		reader:      reader,
		configMgr:   configMgr,
		storage:     option.Storage, // may be nil, loaded lazily
		snapCache:   snapCache,
		deltaCache:  deltaCache,
		snapFlight:  xsync.NewSingleFlight[string](),
		clearFlight: xsync.NewSingleFlight[struct{}](),
	}
}

// WithSnapCache injects a custom snapshot cache.
func WithSnapCache(cacheProvider cache.Cache) func(*option) {
	return func(opt *option) { opt.SnapCacheProvider = cacheProvider }
}

// WithSnapCacheMetaURL builds a Redis-backed snapshot cache from a URL.
// The cache owns the resulting redis.Client and will close it on Close.
func WithSnapCacheMetaURL(metaUrl string, ttl time.Duration) func(*option) {
	cacheProvider, err := cache.NewRedisCacheWithURL(metaUrl, ttl)
	if err != nil {
		panic(err)
	}
	return WithSnapCache(cacheProvider)
}

// WithDeltaCache injects a custom delta cache.
func WithDeltaCache(cacheProvider cache.Cache) func(*option) {
	return func(opt *option) { opt.DeltaCacheProvider = cacheProvider }
}

// WithDeltaCacheMetaURL builds a Redis-backed delta cache from a URL.
// The cache owns the resulting redis.Client and will close it on Close.
func WithDeltaCacheMetaURL(metaUrl string, ttl time.Duration) func(*option) {
	cacheProvider, err := cache.NewRedisCacheWithURL(metaUrl, ttl)
	if err != nil {
		panic(err)
	}
	return WithDeltaCache(cacheProvider)
}

// WithStorage injects a custom storage backend, bypassing lake.setting.
func WithStorage(storage storage.Storage) func(*option) {
	return func(opt *option) { opt.Storage = storage }
}

// Close shuts the Client down: it stops the Reader's background time-sync
// goroutine, drains any in-flight async snapshot saves, then closes the
// snapshot/delta caches (if they implement io.Closer) and the main
// redis.Client.
//
// Close is idempotent and safe to defer. After Close, no other Client
// method may be called.
func (c *Client) Close() error {
	var firstErr error
	c.closeOnce.Do(func() {
		// 1. Stop background goroutines so no new work is started.
		if c.reader != nil {
			c.reader.Close()
		}

		// 2. Drain in-flight snapshot saves. They use context.Background()
		//    so this can take as long as the slowest object-storage Put.
		c.snapWG.Wait()

		// 3. Close caches if they support it (RedisCache, MemoryCache do;
		//    NoOpCache does not).
		if closer, ok := c.snapCache.(io.Closer); ok {
			if err := closer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if closer, ok := c.deltaCache.(io.Closer); ok {
			if err := closer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}

		// 4. Close the main redis.Client (the one created from metaUrl).
		if c.rdb != nil {
			if err := c.rdb.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	})
	return firstErr
}

// ensureInitialized loads lake.setting on first use and wires the storage
// prefix into the index reader/writer. Idempotent and safe under concurrent
// callers.
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
			return fmt.Errorf("failed to load config from Redis (lake.setting): %w", err)
		}
		c.config = cfg
	}

	stor, err := c.config.CreateStorage()
	if err != nil {
		return fmt.Errorf("failed to create %s storage: %w", c.config.Storage, err)
	}
	c.storage = stor

	prefix := c.storage.RedisPrefix()
	c.writer.SetPrefix(prefix)
	c.reader.SetPrefix(prefix)
	return nil
}
