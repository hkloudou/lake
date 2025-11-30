package lake

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hkloudou/lake/v2/internal/cache"
	"github.com/hkloudou/lake/v2/internal/config"
	"github.com/hkloudou/lake/v2/internal/index"
	"github.com/hkloudou/lake/v2/internal/storage"
	"github.com/hkloudou/lake/v2/internal/xsync"
	"github.com/redis/go-redis/v9"
)

// Client is the main interface for Lake v2
type Client struct {
	rdb    *redis.Client
	writer *index.Writer
	reader *index.Reader

	// merger     *merge.Engine // Legacy (deprecated)
	configMgr  *config.Manager
	snapCache  cache.Cache // Snapshot cache (Redis or NoOp)
	deltaCache cache.Cache // Delta file cache (Memory, 10min TTL)

	// Lazy-loaded components
	mu      sync.RWMutex
	storage storage.Storage
	config  *config.Config
	// redisTimeUnix int64

	snapFlight   xsync.SingleFlight[string]
	sampleFlight xsync.SingleFlight[float64]
	clearFlight  xsync.SingleFlight[struct{}] // Prevents concurrent clear operations on same catalog
}

// Option is a function that configures the client
type option struct {
	Storage            storage.Storage
	SnapCacheProvider  cache.Cache
	DeltaCacheProvider cache.Cache
}

// NewLake creates a new Lake client with the given Redis URL
// Config is loaded lazily on first operation
func NewLake(metaUrl string, opts ...func(*option)) *Client {
	// Parse Redis URL
	redisOpt, err := redis.ParseURL(metaUrl)
	if err != nil {
		// Fallback to treating it as an address
		redisOpt = &redis.Options{
			Addr: metaUrl,
		}
	}

	rdb := redis.NewClient(redisOpt)

	// Apply options
	option := &option{}
	for _, opt := range opts {
		opt(option)
	}

	writer := index.NewWriter(rdb)
	reader := index.NewReader(rdb)
	// merger := merge.NewEngine()
	configMgr := config.NewManager(rdb)

	// Use provided cache or default to main Redis cache with 1 hour TTL
	snapCache := option.SnapCacheProvider
	if snapCache == nil {
		snapCache = cache.NewRedisCache(rdb, 2*time.Hour)
	}

	// Use provided cache or default to memory cache with 1 minute TTL
	deltaCache := option.DeltaCacheProvider
	if deltaCache == nil {
		deltaCache = cache.NewMemoryCache(1 * time.Minute) //only keep 1 minute of delta files in memory
	}

	client := &Client{
		rdb:    rdb,
		writer: writer,
		reader: reader,
		// merger:     merger,
		configMgr:    configMgr,
		storage:      option.Storage, // May be nil, will be loaded lazily
		snapCache:    snapCache,
		deltaCache:   deltaCache,
		snapFlight:   xsync.NewSingleFlight[string](),
		sampleFlight: xsync.NewSingleFlight[float64](),
		clearFlight:  xsync.NewSingleFlight[struct{}](),
	}
	// client.startRedisTimeUnixUpdater()
	return client
}

// WithCache returns an option function that sets the cache provider
func WithSnapCache(cacheProvider cache.Cache) func(*option) {
	return func(opt *option) {
		opt.SnapCacheProvider = cacheProvider
	}
}

// WithCache returns an option function that sets the cache provider
func WithSnapCacheMetaURL(metaUrl string, ttl time.Duration) func(*option) {
	cacheProvider, err := cache.NewRedisCacheWithURL(metaUrl, ttl)
	if err != nil {
		panic(err)
	}
	return WithSnapCache(cacheProvider)
}

// WithCache returns an option function that sets the cache provider
func WithDeltaCache(cacheProvider cache.Cache) func(*option) {
	return func(opt *option) {
		opt.DeltaCacheProvider = cacheProvider
	}
}

// WithCache returns an option function that sets the cache provider
func WithDeltaCacheMetaURL(metaUrl string, ttl time.Duration) func(*option) {
	cacheProvider, err := cache.NewRedisCacheWithURL(metaUrl, ttl)
	if err != nil {
		panic(err)
	}
	return WithDeltaCache(cacheProvider)
}

// WithStorage returns an option function that sets the storage provider
func WithStorage(storage storage.Storage) func(*option) {
	return func(opt *option) {
		opt.Storage = storage
	}
}

// ensureInitialized ensures storage and snapMgr are initialized
// Loads config from Redis if not already loaded
func (c *Client) ensureInitialized(ctx context.Context) error {
	c.mu.RLock()
	if c.storage != nil {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if c.storage != nil {
		return nil
	}

	// Load config and initialize storage if not provided
	if c.storage == nil {
		// Load config from Redis if not already loaded
		if c.config == nil {
			cfg, err := c.configMgr.Load(ctx)
			if err != nil {
				return fmt.Errorf("failed to load config from Redis (lake.setting): %w", err)
			}
			c.config = cfg
		}

		// Create storage from config - must succeed, no fallback
		stor, err := c.config.CreateStorage()
		if err != nil {
			return fmt.Errorf("failed to create %s storage: %w", c.config.Storage, err)
		}
		c.storage = stor

		// Set index prefix based on config: Storage:Name
	}
	prefix := c.storage.RedisPrefix()
	c.writer.SetPrefix(prefix)
	c.reader.SetPrefix(prefix)

	// Initialize snapshot manager
	// if c.snapMgr == nil {
	// 	c.snapMgr = snapshot.NewManager(c.storage, c.reader, c.writer)
	// }

	return nil
}
