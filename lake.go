package lake

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/utils"
	"github.com/hkloudou/lake/v3/internal/xsync"
	"github.com/hkloudou/lake/v3/storage"
	"github.com/redis/go-redis/v9"
)

// Client is the entry point for Lake v3. Everything is wired explicitly at
// New: a key prefix, the authoritative index Redis, and a storage Resolver.
// Lake core never imports a cloud SDK; it only calls the Storage the Resolver
// returns. A Client has no background goroutines and nothing to close.
type Client struct {
	rdb       *redis.Client // index Redis (durable)
	idx       *index.Index
	sampleRdb *redis.Client // sample memo hashes; defaults to the index Redis
	resolve   storage.Resolver

	snapProvider  string // WithSnapTarget; "" disables auto-snapshotting
	snapBucket    string
	snapMinDeltas int

	storMu sync.Mutex
	stores map[string]storage.Storage // memoised per (kind, provider, bucket)

	snapSaving   sync.Map                   // per-catalog gate: one async snapshot save at a time
	sampleFlight xsync.SingleFlight[string] // dedupes concurrent sample loaders

	handlers atomic.Pointer[[]EventHandler]
	useMu    sync.Mutex
}

type option struct {
	sampleRdb     *redis.Client
	snapProvider  string
	snapBucket    string
	snapMinDeltas int
}

// New creates a Lake client. prefix namespaces every Redis key; rdb is the
// durable index Redis; resolve maps (kind, provider, bucket) to a Storage.
// Panics on a nil/empty argument (programmer error).
func New(prefix string, rdb *redis.Client, resolve storage.Resolver, opts ...func(*option)) *Client {
	if prefix == "" || rdb == nil || resolve == nil {
		panic("lake: New requires a prefix, an index *redis.Client and a storage.Resolver")
	}
	o := &option{sampleRdb: rdb, snapMinDeltas: 1}
	for _, fn := range opts {
		fn(o)
	}
	return &Client{
		rdb:           rdb,
		idx:           index.New(rdb, prefix),
		sampleRdb:     o.sampleRdb,
		resolve:       resolve,
		snapProvider:  o.snapProvider,
		snapBucket:    o.snapBucket,
		snapMinDeltas: o.snapMinDeltas,
		stores:        map[string]storage.Storage{},
		sampleFlight:  xsync.NewSingleFlight[string](),
	}
}

// WithSnapTarget sets where reads persist the snapshots they generate. Omit
// it, or pass both empty, to disable snapshotting (reads replay all deltas).
// Panics on an invalid provider/bucket: both are embedded in every snapshot
// URI, and one-empty-one-set can only be a config mistake.
func WithSnapTarget(provider, bucket string) func(*option) {
	if provider == "" && bucket == "" {
		return func(*option) {}
	}
	if err := utils.ValidateStorageProvider(provider); err != nil {
		panic(fmt.Errorf("lake: WithSnapTarget: %w", err))
	}
	if err := utils.ValidateStorageBucket(bucket); err != nil {
		panic(fmt.Errorf("lake: WithSnapTarget: %w", err))
	}
	return func(o *option) { o.snapProvider, o.snapBucket = provider, bucket }
}

// WithSnapMinDeltas sets how many deltas must accumulate past the current
// snapshot before a read persists a new one (default 1). A snapshot uploads
// the whole document, so on a large, hot catalog a higher n trades that
// write amplification for replaying up to n-1 deltas per read. Panics on n < 1.
func WithSnapMinDeltas(n int) func(*option) {
	if n < 1 {
		panic(fmt.Sprintf("lake: WithSnapMinDeltas requires n >= 1, got %d", n))
	}
	return func(o *option) { o.snapMinDeltas = n }
}

// WithSampleCacheRedis routes the sample memo hashes ("<prefix>:m:*") to a
// separate, evictable Redis. Defaults to the index Redis.
func WithSampleCacheRedis(rdb *redis.Client) func(*option) {
	return func(o *option) { o.sampleRdb = rdb }
}

// storageFor resolves and memoises the Storage for (kind, provider, bucket);
// the Resolver runs at most once per distinct triple.
func (c *Client) storageFor(kind storage.Kind, provider, bucket string) (storage.Storage, error) {
	if provider == "" || bucket == "" {
		return nil, fmt.Errorf("lake: empty provider/bucket (%q/%q)", provider, bucket)
	}
	key := fmt.Sprintf("%d|%s|%s", kind, provider, bucket)
	c.storMu.Lock()
	defer c.storMu.Unlock()
	if s := c.stores[key]; s != nil {
		return s, nil
	}
	s, err := c.resolve(kind, provider, bucket)
	if err != nil {
		return nil, fmt.Errorf("lake: resolve %s %s://%s: %w", kind, provider, bucket, err)
	}
	if s == nil {
		return nil, fmt.Errorf("lake: resolver returned nil storage for %s %s://%s", kind, provider, bucket)
	}
	c.stores[key] = s
	return s, nil
}
