package lake

import (
	"fmt"
	"sync"

	"github.com/hkloudou/lake/v3/internal/index"
	"github.com/hkloudou/lake/v3/internal/utils"
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
// (the Redis time updater) tick forever and are reclaimed by the OS at exit;
// in-flight async snapshot saves are fire-and-forget. Read-path caching, if
// any, lives in the Storage the Resolver returns (see storage/cached).
type Client struct {
	rdb       *redis.Client // authoritative: snap hash, delta zset, seqid
	sampleRdb *redis.Client // sample (memo) hash; defaults to rdb
	writer    *index.Writer
	reader    *index.Reader

	resolve      storage.Resolver
	snapProvider string // WithSnapTarget; "" disables auto-snapshotting
	snapBucket   string

	storMu sync.RWMutex // guards stores
	stores map[string]storage.Storage

	snapFlight   xsync.SingleFlight[string] // dedupe concurrent snapshot saves on (catalog, stop)
	sampleFlight xsync.SingleFlight[string] // dedupe concurrent Sampler[T] loaders on (catalog, indicator, score)

	handleSecret []byte // WithHandleSecret; empty disables handle signing

	eventHandlers []EventHandler
}

type option struct {
	sampleRdb    *redis.Client
	snapProvider string
	snapBucket   string
	handleSecret []byte
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
	if o.sampleRdb == nil {
		o.sampleRdb = rdb
	}
	c := &Client{
		rdb:          rdb,
		sampleRdb:    o.sampleRdb,
		writer:       index.NewWriter(rdb),
		reader:       index.NewReader(rdb),
		resolve:      resolve,
		snapProvider: o.snapProvider,
		snapBucket:   o.snapBucket,
		handleSecret: o.handleSecret,
		stores:       make(map[string]storage.Storage),
		snapFlight:   xsync.NewSingleFlight[string](),
		sampleFlight: xsync.NewSingleFlight[string](),
	}
	c.writer.SetPrefix(prefix)
	c.reader.SetPrefix(prefix)
	return c
}

// WithSnapTarget sets where Lake writes the snapshots it auto-generates on the
// read path (resolved via the client's Resolver). Omit it — or pass both
// arguments empty — to disable auto-snapshotting: reads then replay all
// deltas from "{}" — correct, just slower for long histories. (Both-empty
// stays a valid "disabled" spelling so config-driven callers can pass unset
// values through.)
//
// Panics on an invalid provider/bucket (programmer error at construction
// time): both are embedded in every snapshot's URI (provider://bucket/path),
// and a "/" or ":" inside either part would make the recorded locator parse
// back to a different bucket — every read of a snapshotted catalog would
// then fail. One-empty-one-set is also a panic: it silently disabled
// snapshotting before, which can only be a config mistake.
func WithSnapTarget(provider, bucket string) func(*option) {
	if provider == "" && bucket == "" {
		return func(*option) {} // explicit "disabled"
	}
	if err := utils.ValidateStorageProvider(provider); err != nil {
		panic(fmt.Errorf("lake: WithSnapTarget: %w", err))
	}
	if err := utils.ValidateStorageBucket(bucket); err != nil {
		panic(fmt.Errorf("lake: WithSnapTarget: %w", err))
	}
	return func(o *option) { o.snapProvider, o.snapBucket = provider, bucket }
}

// WithSampleCacheRedis routes the Sampler memo hash ("<prefix>:m:*") to a
// separate Redis instance. Defaults to the authoritative rdb.
func WithSampleCacheRedis(rdb *redis.Client) func(*option) {
	return func(o *option) { o.sampleRdb = rdb }
}

// WithHandleSecret turns on WriteHandle signing. WriteBegin stamps every
// handle with an HMAC-SHA256 over its identity fields, and WriteNotify
// rejects any handle whose signature is missing or does not match — so a
// handle that round-tripped through an untrusted client provably carries
// exactly the Catalog/Path/MergeType/UUID/URI/ExpiresAt that WriteBegin
// issued — and, because ExpiresAt is then trustworthy, WriteNotify also
// rejects handles past it (no indefinite replay of a leaked handle).
// (Without a secret, notify still enforces the structural URI↔catalog/uuid
// binding; the secret additionally pins Path, MergeType and ExpiresAt.)
// Every process sharing the prefix must be given the same secret.
// Panics on an empty secret (programmer error at construction time).
func WithHandleSecret(secret []byte) func(*option) {
	if len(secret) == 0 {
		panic("lake: WithHandleSecret requires a non-empty secret")
	}
	return func(o *option) { o.handleSecret = append([]byte(nil), secret...) }
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

// storageFor resolves and memoises a bucket-scoped Storage for (kind, provider,
// bucket). The Resolver is invoked at most once per distinct triple, so the same
// bucket can yield different storages per kind (e.g. cached snap, bare delta).
func (c *Client) storageFor(kind storage.Kind, provider, bucket string) (storage.Storage, error) {
	if provider == "" || bucket == "" {
		return nil, fmt.Errorf("lake: empty provider/bucket (%q/%q)", provider, bucket)
	}
	// Numeric kind, not kind.String(): the memo key is identity, so it must not
	// depend on a display string a future Kind could alias.
	key := fmt.Sprintf("%d|%s|%s", kind, provider, bucket)
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
