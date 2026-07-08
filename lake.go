package lake

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

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
// A Client is intended to live for the process lifetime; embedders that
// create short-lived Clients (tests, multi-tenant hosts) should Close them to
// stop the background Redis-clock ticker. In-flight async snapshot saves are
// fire-and-forget. Read-path caching, if any, lives in the Storage the
// Resolver returns (see storage/cached).
type Client struct {
	rdb       *redis.Client // authoritative: snap hash, delta zset, seqid
	sampleRdb *redis.Client // sample (memo) hash; defaults to rdb
	writer    *index.Writer
	reader    *index.Reader

	resolve      storage.Resolver
	snapProvider string // WithSnapTarget; "" disables auto-snapshotting
	snapBucket   string

	storMu     sync.RWMutex // guards stores
	stores     map[string]storage.Storage
	storFlight xsync.SingleFlight[storage.Storage] // dedupe concurrent resolves per (kind, provider, bucket)

	// Two layers with distinct jobs: snapSaving is the cheap per-catalog gate
	// that keeps readData from even COPYING the document while a save is in
	// flight (claimSnapSlot, read.go); snapFlight dedupes identical
	// (catalog, stop, gen) saves inside saveSnapshot itself, covering direct
	// callers and stolen-slot overlaps the gate cannot see.
	snapFlight   xsync.SingleFlight[string] // dedupe concurrent snapshot saves on (catalog, stop, gen)
	snapSaving   sync.Map                   // catalog → claim time.Time of the in-flight async save
	sampleFlight xsync.SingleFlight[string] // dedupe concurrent Sampler[T] loaders on (catalog, indicator, score)

	handleSecret []byte // WithHandleSecret; empty disables handle signing

	eventHandlers atomic.Pointer[[]EventHandler]
	ownsSampleRdb bool // Close() closes sampleRdb only when Lake created it
	closeOnce     sync.Once
}

type option struct {
	sampleRdb     *redis.Client
	ownsSampleRdb bool
	snapProvider  string
	snapBucket    string
	handleSecret  []byte
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
		rdb:           rdb,
		sampleRdb:     o.sampleRdb,
		ownsSampleRdb: o.ownsSampleRdb,
		writer:        index.NewWriter(rdb),
		reader:        index.NewReader(rdb),
		resolve:       resolve,
		snapProvider:  o.snapProvider,
		snapBucket:    o.snapBucket,
		handleSecret:  o.handleSecret,
		stores:        make(map[string]storage.Storage),
		storFlight:    xsync.NewSingleFlight[storage.Storage](),
		snapFlight:    xsync.NewSingleFlight[string](),
		sampleFlight:  xsync.NewSingleFlight[string](),
	}
	c.writer.SetPrefix(prefix)
	c.reader.SetPrefix(prefix)
	return c
}

// Close releases the Client's background resources: it stops the Redis-clock
// ticker and closes the sample-cache Redis client iff Lake itself created it
// (WithSampleCacheURL). The rdb and any client passed via WithSampleCacheRedis
// belong to the caller and are left open. A closed Client keeps serving
// (the clock falls back to its last synced value, then the local clock), but
// long-lived processes should treat Close as final. Idempotent.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.reader.Close()
		if c.ownsSampleRdb && c.sampleRdb != nil {
			err = c.sampleRdb.Close()
		}
	})
	return err
}

// WithSnapTarget sets where Lake writes the snapshots it auto-generates on the
// read path (resolved via the client's Resolver). Omit it to disable
// auto-snapshotting: reads then replay all deltas from "{}" — correct, just
// slower for long histories.
func WithSnapTarget(provider, bucket string) func(*option) {
	return func(o *option) { o.snapProvider, o.snapBucket = provider, bucket }
}

// WithSampleCacheRedis routes the Sampler memo hash ("<prefix>:m:*") to a
// separate Redis instance. Defaults to the authoritative rdb. The client
// stays owned by the caller — Close never touches it.
func WithSampleCacheRedis(rdb *redis.Client) func(*option) {
	return func(o *option) {
		o.closeOwnedSampleRdb() // a Lake-created client this overrides must not leak
		o.sampleRdb = rdb
		o.ownsSampleRdb = false
	}
}

// closeOwnedSampleRdb releases a Lake-created sample client that a later
// option is about to override — otherwise its pool would leak with no owner.
func (o *option) closeOwnedSampleRdb() {
	if o.ownsSampleRdb && o.sampleRdb != nil {
		_ = o.sampleRdb.Close()
	}
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
// invalid URL (programmer error at construction time). The Redis client it
// creates is owned by Lake and closed by Client.Close.
func WithSampleCacheURL(url string) func(*option) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		panic(fmt.Errorf("lake: invalid sample-cache URL: %w", err))
	}
	return func(o *option) {
		o.closeOwnedSampleRdb()
		o.sampleRdb = redis.NewClient(opt)
		o.ownsSampleRdb = true
	}
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
	key := strconv.Itoa(int(kind)) + "|" + provider + "|" + bucket
	c.storMu.RLock()
	s := c.stores[key]
	c.storMu.RUnlock()
	if s != nil {
		return s, nil
	}
	// Resolve OUTSIDE the map lock: resolve is user code that may do real I/O
	// (SDK setup, STS credentials). Holding the write lock across it would
	// stall every concurrent lookup — including already-memoised ones. The
	// flight dedupes concurrent resolves of the SAME triple; distinct triples
	// resolve in parallel.
	return c.storFlight.Do(key, func() (storage.Storage, error) {
		c.storMu.RLock()
		s := c.stores[key]
		c.storMu.RUnlock()
		if s != nil {
			return s, nil
		}
		s, err := c.resolve(kind, provider, bucket)
		if err != nil {
			return nil, fmt.Errorf("lake: resolve %s %s://%s: %w", kind, provider, bucket, err)
		}
		if s == nil {
			return nil, fmt.Errorf("lake: resolver returned nil storage for %s %s://%s", kind, provider, bucket)
		}
		c.storMu.Lock()
		c.stores[key] = s
		c.storMu.Unlock()
		return s, nil
	})
}
