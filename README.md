# Lake V3

[![Go Version](https://img.shields.io/badge/go-%3E%3D1.25-blue)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Release](https://img.shields.io/github/v/release/hkloudou/lake)](https://github.com/hkloudou/lake/releases)

> Distributed JSON document store with atomic writes, RFC-standard merging,
> snapshot acceleration, and computed sampling.

> **⚠️ Status: not production-ready.** No current release is sanctioned for
> production use — v3 is alpha (its public API may still change before `v3.0.0`
> stable), and the v2 line is not recommended for new production use either.
> See [Migrating from v2 to v3](#-migrating-from-v2-to-v3).

## ✨ Key Features

- **🔒 Atomic Writes** — direct-upload then notify; the index entry (and its
  tsSeq) is allocated only after the upload succeeds, so a slow / aborted upload
  never appears in the index — no pending phase, nothing to roll back
- **📜 RFC Standard** — Full RFC 7396 (JSON Merge Patch), plus simple field Replace
- **⚡ High Throughput** — Up to 999,999 writes/sec per catalog (Lua-bound seqid)
- **🧩 Storage-agnostic** — Lake core imports no cloud SDK. You inject one
  `func(provider, bucket) (Storage, error)` resolver; each delta records its own
  `provider://bucket/path` locator, so a catalog's bodies can span buckets/clouds
- **💾 Composable Caching** — opt-in `storage/cached` decorator wraps any backend
  in your resolver (read-through Get + write-through Put); a snapshot save warms
  the cache, so the next read skips a cold object-store fetch
- **🎯 Snapshot Acceleration** — read-path packed snapshots, async generation
- **🧮 Generic Sampling** — `NewSampler[T]` computes derived data on demand with
  a layered staleness policy; replaces v2's separate "meta" concept
- **🔍 Event Middleware** — `client.Use(handler)` for logging / monitoring

## 🚀 Quick Start

### Installation

```bash
go get github.com/hkloudou/lake/v3@latest
```

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "log"
    "net/http"
    "bytes"

    "github.com/hkloudou/lake/v3"
    "github.com/hkloudou/lake/v3/storage"
    lakeoss "github.com/hkloudou/lake/v3/storage/oss"
    "github.com/redis/go-redis/v9"
)

func main() {
    // 1. Wire the pieces explicitly — no lake.setting, no global state.
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    oss, _ := lakeoss.New(lakeoss.Config{Endpoint: "oss-cn-hangzhou", AccessKey: ak, SecretKey: sk})

    resolve := func(provider, bucket string) (storage.Storage, error) {
        switch provider {
        case "oss":
            return oss.Bucket(bucket), nil
        }
        return nil, fmt.Errorf("unknown provider %q", provider)
    }

    client := lake.New("my-lake", rdb, resolve,
        lake.WithSnapTarget("oss", "my-bucket"), // where auto-snapshots go
    )

    ctx := context.Background()
    body := []byte(`{"name":"Alice","age":30}`)

    // 2. Reserve a UUID + signed PUT URL for the chosen (provider, bucket).
    h, err := client.WriteBegin(ctx, lake.WriteBeginRequest{
        Catalog:   "users",
        Path:      "/profile",
        MergeType: lake.MergeTypeReplace,
        Provider:  "oss",
        Bucket:    "my-bucket",
    })
    if err != nil {
        log.Fatal(err)
    }

    // 3. Upload the body directly to OSS. Bytes never pass through Lake.
    req, _ := http.NewRequestWithContext(ctx, h.UploadMethod, h.UploadURL, bytes.NewReader(body))
    for k, v := range h.UploadHeaders {
        req.Header.Set(k, v)
    }
    if _, err := http.DefaultClient.Do(req); err != nil {
        log.Fatal(err)
    }

    // 4. Notify Lake — records the delta (carrying h.URI). No storage op here.
    if err := client.WriteNotify(ctx, h); err != nil {
        log.Fatal(err)
    }

    list := client.List(ctx, "users")
    jsonStr, _ := lake.ReadString(ctx, list)
    fmt.Printf("Data: %s\n", jsonStr)
}
```

## 📚 API Reference

### Client creation

```go
func New(prefix string, rdb *redis.Client, resolve storage.Resolver, opts ...func(*option)) *Client
```

| Argument | Description |
|----------|-------------|
| `prefix` | Namespaces every Redis key and the seqid counter |
| `rdb` | The authoritative **index** Redis (must persist) |
| `resolve` | The single storage-injection point: `func(provider, bucket string) (storage.Storage, error)` |

| Option | Description |
|--------|-------------|
| `WithSnapTarget(provider, bucket)` | Where Lake writes auto-generated snapshots. Omit → no auto-snapshotting (reads replay all deltas) |
| `WithSampleCacheURL(url)` / `WithSampleCacheRedis(rdb)` | Route the Sampler memo hash (`<prefix>:m:*`) to a separate Redis |
| `(*Client) Use(handler EventHandler)` | Register an event handler |

`New` panics on an empty `prefix`, nil `rdb`, or nil `resolve` (programmer error).

### Storage

Lake core is storage-agnostic — it never imports a cloud SDK. You provide a
**Resolver** that maps a `(provider, bucket)` pair to a bucket-scoped
`storage.Storage`. Ready-made backends ship as optional subpackages you use
*inside* your resolver:

| Package | Constructor | Presign |
|---------|-------------|---------|
| `storage/oss` | `oss.New(oss.Config{...}) → (*Client).Bucket(name)` | ✅ |
| `storage/file` | `file.New(basePath) → (*FS).Bucket(name)` | ❌ |
| `storage/mem` | `mem.New() → (*Store).Bucket(name)` | ❌ (tests) |

```go
type Storage interface {
    // path locates the object; catalog is context (lifecycle / metrics).
    Get(ctx context.Context, catalog, path string) ([]byte, error)
    Put(ctx context.Context, catalog, path string, data []byte) error
}
type Presigner interface { // optional; OSS-class only
    PresignPut(ctx context.Context, catalog, path string, opts PresignOptions) (PresignedUpload, error)
}
type Resolver func(provider, bucket string) (Storage, error)
```

Lake memoises the resolved `Storage` per `(provider, bucket)`, so your resolver
is called at most once per distinct pair. Put credential / endpoint / pooling /
multi-account routing inside the closure.

`storage/cached` is a decorator, not a backend: `cached.Wrap(namespace, backend, cache)`
adds read-through (Get) and write-through (Put) caching to any `Storage`, and
`cached.Resolver(inner, policy)` applies a per-`(provider, bucket)` cache across a
whole resolver. A snapshot save warms the cache so the next read skips a cold
object-store fetch — see **Configuration** below.

### Write — three-step direct upload

Client bytes never traverse the Lake process. The write target (provider +
bucket) is chosen **per write** and recorded in the delta.

| Function | Description |
|----------|-------------|
| `(*Client) WriteBegin(ctx, WriteBeginRequest, opts...) (*WriteHandle, error)` | Reserve a UUID, derive the object path, presign a PUT against `(Provider, Bucket)`. **No Redis op.** |
| (HTTP PUT to `handle.UploadURL`) | The client uploads bytes directly using the signed URL + `handle.UploadHeaders`. |
| `(*Client) WriteNotify(ctx, *WriteHandle) error` | Allocate the tsSeq and atomically record the delta (carrying `handle.URI`). **No storage op.** |

```go
type WriteBeginRequest struct {
    Catalog   string    `json:"catalog"`
    Path      string    `json:"path"`      // "/" means root
    MergeType MergeType `json:"mergeType"` // 1=Replace, 2=RFC7396
    Provider  string    `json:"provider"`  // storage provider, e.g. "oss"
    Bucket    string    `json:"bucket"`    // target bucket
}

type WriteHandle struct {
    Catalog       string            `json:"catalog"`
    Path          string            `json:"path"`
    MergeType     MergeType         `json:"mergeType"`
    UUID          string            `json:"uuid"`
    Provider      string            `json:"provider"`
    Bucket        string            `json:"bucket"`
    Key           string            `json:"key"` // object path within the bucket
    URI           string            `json:"uri"` // provider://bucket/key — recorded in the delta
    UploadURL     string            `json:"uploadURL"`
    UploadMethod  string            `json:"uploadMethod"`
    UploadHeaders map[string]string `json:"uploadHeaders"`
    ExpiresAt     int64             `json:"expiresAt"` // unix seconds
}
```

**Begin options**: `WithUploadTTL(d)`, `WithMaxBodyBytes(n)`, `WithUploadContentType(ct)`.

> **Presign capability**: WriteBegin requires the resolved backend to implement
> `storage.Presigner`. OSS supports it; file / memory return
> `lake.ErrPresignNotSupported`.
>
> **Bodies are stored RAW** — for at-rest encryption use OSS SSE; compress
> client-side if you want it.

**MergeType constants**:

```go
lake.MergeTypeReplace  // = 1: simple field replacement
lake.MergeTypeRFC7396  // = 2: RFC 7396 JSON Merge Patch (null removes)
```

### Read

| Function | Description |
|----------|-------------|
| `(*Client) List(ctx, catalog) *ListResult` | Snapshot info + delta index (1 HGet + 1 ZRange) |
| `(*Client) BatchList(ctx, catalogs) map[string]*ListResult` | Batched list across N catalogs in 2 round-trips |
| `ReadBytes / ReadString / ReadMap(ctx, *ListResult)` | Merged document as bytes / string / map |
| `Read[T any](ctx, *ListResult) (*T, error)` | Generic typed read |

```go
list := client.List(ctx, "users")
if list.Err != nil {
    return list.Err
}
jsonStr, err := lake.ReadString(ctx, list)
profile, err := lake.Read[UserProfile](ctx, list)
```

Read resolves each delta/snap by its stored URI (`provider://bucket/path` →
resolver → `Get`), merges in score order, and — if `WithSnapTarget` is set —
asynchronously persists a fresh snapshot off the read critical path.

### Sample (computed, cached)

`NewSampler[T]` is the single entry point for deriving secondary state from a
catalog. Construct one per `(indicator, T, loader)` and reuse it; it memoises
each catalog's computed value in the `<prefix>:m:<indicator>` Redis hash and
recomputes only when stale.

| API | Description |
|-----|-------------|
| `NewSampler[T](indicator, loader, …opts) *Sampler[T]` | Build a reusable sampler |
| `(*Sampler[T]) Sample(ctx, *ListResult) (T, error)` | One catalog: hit → 1 HGET, miss → loader + HSET |
| `(*Sampler[T]) Batch(ctx, map[string]*ListResult) map[string]*SampleResult[T]` | Many catalogs: 1 HMGET + concurrent loaders for misses |

```go
sampler := lake.NewSampler[Report]("daily",
    func(l *lake.ListResult) (Report, error) {
        data, err := lake.ReadMap(ctx, l)
        if err != nil {
            return Report{}, err
        }
        return buildReport(data), nil
    },
    lake.WithMaxAge(time.Hour),                              // recompute hourly even if unchanged
    lake.WithLoaderErrorDefault(Report{Status: "degraded"}), // served on loader error, never cached
)

report, err := sampler.Sample(ctx, client.List(ctx, "users"))
```

Staleness is layered: a **data-version floor** (always on) recomputes when the
catalog advanced past the cached version; `WithMaxAge(d)` and a custom
`WithShouldRefresh(fn)` predicate can only *add* recomputes. Loader errors (and
their `WithLoaderErrorDefault` / `WithLoaderErrorFallback` substitutes) are
per-call and never written back, so a transient blip can't freeze a degraded
value into the cache. The memo hash may live on a dedicated cache-tier Redis
(`WithSampleCacheURL`); it's a derived cache — flush/restart merely recomputes.

### Backup

| Function | Description |
|----------|-------------|
| `(*Client) IterateSnaps(ctx, fn) error` | Stream each `(catalog, snap)` via HSCAN; stop when `fn` returns false |

`IterateSnaps` is the single enumeration primitive. Each `snap.URI` is a
complete object locator, so backup tooling can copy snapshots straight to an
archive. Accumulate a map inside `fn` if you want the whole set; Lake
intentionally bundles no map helper and no archive step — that belongs in a
caller-side `cmd`, not the core library.

```go
err := client.IterateSnaps(ctx, func(catalog string, snap lake.SnapInfo) bool {
    return copyToArchive(ctx, snap.URI) == nil // stop on first failure
})
```

## 📖 Core Concepts

### Path format (the JSON field path)

- Must start with `/`; must not end with `/`
- Each segment starts with a letter / `_` / `$` (no leading digit)
- `/` alone means the whole document

### Storage URI

Each delta records where its body lives as `provider://bucket/path`, a complete
and portable object locator (`ossutil cp oss://bucket/path .` just works). The
object path is a Lake convention:

```
{md5(catalog)[0:4]}/{encoded(catalog)}/{uuid}.dat       # delta
{md5(catalog)[0:4]}/{encoded(catalog)}/{stopTsSeq}.snap # snap
```

For path safety the catalog is encoded: pure-lowercase `users` → `(users`,
pure-uppercase `USERS` → `)USERS`, mixed / non-ASCII → lowercased base32.
Catalog validation forbids `:` `|` `(` `)` so the forms never collide.

### Three-step direct upload

```
WriteBegin:  UUID v4 → object path → PresignPut(provider, bucket, path)  (NO Redis op)
(client uploads bytes directly to handle.UploadURL)
WriteNotify: Lua → INCR seqid → tsSeq; ZADD [mergeType, path, tsSeq, uri]  (NO storage op)
```

Because tsSeq is allocated only at notify (after the upload), a slow or aborted
upload never appears in the index — nothing to wait for, nothing to roll back.
An aborted write leaves at most one orphaned object (reaped by future sweep
tooling).

## 🏗️ Architecture

### Redis index

```
{prefix}:d:{catalog}    ZSet  # delta — per-catalog change log
  score  = timestamp + seqid/1e6        (e.g. 1700000000.000123)
  member = [mergeType, path, tsSeq, uri] (JSON array; written by the notify Lua via cjson)

{prefix}:s              Hash  # snap — deployment-wide, field = catalog
  value  = [tsSeq, uri]                 (JSON array; HSCAN drives IterateSnaps)

{prefix}:m:{indicator}  Hash  # sample (memo) — per-indicator, field = catalog
  value  = [score, updatedAt, data]     (score = data version, updatedAt = compute time)
```

### Read flow

```
List          ── 1× pipeline (snap HGet + delta ZRange)
   ├── load snapshot   (resolve(snap.URI).Get — cached if the backend is wrapped)
   ├── load deltas × N (resolve(delta.URI).Get, 10 workers)
   ↓
merge.Merge   (CPU-bound, in-process)
   ├── return merged document
   └── async (if WithSnapTarget): Put new snapshot to the snap target
```

## ⚙️ Configuration

Everything is explicit at `New`; there is no Redis-side `lake.setting` and no
global state. Lake core owns only the **index Redis** (durable, must persist);
read-path caching is opt-in and composed into your resolver with `storage/cached`,
backed by an ephemeral, LRU-evictable **cache Redis**:

```go
// `backends` is your provider→Storage resolver (the `resolve` from Quick Start).
// cached.Resolver wraps each backend with a cache chosen per (provider, bucket);
// a snapshot Put warms the cache (write-through), so the next read skips a cold
// object-store GET. Routing by bucket lets snaps and deltas use different tiers —
// which only differ when you give them different buckets.
cacheRDB := redis.NewClient(&redis.Options{Addr: "cache-redis:6379"}) // ephemeral, LRU
resolve := cached.Resolver(backends, func(provider, bucket string) cached.Cache {
    switch bucket {
    case "my-snaps": // the WithSnapTarget bucket: shared across processes, long TTL
        return cached.NewRedisCache(cacheRDB, 2*time.Hour)
    default: // delta buckets: process-local, short TTL (immutable, soon folded into a snap)
        return cached.NewMemoryCache(time.Minute)
    }
})

client := lake.New("my-lake",
    redis.NewClient(&redis.Options{Addr: "main-redis:6379"}), // index (durable)
    resolve,
    lake.WithSnapTarget("oss", "my-snaps"),
    lake.WithSampleCacheRedis(cacheRDB), // sample memo shares the same cache tier
)
```

Everything in the cache Redis — snap/delta bytes *and* the sample memo
(`WithSampleCacheRedis`) — is rebuildable, so `maxmemory-policy allkeys-lru` plus
per-key TTL evicts it safely; only the index Redis must persist. (Snap/delta
bytes are one TTL'd string per object, evicted per-key; the sample memo is one
Hash per indicator, so LRU drops a whole indicator at once — both just recompute
on the next read.)

| Property | Index Redis | Cache Redis |
|----------|-------------|-------------|
| Persistence | ✅ AOF + RDB | ❌ Disabled |
| Eviction | ❌ None | ✅ `allkeys-lru` |
| Max data loss | 1 second | All (OK — rebuildable) |

## 🔍 Event Middleware

```go
client.Use(func(catalog, event string, attrs map[string]any) {
    log.Printf("[lake] %s %s %v", catalog, event, attrs)
})
```

| Event | Attrs |
|-------|-------|
| `List` / `BatchList` | — |
| `WriteBegin` | `path`, `mergeType`, `provider`, `bucket` |
| `WriteNotify` | `path`, `uri` |
| `Sample` / `BatchSample` | `indicator` |
| `SampleCacheError` | `op`, `err` |

> For distributed tracing, instrument the Redis / storage clients in your
> resolver; Lake intentionally avoids dragging in OpenTelemetry.

## 🧪 Testing

```bash
go test ./...
go test -count=1 -race ./...
```

Integration tests need a reachable Redis at `127.0.0.1:6379`; they skip
gracefully when it is absent. The notify Lua's cjson-encoded member is only
exercised end-to-end with Redis present (`TestWriteReadRoundTrip_Redis`).

## 💡 Design Philosophy

### "Object storage is the source of truth, Redis is the hot index"

Every Redis key (with the partial exception of the `seqid` counter) is
conceptually rebuildable. Sample results — and any read-path caches you compose
via `storage/cached` — are all *caches*: failing to write them never fails a
user-visible operation.

### A patch body is the client's responsibility

Every committed delta is replayed by `merge` on **every read** (and on each
snapshot save). `WriteNotify` does not fetch or validate the uploaded body — so
a body that cannot be applied (invalid JSON, an RFC 7396 patch that doesn't
parse) fails merge, and because the same merge gates snapshotting the failure is
sticky: every read of that catalog errors until the bad delta is removed. There
is intentionally no read-time skip/quarantine. The merge error names the
offending delta (`path`, `tsSeq`, `uri`, `catalog`); recovery is manual (`ZREM`
the member from `{prefix}:d:{catalog}`). Keeping bodies valid before upload is
the contract.

### Snapshot save failure is not user-visible

A snapshot is an optimization. If the async save fails, the next read
regenerates it. Reads never wait for a snapshot to be persisted. With no
`WithSnapTarget`, snapshotting is simply off — reads replay all deltas.

### No background compaction (yet)

There is no `ClearHistory` and no reaper in v3-alpha: delta zsets and their
objects accumulate. Reads stay correct and fast (they start from the latest
snapshot and skip everything below it), but storage grows. Compaction will
return as explicit caller-side tooling.

## 🔄 Migrating from v2 to v3

v3 is **not** wire-compatible with v2. The headline changes:

- **Module path**: `…/lake/v2` → `…/lake/v3`.
- **Construction**: `NewLake(metaUrl, opts)` + Redis `lake.setting` → explicit
  `New(prefix, rdb, resolve, opts)`. `lake.setting`, `WithStorage`, and the
  `internal/config` layer are gone; storage is injected via a `Resolver`.
- **Storage is per-write + self-describing**: `WriteBeginRequest` gains
  `Provider` + `Bucket`; the delta records `provider://bucket/path`. The delta
  member is now a JSON array `[mergeType, path, tsSeq, uri]` and the snap value
  `[tsSeq, uri]` — old members/snaps don't decode; flush and repopulate.
- **RFC 6902 removed**: only `MergeTypeReplace` (1) and `MergeTypeRFC7396` (2)
  remain.
- **`ClearHistory` removed** (compaction deferred). **`AllSnaps` removed** — use
  `IterateSnaps`. **File API**, **`WriteRequest.Meta`**, and **`MotionSample`**
  removed (use `NewSampler[T]`).
- **Snapshots auto-generate** to `WithSnapTarget`; omit it to disable.

If you depend on a removed surface and cannot rewrite yet, the v2 line is still
available (`go get github.com/hkloudou/lake/v2@latest`) — but note that no
current version, v2 included, is recommended for production (see the status note
at the top).

## 🤝 Contributing

- All tests pass (`go test ./...`), code is `gofmt`-clean, `go vet ./...` is clean.

## 📄 License

MIT — see [LICENSE](LICENSE).

## 🔗 Links

- **GitHub**: https://github.com/hkloudou/lake
- **Issues**: https://github.com/hkloudou/lake/issues
- **v2 branch (maintenance)**: https://github.com/hkloudou/lake/tree/v2
