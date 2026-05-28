# Lake V3

[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Release](https://img.shields.io/github/v/release/hkloudou/lake)](https://github.com/hkloudou/lake/releases)

> Distributed JSON document store with atomic writes, RFC-standard merging,
> snapshot acceleration, and computed sampling.

> **⚠️ v3 status: alpha — public API may still change before `v3.0.0` stable.**
> Production users should pin v2 (`go get github.com/hkloudou/lake/v2@latest`)
> until v3 stabilises. See [Migrating from v2 to v3](#migrating-from-v2-to-v3).

## ✨ Key Features

- **🔒 Atomic Writes** — Two-phase commit with pending state, no data loss under
  concurrent writers and slow object storage
- **📜 RFC Standards** — Full RFC 7396 (Merge Patch) and RFC 6902 (JSON Patch)
- **⚡ High Throughput** — Up to 999,999 writes/sec per catalog (Lua-bound seqid)
- **💾 Smart Caching** — Redis snapshot cache + in-memory delta cache
- **🎯 Snapshot Acceleration** — Time-range packed snapshots, async generation
- **🧮 Generic Sampling** — `NewSampler[T]` computes derived data on demand
  with a layered staleness policy; replaces v2's separate "meta" concept
- **🔍 Event Middleware** — `client.Use(handler)` for logging / monitoring
- **🔐 Optional AES-GCM Encryption** — minimal overhead, configurable via
  `lake.setting`

## 🚀 Quick Start

### Installation

```bash
go get github.com/hkloudou/lake/v3@v3.0.0-alpha.1
```

### Basic Usage

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "log"
    "net/http"

    "github.com/hkloudou/lake/v3"
)

func main() {
    client := lake.NewLake("redis://localhost:6379")

    client.Use(func(catalog, event string, attrs map[string]any) {
        log.Printf("[lake] %s %s %v", catalog, event, attrs)
    })

    ctx := context.Background()
    body := []byte(`{"name":"Alice","age":30}`)

    // 1. Reserve a UUID and signed OSS PUT URL — no Redis op here.
    h, err := client.WriteBegin(ctx, lake.WriteBeginRequest{
        Catalog:   "users",
        Path:      "/profile",
        MergeType: lake.MergeTypeReplace,
    })
    if err != nil {
        log.Fatal(err)
    }

    // 2. Upload the body directly to OSS. Bytes do NOT pass through Lake.
    req, _ := http.NewRequestWithContext(ctx, h.UploadMethod, h.UploadURL, bytes.NewReader(body))
    for k, v := range h.UploadHeaders {
        req.Header.Set(k, v)
    }
    if _, err := http.DefaultClient.Do(req); err != nil {
        log.Fatal(err)
    }

    // 3. Notify Lake to commit the index entry referencing the upload.
    if err := client.WriteNotify(ctx, h); err != nil {
        log.Fatal(err)
    }

    list := client.List(ctx, "users")
    jsonStr, _ := lake.ReadString(ctx, list)
    fmt.Printf("Data: %s\n", jsonStr)
}
```

## 📚 API Reference

### Package Import

```go
import "github.com/hkloudou/lake/v3"
```

### Client Creation

| Function | File | Description |
|----------|------|-------------|
| `NewLake(metaUrl, opts...) *Client` | [lake.go](lake.go) | Create client with Redis URL |
| `WithStorage(storage.Storage) func(*option)` | [lake.go](lake.go) | Inject custom storage (memory / OSS / file) |
| `WithSnapCacheMetaURL(url, ttl) func(*option)` | [lake.go](lake.go) | Use a separate Redis for snapshot cache |
| `WithDeltaCacheMetaURL(url, ttl) func(*option)` | [lake.go](lake.go) | Use a separate Redis for delta cache |
| `WithSnapCache(cache.Cache) func(*option)` | [lake.go](lake.go) | Provide a `cache.Cache` directly |
| `WithDeltaCache(cache.Cache) func(*option)` | [lake.go](lake.go) | Same, for delta cache |
| `(*Client) Use(handler EventHandler)` | [middleware.go](middleware.go) | Register an event handler |

### Write — three-step direct upload

V3 splits Write into three steps so client bytes never traverse the Lake
process:

| Function | File | Description |
|----------|------|-------------|
| `(*Client) WriteBegin(ctx, WriteBeginRequest, opts...) (*WriteHandle, error)` | [write.go](write.go) | Reserve a UUID and signed OSS PUT URL. **No Redis op** — pure function of (catalog, path, mergeType, OSS credentials). |
| (HTTP PUT to `handle.UploadURL`) | — | The client uploads bytes directly to OSS using the signed URL and the headers in `handle.UploadHeaders`. |
| `(*Client) WriteNotify(ctx, *WriteHandle) error` | [write.go](write.go) | Allocate the tsSeq and atomically register the delta in Redis. |

**WriteBeginRequest**:

```go
type WriteBeginRequest struct {
    Catalog   string    `json:"catalog"`
    Path      string    `json:"path"`      // "/" means root
    MergeType MergeType `json:"mergeType"` // 1=Replace, 2=RFC7396, 3=RFC6902
}
```

**WriteHandle** (JSON-serialisable for non-Go SDKs):

```go
type WriteHandle struct {
    Catalog       string            `json:"catalog"`
    Path          string            `json:"path"`
    MergeType     MergeType         `json:"mergeType"`
    UUID          string            `json:"uuid"`
    StorageKey    string            `json:"storageKey"`
    UploadURL     string            `json:"uploadURL"`
    UploadMethod  string            `json:"uploadMethod"`
    UploadHeaders map[string]string `json:"uploadHeaders"`
    ExpiresAt     time.Time         `json:"expiresAt"`
}
```

**Begin options**:

```go
lake.WithUploadTTL(15 * time.Minute)        // signed URL validity
lake.WithMaxBodyBytes(100 * 1024 * 1024)    // hard cap baked into signature
lake.WithUploadContentType("application/json")
```

**Why three steps**

- **Bandwidth** — body bytes go through the OSS provider's free inbound
  pipe, never through your Lake servers.
- **FaaS-friendly** — Begin needs only OSS credentials; Notify needs only
  Redis. Each can be a Lambda / Workers function near its dependency.
- **Multi-language** — Begin returns JSON. Any client (browser / Python /
  Rust / curl) can talk to Lake's HTTP endpoints; the Go SDK is a
  convenience layer, not a requirement.
- **Self-describing OSS objects** — the signed URL forces the client to
  attach `x-oss-meta-catalog`, `x-oss-meta-path`, `x-oss-meta-merge-type`
  headers. An LIST + GetObjectMeta on the bucket is enough to rebuild
  Lake's Redis index from scratch.

**MergeType constants** ([export.go](export.go)):

```go
lake.MergeTypeReplace  // = 1: simple field replacement
lake.MergeTypeRFC7396  // = 2: RFC 7396 JSON Merge Patch (null removes)
lake.MergeTypeRFC6902  // = 3: RFC 6902 JSON Patch (operations array)
```

> **Storage support**: WriteBegin requires a `Presigner`-capable storage
> backend. OSS supports it; File / Memory return `lake.ErrPresignNotSupported`
> (file/memory backends are now read-only at the V3 boundary).
>
> **Bodies are stored RAW**: V3 storage no longer gzips/encrypts. For
> at-rest encryption use OSS SSE; for compression encode client-side.

### Read

| Function | File | Description |
|----------|------|-------------|
| `(*Client) List(ctx, catalog) *ListResult` | [list.go](list.go) | Fetch snapshot info + delta index (1 HGet + 1 ZRange) |
| `(*Client) BatchList(ctx, catalogs) map[string]*ListResult` | [list.go](list.go) | Batched list across N catalogs in 2 round-trips total |
| `ReadBytes(ctx, *ListResult) ([]byte, error)` | [helpers.go](helpers.go) | Merged document as raw bytes |
| `ReadString(ctx, *ListResult) (string, error)` | [helpers.go](helpers.go) | Merged document as JSON string |
| `ReadMap(ctx, *ListResult) (map[string]any, error)` | [helpers.go](helpers.go) | Merged document as map |
| `Read[T any](ctx, *ListResult) (*T, error)` | [helpers.go](helpers.go) | Generic typed read |

```go
type ListResult struct {
    Err        error // non-nil on Redis / decode failures
    HasPending bool  // a pending write (< 120s old) overlaps the result
    // ...other fields are read-only details
}

func (m ListResult) Exist() bool         // catalog has snapshot or deltas
func (m ListResult) LastUpdated() float64 // score of the most recent change
```

**Common pattern**:

```go
list := client.List(ctx, "users")
if list.Err != nil {
    return list.Err
}
if list.HasPending {
    return errors.New("write in progress, retry")
}

// Pick the read shape you want:
jsonStr, err := lake.ReadString(ctx, list)
data, err    := lake.ReadMap(ctx, list)
profile, err := lake.Read[UserProfile](ctx, list)
```

### Sample (computed, cached)

`NewSampler[T]` is the single entry point for deriving secondary state from a
catalog. Construct one `Sampler` per `(indicator, T, loader)` and reuse it: it
memoises each catalog's computed value in Redis and recomputes only when the
value is stale.

| API | File | Description |
|-----|------|-------------|
| `NewSampler[T](indicator, loader, …opts) *Sampler[T]` | [sample.go](sample.go) | Build a reusable sampler |
| `(*Sampler[T]) Sample(ctx, *ListResult) (T, error)` | [sample.go](sample.go) | One catalog: hit → 1 HGET, miss → loader + HSET |
| `(*Sampler[T]) Batch(ctx, map[string]*ListResult) map[string]*SampleResult[T]` | [sample.go](sample.go) | Many catalogs: 1 HMGET + concurrent loaders for misses |

```go
sampler := lake.NewSampler[Report]("daily",
    func(l *lake.ListResult) (Report, error) {
        data, err := lake.ReadMap(ctx, l)
        if err != nil {
            return Report{}, err
        }
        return buildReport(data), nil
    },
    lake.WithMaxAge(time.Hour),                              // recompute hourly even if data is unchanged
    lake.WithLoaderErrorDefault(Report{Status: "degraded"}), // serve a default if the loader fails (never cached)
)

list := client.List(ctx, "users")
report, err := sampler.Sample(ctx, list)
```

**Staleness is layered.** A cached sample is reused only while fresh:

1. **Data-version floor (always on)** — if the catalog advanced past the
   version the sample was computed at (`ListResult.LastUpdated()`), it is
   recomputed. But the data version alone is not the whole story: the *same*
   version can still need a refresh, because a derived value can depend on
   more than the catalog's own bytes.
2. **`WithMaxAge(d)`** — recompute when the entry is older than `d` by the
   Redis server clock. For time-sensitive derivations (a "today" report is
   correct only for today, not forever).
3. **`WithShouldRefresh(fn)`** — a custom predicate, the analog of React's
   `shouldComponentUpdate`: return `true` to force a recompute even when the
   data version is unchanged (cross-catalog dependencies, external inputs).
   It runs on every cache hit, so it must be pure and cheap — no I/O; compare
   versions you already hold (e.g. from the same `BatchList`).

These triggers can only *add* recomputes; none serves a value older than the
data-version floor allows.

**Errors never poison the cache.** A loader error — or its
`WithLoaderErrorDefault` / `WithLoaderErrorFallback` substitute — is a
per-call response and is never written back, so a transient blip (a database
hiccup) cannot freeze a degraded value into the cache until the next write.
Cache-tier failures degrade gracefully too: a failed read recomputes, a
failed write still returns the freshly computed value.

**Storage layout**: samples live in `{prefix}:samples:{indicator}` Redis
Hashes, each catalog a field holding a `[score, updatedAt, data]` JSON array
(`score` = data version, `updatedAt` = compute time). All catalogs sharing one
indicator are colocated, so indicator-wide enumeration / clearing is single-key.

### Cleanup

| Function | File | Description |
|----------|------|-------------|
| `(*Client) ClearHistory(ctx, catalog) error` | [clear.go](clear.go) | Drop all delta entries at or before the catalog's latest snap |

V3 keeps only one snap per catalog (snaps are idempotent and self-correcting — an
"out of date" snap is replaced on the next read). There is no historical-snap
retention concept; `ClearHistoryWithRetention` from earlier drafts is removed.

### Backup

| Function | File | Description |
|----------|------|-------------|
| `(*Client) AllSnaps(ctx) (map[string]SnapInfo, error)` | [snapshot.go](snapshot.go) | Single HGETALL on `<prefix>:snaps` returns every catalog's snap metadata |

`AllSnaps` is intended for backup tooling: feed each `(catalog, StartTsSeq, StopTsSeq)`
triple into `Storage.MakeSnapKey` to obtain the OSS object key, then copy that
object to your archive bucket. This avoids a full OSS `LIST`, which is slow and
paginated, and gives you a consistent snapshot of the deployment in one Redis
round-trip.

### Examples

The merge semantics are selected by `MergeType` on `WriteBegin`; the body you
PUT to the signed URL is the patch document (see the upload + `WriteNotify`
steps in Quick Start above).

```go
// RFC 7396 — partial merge patch; null deletes a field.
// Upload body: {"age":31,"city":"NYC","oldField":null}
h, _ := client.WriteBegin(ctx, lake.WriteBeginRequest{
    Catalog:   "users",
    Path:      "/profile",
    MergeType: lake.MergeTypeRFC7396,
})

// RFC 6902 — explicit JSON Patch operations.
// Upload body: [{"op":"add","path":"/tags","value":["vip"]}]
h, _ = client.WriteBegin(ctx, lake.WriteBeginRequest{
    Catalog:   "users",
    Path:      "/",
    MergeType: lake.MergeTypeRFC6902,
})
```

### File Structure

```
lake/
├── lake.go              # Client, options, ensureInitialized
├── middleware.go        # Use(), EventHandler
├── write.go             # WriteBegin, WriteNotify, WriteHandle
├── read.go              # Internal readData (parallel snap + deltas + merge)
├── list.go              # List, BatchList, ListResult
├── helpers.go           # ReadBytes, ReadString, ReadMap, Read[T]
├── clear.go             # ClearHistory entry points
├── clear_optimized.go   # Concurrent storage delete + batch ZREM
├── sample.go            # Sampler[T] (NewSampler): cached derived sampling
├── snapshot.go          # Async snapshot save under SingleFlight
├── export.go            # MergeType re-exports
└── internal/
    ├── index/           # Redis ZSet operations, TimeSeqID, Lua scripts
    ├── storage/         # Memory / File / OSS backends
    ├── merge/           # Replace / RFC7396 / RFC6902 mergers
    ├── cache/           # Redis + Memory caches with SingleFlight
    ├── config/          # lake.setting loader
    ├── encode/          # Catalog name encoding chokepoint
    ├── encrypt/         # AES-GCM + gzip
    ├── utils/           # Path validation
    └── xsync/           # SingleFlight primitive
```

## ⚙️ Configuration

### `lake.setting` (Redis-backed)

Lake loads its bucket / storage choice from the Redis key `lake.setting`:

```json
{
  "Name": "my-lake",
  "Storage": "oss",
  "Bucket": "my-bucket",
  "Endpoint": "oss-cn-hangzhou",
  "AccessKey": "your-access-key",
  "SecretKey": "your-secret-key",
  "AESPwd": "optional-encryption-key",
  "BasePath": ""
}
```

Supported `Storage` values: `oss`, `file`, `memory` (or empty → memory).

### Two-Redis layout (recommended)

Lake makes a clean separation between **index Redis** (durable, must persist)
and **cache Redis** (ephemeral, LRU-evicted).

```go
client := lake.NewLake(
    "redis://main-redis:6379",
    lake.WithSnapCacheMetaURL("redis://cache-redis:6379", 2*time.Hour),
)
```

| Property | Cache Redis | Index Redis |
|----------|-------------|-------------|
| Persistence | ❌ Disabled | ✅ AOF + RDB |
| Eviction | ✅ LRU | ❌ None |
| Importance | Low (rebuildable) | Critical |
| Max Data Loss | All (OK) | 1 second |

**Index Redis (recommended config):**

```text
appendonly yes
appendfsync everysec
save 900 1
save 300 10
save 60 100
rename-command FLUSHDB ""
rename-command FLUSHALL ""
```

**Cache Redis (recommended config):**

```text
appendonly no
save ""
maxmemory 4096mb
maxmemory-policy allkeys-lru
```

## 🔍 Event Middleware

```go
type EventHandler func(catalog string, event string, attrs map[string]any)

client.Use(func(catalog, event string, attrs map[string]any) {
    log.Printf("[lake] %s %s %v", catalog, event, attrs)
})
```

| Event | Attrs | Notes |
|-------|-------|-------|
| `List` | — | Single-catalog list |
| `BatchList` | — | One event per catalog inside the batch |
| `WriteBegin` | `path`, `mergeType` | Emitted before path validation |
| `WriteNotify` | `path`, `uuid` | Emitted on commit to the delta index |
| `Sample` | `indicator` | Once per Sample call (cache hit or miss) |
| `BatchSample` | `indicator` | One event per catalog inside the batch |
| `SampleCacheError` | `op`, `err` | Cache read/write failed; degraded to recompute / best-effort write |
| `ClearHistory` | — | Once per catalog clear |

> For distributed tracing, instrument the underlying Redis / OSS clients
> (e.g. `redisotel.InstrumentTracing`); Lake intentionally avoids dragging in
> OpenTelemetry as a dependency.

## 📖 Core Concepts

### Path Format

- Must start with `/`
- Must not end with `/`
- Each segment must start with a letter / `_` / `$` (no leading digit)
- `/` alone means the whole document
- `|` is forbidden — it is the delimiter inside delta member encoding

| Valid | Invalid |
|-------|---------|
| `/` | `user` |
| `/user` | `/user/` |
| `/user/profile` | `/123` |
| `/$config` | `/user-name` |

### Three-step direct upload

```
WriteBegin:
  1. Generate UUID v4
  2. PresignPut("{md5}/{encoded}/{uuid}.dat") with required user metadata
     → return WriteHandle (NO Redis op)

(client uploads bytes directly to OSS via handle.UploadURL)

WriteNotify:
  3. Lua: INCR seqid → tsSeq; ZADD delta|{type}|{path}|{tsSeq}|{uuid}
     (single atomic op)
```

The protocol has NO pending phase. Because tsSeq is allocated only at
step 3 (after the OSS upload has succeeded), a slow or aborted upload
never appears in the index — there is nothing to wait for, nothing to
roll back. Aborted writes leave at most one orphaned OSS object,
reaped by future sweep tooling.

### Catalog encoding

For OSS / file paths Lake encodes catalog names for path safety:

- pure lowercase (`users`) → `(users`
- pure uppercase (`USERS`) → `)USERS`
- mixed / non-ASCII → lowercased base32

For Redis keys, v3 stores catalog names **verbatim** —
[`encode.EncodeRedisCatalogName`](internal/encode/catalog.go) is currently the
identity function and acts as the single chokepoint should encoding be added
back later. Callers must therefore avoid `:` and `|` in catalog names today.

## 🏗️ Architecture

### Redis index

```
{prefix}:{catalog}:delta   ZSet
  score  = timestamp + seqid / 1e6   (e.g. 1700000000.000123)
  member = "delta|{mergeType}|{path}|{ts}_{seqid}|{uuid}"

{prefix}:snaps             Hash    field=catalog
  value = "{stopTsSeq}"                         # one entry per catalog,
                                                # overwritten on each save;
                                                # HGETALL drives backup tooling

{prefix}:samples:{indicator}   Hash
  field = catalog
  value = [score, updatedAt, data]              # JSON array, atomic per HSET
                                                # score=data version, updatedAt=compute time
```

### Object storage

```
{md5(catalog)[0:4]}/{encoded(catalog)}/{uuid}.dat       # delta (uuid allocated by WriteBegin)
{md5(catalog)[0:4]}/{encoded(catalog)}/{stopTsSeq}.snap # snap (server-generated)
```

The local-file backend uses a deeper layout (`md5[0:2]/encoded/h1/h2/h3/...`)
to keep per-directory file counts under filesystem-friendly bounds.

### Read flow

```
List          ── 1× pipeline (snap ZRangeRev + delta ZRange)
   │
   ├── load snapshot data        (snap cache → object storage)
   ├── load delta bodies × N     (delta cache → object storage, 10 workers)
   ↓
merge.Merge   (CPU-bound, in-process)
   │
   ├── return merged document to caller
   └── async: Storage.Put new snapshot under SingleFlight
```

## 📊 Performance

Indicative numbers from typical OSS-backed workloads:

| Metric | Value |
|--------|-------|
| Write throughput | up to 999,999 / sec per catalog (seqid-bound) |
| Atomic overhead | < 2 % of total write latency |
| Cache hit ratio | ~ 90 % |
| Delta load fan-out | 10 workers in parallel |
| Snapshot save | async, off the read critical path |

Read-path timing on warm caches is dominated by `merge.Merge`; cold reads are
dominated by object-storage `Get`.

## 🧪 Testing

```bash
go test ./...
go test -v ./internal/merge
go test -count=1 -race ./internal/index
```

Some example tests rely on a reachable Redis at `127.0.0.1:6379` and a
configured OSS bucket. They will fail in environments without those.

## 💡 Design Philosophy

### "OSS is the source of truth, Redis is the hot index"

v3 leans into this contract: every Redis key, with the partial exception of
the `lake:seqid` counter, is conceptually rebuildable from object storage.
Sample results, snapshot caches, and delta caches are all explicitly
described as *caches* — failure to write them never fails a user-visible
operation.

(Today the contract is not yet airtight — delta filenames don't carry the
JSON path, so a full rebuild from OSS alone still loses some metadata. Closing
that gap is a tracked v3 work item.)

### Fail-fast on programmer errors

A few code paths panic rather than return errors:

- `WithSnapCacheMetaURL` / `WithDeltaCacheMetaURL` panic on an invalid URL
- `indexIO.Make*Key` panics if `SetPrefix` was never called

These represent invariant violations from the embedding program, not runtime
errors from the data path.

### Snapshot save failure is not user-visible

A snapshot is an optimization. If the async save fails, the next read simply
regenerates it. Reads never wait for a snapshot to be persisted.

### Orphan cleanup is via `ClearHistory`

Two-phase commit can leave behind a pending member + storage object if the
process dies between step 2 and step 3. There is no background reaper —
instead, `ClearHistory` is the single, explicit cleanup entry point. Pending
members carry enough information (mergeType + path + tsSeq) to reconstruct
the storage key and delete it.

## 🔄 Migrating from v2 to v3

v3 is **not** wire-compatible with v2 callers. Concretely:

- **Module path**: `github.com/hkloudou/lake/v2` → `github.com/hkloudou/lake/v3`
- **`WriteRequest.Meta` removed.** v2's catalog-level meta is gone — derived
  state now goes through `NewSampler[T]` instead. Callers that stored
  meta-as-JSON alongside writes should compute it lazily inside a sampler's loader.
- **File API removed.** `WriteFile`, `FileExists`, `FilesAndMeta`,
  `WriteFileRequest`, and `Storage.MakeFileKey` are gone. Lake v3 handles
  JSON documents only.
- **`MotionSample` (v1 sample) removed.** Use `NewSampler[T]` instead — its
  `WithShouldRefresh` predicate and `Batch` over many catalogs subsume v1's
  `shouldUpdated` callback and `motionCatalogs` cross-catalog sampling.
- **Sample storage layout inverted.** v2 used
  `{prefix}:{catalog}:sample` Hashes with the indicator as the field. v3 uses
  `{prefix}:samples:{indicator}` Hashes with the catalog as the field. v2
  cache entries cannot be read by v3 and will be silently recomputed.
- **Snap storage replaced.** v2 stored snap metadata in per-catalog ZSets
  (`{prefix}:{catalog}:snap`) and tracked historical snapshots via a retention
  parameter. v3 stores a single latest snap per catalog as a field of one
  global Hash (`{prefix}:snaps`), enabling whole-deployment backup via one
  HGETALL. The previous OSS snap object on each save becomes orphan storage
  (acceptable trade-off; reaped by future SweepOrphans tooling).
  `ClearHistoryWithRetention(ctx, catalog, keepSnaps)` is removed —
  use `ClearHistory(ctx, catalog)` instead.
- **`Client.AllSnaps(ctx)`** is new: returns every catalog's snap metadata in
  one HGETALL, intended for backup workflows.
- **`merge.Merge` signature** changed: dropped the unused `catalog` parameter
  and the always-empty second return value. (Internal API.)
- **`Writer.Commit` signature** dropped the meta argument. (Internal API.)

If you depend on any of the removed surfaces and cannot rewrite, **stay on
the v2 line**: it remains maintained on the [`v2` branch](https://github.com/hkloudou/lake/tree/v2)
and still resolves at `go get github.com/hkloudou/lake/v2@latest`.

## 📚 Examples

- **Quick Start** above — `WriteBegin` → upload → `WriteNotify` → `List` → `Read`
- [sample_test.go](./sample_test.go), [sample_batch_test.go](./sample_batch_test.go) — `Sampler[T]` staleness policy and batch behavior
- [middleware_event_test.go](./middleware_event_test.go) — event handler / middleware usage

## 🤝 Contributing

- All tests pass (`go test ./...`)
- Code is formatted (`go fmt ./...`)
- `go vet ./...` is clean
- Commits are descriptive

## 📄 License

MIT — see [LICENSE](LICENSE).

## 🔗 Links

- **GitHub**: https://github.com/hkloudou/lake
- **Issues**: https://github.com/hkloudou/lake/issues
- **Releases**: https://github.com/hkloudou/lake/releases
- **v2 branch (maintenance)**: https://github.com/hkloudou/lake/tree/v2
