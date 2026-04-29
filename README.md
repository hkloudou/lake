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
- **🧮 Generic Sampling** — `Sample[T]` computes derived data on demand and
  caches it atomically; replaces v2's separate "meta" concept
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
    "context"
    "fmt"
    "log"

    "github.com/hkloudou/lake/v3"
)

func main() {
    client := lake.NewLake("redis://localhost:6379")

    client.Use(func(catalog, event string, attrs map[string]any) {
        log.Printf("[lake] %s %s %v", catalog, event, attrs)
    })

    ctx := context.Background()

    err := client.Write(ctx, lake.WriteRequest{
        Catalog:   "users",
        Path:      "/profile",
        Body:      []byte(`{"name":"Alice","age":30}`),
        MergeType: lake.MergeTypeReplace,
    })
    if err != nil {
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

### Write

| Function | File | Description |
|----------|------|-------------|
| `(*Client) Write(ctx, WriteRequest) error` | [write.go](write.go) | Write JSON delta with merge strategy |

```go
type WriteRequest struct {
    Catalog   string    // Document namespace
    Path      string    // JSON path; "/" means root document
    Body      []byte    // Raw JSON bytes
    MergeType MergeType // MergeTypeReplace, MergeTypeRFC7396, or MergeTypeRFC6902
}
```

**MergeType constants** ([export.go](export.go)):

```go
lake.MergeTypeReplace  // = 1: simple field replacement
lake.MergeTypeRFC7396  // = 2: RFC 7396 JSON Merge Patch (null removes)
lake.MergeTypeRFC6902  // = 3: RFC 6902 JSON Patch (operations array)
```

### Read

| Function | File | Description |
|----------|------|-------------|
| `(*Client) List(ctx, catalog, opts...) *ListResult` | [list.go](list.go) | Fetch snapshot info + delta index |
| `(*Client) BatchList(ctx, catalogs, opts...) map[string]*ListResult` | [list.go](list.go) | Batched list across N catalogs in 2 round-trips |
| `WithStrictPending() ListOption` | [list.go](list.go) | Treat any pending member as `HasPending` |
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

`Sample[T]` is the canonical way to derive secondary state from a catalog.
The first call invokes `loader`, and subsequent calls reuse the cached value
unless the catalog has changed (compared by `LastUpdated()`).

| Function | File | Description |
|----------|------|-------------|
| `Sample[T](ctx, *ListResult, indicator, loader) (T, error)` | [sample.go](sample.go) | Generic cached sampling |
| `ErrPendingWrites` | [sample.go](sample.go) | Returned when the list reports pending writes |

```go
list := client.List(ctx, "users")
report, err := lake.Sample[Report](ctx, list, "daily",
    func(l *lake.ListResult) (Report, error) {
        data, err := lake.ReadMap(ctx, l)
        if err != nil {
            return Report{}, err
        }
        return buildReport(data), nil
    })
// data unchanged → 1× HGET, returns cached Report
// data changed   → loader runs, result is HSET'd atomically with the score
```

**Storage layout**: Sample results live in
`{prefix}:samples:{indicator}` Redis Hashes, with each catalog as a field
holding a `[score, data]` JSON array. All catalogs sharing one indicator are
colocated, so indicator-wide enumeration / clearing is single-key.

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

```go
// RFC 7396 — partial update; null deletes a field
client.Write(ctx, lake.WriteRequest{
    Catalog:   "users",
    Path:      "/profile",
    Body:      []byte(`{"age":31,"city":"NYC","oldField":null}`),
    MergeType: lake.MergeTypeRFC7396,
})

// RFC 6902 — explicit operations
client.Write(ctx, lake.WriteRequest{
    Catalog:   "users",
    Path:      "/",
    Body:      []byte(`[{"op":"add","path":"/tags","value":["vip"]}]`),
    MergeType: lake.MergeTypeRFC6902,
})

// Strict consistency — any pending write blocks the read
list := client.List(ctx, "users", lake.WithStrictPending())
```

### File Structure

```
lake/
├── lake.go              # Client, options, ensureInitialized
├── middleware.go        # Use(), EventHandler
├── write.go             # Write, WriteRequest
├── read.go              # Internal readData (parallel snap + deltas + merge)
├── list.go              # List, BatchList, ListResult, WithStrictPending
├── helpers.go           # ReadBytes, ReadString, ReadMap, Read[T]
├── clear.go             # ClearHistory entry points
├── clear_optimized.go   # Concurrent storage delete + batch ZREM
├── sample.go            # Sample[T], ErrPendingWrites
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
| `Write` | `path`, `mergeType` | After validation, before any I/O |
| `Sample` | `indicator` | Once per Sample call (cache hit or miss) |
| `ClearHistory` | `keepSnaps` | Once per catalog clear |

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

### Two-phase commit (atomic write)

```
Write:
  1. Lua: GetTimeSeqID + ZADD pending|...      (atomic, in Redis)
  2. Storage.Put(deltaKey, body)                (slow path)
  3. Lua: ZREM pending + ZADD delta|...         (atomic, in Redis)
```

If step 2 fails → step 1 is rolled back via `ZREM pending`. If step 3 fails →
the pending member auto-expires after 120 s and is reaped by `ClearHistory`.

### Pending detection

Read paths consult Redis time (synced every 5 s) to age pending members:

- `age > 120 s` → ignored (write was abandoned)
- `age ≤ 120 s` → `HasPending = true` *only if* a committed delta follows the
  pending member (tail pending is harmless to in-flight reads)
- With `WithStrictPending()` → any in-window pending sets `HasPending`

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
  member ∈ {
    delta|{mergeType}|{path}|{ts}_{seqid}            # committed
    pending|delta|{mergeType}|{path}|{ts}_{seqid}    # in-flight (≤120s)
  }

{prefix}:snaps             Hash    field=catalog
  value = "{startTsSeq}|{stopTsSeq}"           # one entry per catalog,
                                                # overwritten on each save;
                                                # HGETALL drives backup tooling

{prefix}:samples:{indicator}   Hash
  field = catalog
  value = [score, data]                         # JSON array, atomic per HSET
```

### Object storage

```
{md5(catalog)[0:4]}/{encoded(catalog)}/{ts}_{seqid}_{mergeType}.dat   # delta
{md5(catalog)[0:4]}/{encoded(catalog)}/{startTsSeq}~{stopTsSeq}.snap  # snap
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
  state now goes through `Sample[T]` instead. Callers that stored meta-as-JSON
  alongside writes should compute it lazily inside a `Sample[T]` loader.
- **File API removed.** `WriteFile`, `FileExists`, `FilesAndMeta`,
  `WriteFileRequest`, and `Storage.MakeFileKey` are gone. Lake v3 handles
  JSON documents only.
- **`MotionSample` (v1 sample) removed.** Use generic `Sample[T]` instead.
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
- **Named error**: pending writes now surface as `lake.ErrPendingWrites`,
  compatible with `errors.Is`.

If you depend on any of the removed surfaces and cannot rewrite, **stay on
the v2 line**: it remains maintained on the [`v2` branch](https://github.com/hkloudou/lake/tree/v2)
and still resolves at `go get github.com/hkloudou/lake/v2@latest`.

## 📚 Examples

- [example_test.go](./example_test.go) — Write, Read, RFC patches
- [cache_example_test.go](./cache_example_test.go) — Two-Redis cache setup
- [trace_example_test.go](./trace_example_test.go) — Event handler usage

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
