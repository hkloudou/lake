# Lake V2

[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue)](https://golang.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Release](https://img.shields.io/github/v/release/hkloudou/lake)](https://github.com/hkloudou/lake/releases)

> High-performance distributed JSON document system with atomic writes, RFC-standard merging, and intelligent caching.

## ✨ Key Features

- **🔒 Atomic Writes** - Two-phase commit with pending state, prevents data loss in concurrent scenarios
- **📜 RFC Standards** - Full RFC 7396 (Merge Patch) and RFC 6902 (JSON Patch) support
- **⚡ High Performance** - 999,999 writes/sec per catalog, parallel I/O, worker pool optimization
- **💾 Smart Caching** - Redis-based cache with namespace isolation (~90% hit ratio)
- **🎯 Intelligent Encoding** - MD5-based sharding, case-insensitive safe paths
- **🔍 Built-in Tracing** - Context-based performance monitoring (zero overhead when disabled)
- **🔐 AES Encryption** - Optional AES-GCM encryption with minimal overhead (<0.05ms)
- **📊 Snapshot System** - Time-range based snapshots for efficient incremental reads

## 🚀 Quick Start

### Installation

```bash
go get github.com/hkloudou/lake/v2@latest
```

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    
    "github.com/hkloudou/lake/v2"
)

func main() {
    // Create client (config loaded lazily)
    client := lake.NewLake("redis://localhost:6379")
    ctx := context.Background()
    
    // Write data
    err := client.Write(ctx, lake.WriteRequest{
        Catalog:   "users",
        Path:      "/profile",  // Path format: starts with /
        Body:      []byte(`{"name":"Alice","age":30}`),
        MergeType: lake.MergeTypeReplace,
    })
    
    // List catalog entries
    list := client.List(ctx, "users")
    
    // Read merged data
    data, _ := lake.ReadMap(ctx, list)
    fmt.Printf("Data: %+v\n", data)
}
```

### With Caching

```go
import "time"

client := lake.NewLake(
    "redis://localhost:6379",
    lake.WithSnapCacheMetaURL("redis://localhost:6379", 5*time.Minute),
)
```

### Why Separate SnapCache?

**Recommended**: Use a dedicated Redis instance for snapshot caching

**Reasons:**
1. **Data Isolation** - Snapshots are pure cache (can be rebuilt), separate from critical index data
2. **Memory Management** - Enable LRU eviction on cache Redis without affecting index data
3. **Performance** - No persistence overhead (AOF/RDB disabled) for faster access
4. **Cost Optimization** - Use cheaper ephemeral storage for cache Redis
5. **Independent Scaling** - Scale cache and index Redis independently based on workload

**Recommended Setup: OCI Bitnami Redis**

```bash
# Install using Helm
helm install lake-cache oci://registry-1.docker.io/bitnamicharts/redis -f cache-redis-values.yaml
```

**cache-redis-values.yaml:**
```yaml
architecture: standalone  # Standalone mode (single node)

replica:
  replicaCount: 1  # Single node, no replicas needed

master:
  kind: Deployment
  persistence:
    enabled: false  # Disable persistence (ephemeral storage)
  
  resources:
    limits:
      memory: "4096Mi"    # Maximum memory: 4GB
    requests:
      memory: "256Mi"     # Initial memory request: 256MB
  
  configuration: |
    # Disable AOF persistence
    appendonly no
    
    # Disable RDB auto-save
    save ""
    
    # Enable LRU cache with 4GB memory limit
    maxmemory 4096mb
    
    # Set eviction policy to allkeys-lru (evict least recently used keys)
    maxmemory-policy allkeys-lru

auth:
  enabled: false  # Disable authentication (internal use)

usePassword: false

cluster:
  enabled: false  # Disable cluster mode

persistence: 
  existingClaim: false  # Don't use existing PVC
  enabled: false  # Disable persistence

sysctlImage:
  enabled: true
  repository: busybox
  tag: v1.35.0
  command:
    - /bin/sh
    - '-c'
    - |
      mount -o remount rw /proc/sys
      sysctl -w net.core.somaxconn=65535
      sysctl -w net.ipv4.ip_local_port_range="1024 65535"
```

**Why This Configuration?**
- ✅ **No Persistence** - Cache Redis stores snapshot data content (can be rebuilt from OSS), no need to persist
- ✅ **LRU Eviction** - Automatically evicts cached snapshot data when memory is full (only affects cache, not index data)
- ✅ **High Performance** - No disk I/O overhead from AOF/RDB
- ✅ **Memory Efficient** - Uses 4GB max, starts with 256MB
- ✅ **Optimized Networking** - Increased connection limits for high throughput

**Important**: 
- **Cache Redis** (this section): Only caches snapshot data content, can use LRU eviction
- **Main Redis** (below): Stores index data (snap/delta/pending members), **permanently saved** unless manually deleted, **MUST have persistence enabled**

### Main Redis Configuration (Index Storage)

**Critical**: Main Redis stores index data (delta/pending/snapshot members) and MUST have persistence enabled.

**Recommended Configuration for Minimum Data Loss:**

```yaml
master:
  persistence:
    enabled: true
    path: /data
  
  configuration: |
    dir /data
    
    # Enable AOF persistence (Append-Only File)
    appendonly yes
    
    # Configure RDB auto-save policies (multiple time points for redundancy)
    save 900 1     # Save after 900 seconds (15 min) if at least 1 modification
    save 300 10    # Save after 300 seconds (5 min) if at least 10 modifications
    save 60 100    # Save after 60 seconds (1 min) if at least 100 modifications
    
    # AOF sync policy (sync every second - balance between performance and durability)
    appendfsync everysec
    
    # Disable dangerous commands (prevent accidental data loss)
    rename-command FLUSHDB ""
    rename-command FLUSHALL ""
```

**Why This is the Lowest Data Loss Configuration:**

1. **Dual Persistence (AOF + RDB)**
   - AOF: Logs every write operation, can recover to the last second
   - RDB: Creates point-in-time snapshots at multiple intervals
   - If AOF corrupts, RDB provides backup recovery

2. **Multi-Level RDB Snapshots**
   - High-frequency writes: RDB every 1 minute (60s/100 changes)
   - Medium-frequency: RDB every 5 minutes (300s/10 changes)
   - Low-frequency: RDB every 15 minutes (900s/1 change)
   - Ensures data is saved regardless of write pattern

3. **AOF everysec (Best Balance)**
   - Max 1 second of data loss in worst case
   - Better performance than `appendfsync always`
   - More durable than `appendfsync no`

4. **Command Protection**
   - FLUSHDB/FLUSHALL disabled to prevent accidental deletion
   - Critical for production environments

**Data Loss Scenarios:**

| Scenario | Max Data Loss | Recovery Method |
|----------|---------------|-----------------|
| Graceful shutdown | 0 seconds | AOF + RDB intact |
| Power failure | 1 second | AOF recovery |
| AOF corruption | Up to RDB interval | RDB snapshot recovery |
| Both corrupted | Manual recovery | Rebuild from OSS deltas |

**Cache vs Main Redis Comparison:**

| Feature | Cache Redis | Main Redis |
|---------|-------------|------------|
| Persistence | ❌ Disabled | ✅ AOF + RDB |
| Eviction | ✅ LRU enabled | ❌ No eviction |
| Data Importance | Low (rebuiltable) | Critical (index) |
| Disk I/O | None | Moderate |
| Max Data Loss | All (OK) | 1 second |
| Recovery Time | Fast (rebuild) | Instant (AOF) |

### With Performance Tracing

```go
import "github.com/hkloudou/lake/v2/trace"

ctx := trace.WithTrace(context.Background(), "Write")
client.Write(ctx, req)

tr := trace.FromContext(ctx)
fmt.Println(tr.Dump())
// Output:
// === Trace [Write]: Total 248ms ===
// [1] Init: 14.84ms
// [2] PreCommit: 2.14ms {tsSeq:..., seqID:1}
// [3] StoragePut: 203.48ms {key:..., size:5}
// [4] Commit: 2.57ms
```

## 📖 Core Concepts

### Path Format

Path follows a strict format for network-safe transmission:

- **Must start with `/`** - Like URL paths
- **Must not end with `/`** - No trailing slashes
- **Segments follow JavaScript naming** - Start with letter/`_`/`$`, followed by letters/digits/`_`/`$`/`.`
- **Root document**: Use `"/"` for entire document operations

**Valid Examples:**
```
/              → Root document
/user          → Single field
/user/profile  → Nested field (user.profile in JSON)
/user.info     → Field with dot in name (user\.info in gjson)
/$config       → Dollar sign prefix allowed
```

**Invalid Examples:**
```
user           ✗ No leading /
/user/         ✗ Trailing /
/123           ✗ Starts with number
/user-name     ✗ Contains hyphen
```

### Merge Types

Lake V2 supports three merge strategies:

#### 1. MergeTypeReplace (Simple Replacement)
```go
client.Write(ctx, lake.WriteRequest{
    Path:      "/user/name",  // Path format
    Body:      []byte(`"Alice"`),
    MergeType: lake.MergeTypeReplace,
})
```

#### 2. MergeTypeRFC7396 (JSON Merge Patch)
[RFC 7396](https://datatracker.ietf.org/doc/html/rfc7396) - Declarative merging with null deletion:

```go
// Merge patch (adds city, removes age with null)
client.Write(ctx, lake.WriteRequest{
    Path:      "/user",
    Body:      []byte(`{"city":"NYC","age":null}`),
    MergeType: lake.MergeTypeRFC7396,
})
```

#### 3. MergeTypeRFC6902 (JSON Patch)
[RFC 6902](https://datatracker.ietf.org/doc/html/rfc6902) - Imperative operations (add, remove, replace, move, copy):

```go
client.Write(ctx, lake.WriteRequest{
    Path:      "/",  // Root document
    Body:      []byte(`[
        {"op":"add","path":"/a/b/c","value":42},
        {"op":"move","from":"/a/b/c","path":"/x/y/z"}
    ]`),
    MergeType: lake.MergeTypeRFC6902,
})
```

### Atomic Writes

Lake V2 uses a two-phase commit protocol to prevent data loss:

1. **Pre-Commit** - Generate TimeSeqID and mark as pending in Redis (atomic via Lua)
2. **Storage Write** - Write to OSS/S3 (may be slow)
3. **Commit** - Remove pending, add committed (atomic via Lua)

This ensures no writes are lost even if concurrent reads create snapshots during slow OSS operations.

### Catalog Encoding

Catalogs are intelligently encoded for optimal performance:

- **Pure lowercase** (`users`): `(` prefix → `9bc6/(users`
- **Pure uppercase** (`USERS`): `)` prefix → `4020/)USERS`  
- **Mixed/unsafe** (`Users`, `中文`): base32 → `f9aa/kvzwk4tt`
- **MD5 sharding**: 65,536 directories for balanced distribution

## 🔧 Configuration

### Redis Configuration

Store configuration in Redis at key `lake.setting`:

```json
{
  "Name": "my-lake",
  "Storage": "oss",
  "Bucket": "my-bucket",
  "Endpoint": "oss-cn-hangzhou",
  "AccessKey": "your-access-key",
  "SecretKey": "your-secret-key",
  "AESPwd": "optional-encryption-key"
}
```

### Custom Storage

```go
import "github.com/hkloudou/lake/v2/internal/storage"

client := lake.NewLake(
    "redis://localhost:6379",
    lake.WithStorage(storage.NewMemoryStorage()),
)
```

## 📊 Performance

### Benchmarks

- **Write Throughput**: 999,999 operations/sec per catalog
- **Read Performance**: **2x faster** with async snapshot save (v2.2.0)
- **Delta Loading**: 10x faster with worker pool (10 concurrent)
- **Cache Hit Ratio**: ~90% typical workload
- **Atomic Overhead**: <2% (4ms for Redis operations)

### Timing Breakdown

**Write Operation:**
```
Init:        14ms  (first write only, config loading)
PreCommit:    2ms  (Redis Lua: generate ID + mark pending)
StoragePut: 180ms  (OSS write - main bottleneck)
Commit:       2ms  (Redis Lua: finalize)
───────────────────
Total:      198ms  (OSS-dominated, atomic overhead minimal)
```

**Read Operation (v2.2.0 - Async Snapshot):**
```
Before v2.2.0 (sync snapshot):
  LoadData:  180ms
  Merge:      10ms
  SnapSave:  200ms  ← Blocking!
  ──────────────
  Total:     390ms

After v2.2.0 (async snapshot):
  LoadData:  180ms
  Merge:      10ms
  SnapSave:  async  ← Non-blocking!
  ──────────────
  Total:     190ms  ← 2x faster! 🚀
```

### Key Optimizations (v2.2.0)

1. **Async Snapshot Save** - Snapshot generation no longer blocks Read response
2. **Redis-Based Lock Detection** - Uses Redis TIME to detect pending write timeouts (120s, clock-skew resistant)
3. **SingleFlight** - Prevents duplicate concurrent snapshot saves
4. **Parallel I/O** - Snapshot and deltas load concurrently
5. **Worker Pool** - 10 concurrent delta loads
6. **Smart Caching** - Redis cache with ~90% hit ratio
7. **Optimized Storage** - Simplified member format saves ~30% Redis space

## 🏗️ Architecture

### Data Format

```
Redis Index:
  {prefix}:delta:base64(catalog) -> ZADD
    score: timestamp.seqid (float: timestamp + seqid/1000000.0)
           Must have exactly 6 decimal places, seqid > 0
           Valid: 1700000000.000001 to 1700000000.999999
    
  Delta member format:
    delta|{mergeType}|{field}|{tsSeq}
    Example: delta|1|/user/name|1700000000_1
    
  Pending member format (uncommitted writes):
    pending|delta|{mergeType}|{field}|{tsSeq}
    Example: pending|delta|1|/user/name|1700000000_1
    
  Snapshot member format:
    snap|{startTsSeq}|{stopTsSeq}
    Example: snap|1700000000_1|1700000100_500

OSS Storage:
  {md5[0:4]}/{encoded}/delta/{ts}_{seqid}_{type}.json
  {md5[0:4]}/{encoded}/snap/{start}~{stop}.snap
```

### Flow Diagram

```
Write (Atomic Two-Phase Commit):
  1. Lua: GetTimeSeqID + ZADD pending|... (atomic)
  2. OSS: PUT data file
  3. Lua: ZREM pending + ZADD delta|... (atomic)

Read (Parallel + Async):
  1. List: Get snapshot info + delta index
     - Check pending writes using Redis TIME (< 120s = error, > 120s = ignore) ✨ v2.2.0
  2. Parallel Load:
     - Thread 1: Cache/OSS load snapshot data
     - Thread 2: Worker pool load delta bodies (10 concurrent)
  3. Merge: CPU-bound merge operation
  4. Async: Save new snapshot (background, non-blocking) ✨ v2.2.0
```

### What's New in v2.2.0

- **Async Snapshot Save**: Read operations no longer wait for snapshot saves (2x faster!)
- **Improved Pending Detection**: Uses Redis TIME for accurate lock expiry (120s timeout, prevents clock skew)
- **Unified Merge Interface**: Single `Merger` interface for all merge strategies (Replace, RFC7396, RFC6902)
- **Path Validation**: Strict path format with `/` prefix, network-safe for HTTP transmission
- **Enhanced Score Parsing**: Support multiple formats (underscore/decimal/float64) with 6-decimal precision validation
- **Optimized Storage**: Simplified delta member format, ~30% space saving in Redis
- **File Structure**: Code organized into write.go, read.go, snapshot.go, helpers.go
- **SingleFlight Snapshots**: Prevents duplicate concurrent snapshot generation
- **Simplified Architecture**: Removed snapMgr dependency, cleaner code

## 🧪 Testing

```bash
# Run all tests
go test ./...

# Run with trace
go test -v -run TestWriteWithTrace

# Specific package
go test -v ./internal/merge
```

## 💡 Design Philosophy & Known Behaviors

### Pending Write Detection

**Problem**: Concurrent writes with slow OSS may cause data loss during snapshots.

**Solution**: Two-phase commit with pending state
- Phase 1: Mark as `pending|` in Redis (atomic)
- Phase 2: Write to OSS
- Phase 3: Commit to `delta|` (atomic)

**Read Behavior** (v2.2.0 - Optimized):
- Uses **Redis TIME** for accurate age calculation (avoids server clock skew)
- Pending < 120s: **Error returned** (write in progress, client should retry)
- Pending > 120s: **Ignored** (abandoned write, auto-cleaned)
- Error stored in `ListResult.Err` (non-fatal, can be checked before Read)
- Background updater syncs Redis time every 5s (minimal overhead)

```go
list := client.List(ctx, catalog)
if list.Err != nil {
    // Pending writes detected (age < 120s), retry later
    time.Sleep(100 * time.Millisecond)
    list = client.List(ctx, catalog)
}
data, _ := lake.ReadMap(ctx, list)
```

### Snapshot Save Failures

**Philosophy**: Snapshot is an optimization, not critical data.

**Behavior**:
- Snapshot save failure **does not fail Read operation**
- Error recorded in trace for debugging
- Next read will regenerate snapshot
- Data consistency maintained (snapshots can be rebuilt)

### Pending Cleanup Strategy

**Current**: No automatic cleanup of old pending records.

**Rationale**:
- Pending records are rare (only during write failures)
- Manual cleanup through bulk data deletion (planned for future versions)
- Simplicity over complexity
- Avoiding background tasks and their overhead

**Future**: Unified cleanup when deleting data older than N days.

### Error Handling

**Panic Locations** (defensive programming):
1. `WithSnapCacheMetaURL()` - Invalid Redis URL at initialization
2. `makeCatalogKey()` - Prefix not set (internal invariant violation)

**Rationale**: These represent programming errors, not runtime errors. Fail-fast to catch bugs early.

## 📚 Examples

- [Basic Examples](./example_test.go) - Write, Read, RFC patches
- [Trace Examples](./trace_example_test.go) - Performance monitoring
- [Cache Examples](./cache_example_test.go) - Redis caching setup

## 🤝 Contributing

Contributions are welcome! Please ensure:

- All tests pass (`go test ./...`)
- Code is formatted (`go fmt ./...`)
- Commits are descriptive

## 📄 License

MIT License - see [LICENSE](LICENSE)

## 🔗 Links

- **GitHub**: https://github.com/hkloudou/lake
- **Issues**: https://github.com/hkloudou/lake/issues
- **Releases**: https://github.com/hkloudou/lake/releases

---

**Previous Version**: For v1 (legacy), see the `v1` branch.
