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
- **🔍 OpenTelemetry Tracing** - Native OTel spans for all operations (zero overhead when no TracerProvider configured)
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
    "log"

    "github.com/hkloudou/lake/v2"
    "go.opentelemetry.io/otel"
)

var appTracer = otel.Tracer("my-app")

func main() {
    client := lake.NewLake("redis://localhost:6379")

    ctx, span := appTracer.Start(context.Background(), "main")
    defer span.End()

    // Write data (creates child span: Lake.Write)
    err := client.Write(ctx, lake.WriteRequest{
        Catalog:   "users",
        Path:      "/profile",
        Body:      []byte(`{"name":"Alice","age":30}`),
        MergeType: lake.MergeTypeReplace,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Read merged data (creates child spans: Lake.List, Lake.Read)
    list := client.List(ctx, "users")
    jsonStr, _ := lake.ReadString(ctx, list)
    fmt.Printf("Data: %s\n", jsonStr)
}
```

## 📚 API Reference

> This section provides precise API signatures and file locations for AI assistants and automation tools.

### Package Import

```go
import "github.com/hkloudou/lake/v2"
```

### Client Creation

| Function | File | Description |
|----------|------|-------------|
| `NewLake(metaUrl string, opts ...func(*option)) *Client` | [lake.go:48](lake.go#L48) | Create client with Redis URL |
| `WithStorage(storage storage.Storage) func(*option)` | [lake.go:133](lake.go#L133) | Use custom storage |
| `WithSnapCacheMetaURL(metaUrl string, ttl time.Duration) func(*option)` | [lake.go:108](lake.go#L108) | Use separate Redis for snapshot cache |
| `WithDeltaCacheMetaURL(metaUrl string, ttl time.Duration) func(*option)` | [lake.go:124](lake.go#L124) | Use separate Redis for delta cache |

### Write Operations

| Function | File | Description |
|----------|------|-------------|
| `(*Client) Write(ctx, WriteRequest) error` | [write.go:53](write.go#L53) | Write JSON data with merge strategy |
| `(*Client) WriteFile(ctx, WriteFileRequest) error` | [file.go:21](file.go#L21) | Write binary file to catalog |

**WriteRequest struct** ([write.go:14](write.go#L14)):
```go
type WriteRequest struct {
    Catalog   string    // Document namespace (e.g., "users", "orders")
    Path      string    // JSON path starting with "/" (e.g., "/profile", "/settings/theme")
    Body      []byte    // JSON data as raw bytes
    MergeType MergeType // lake.MergeTypeReplace, lake.MergeTypeRFC7396, or lake.MergeTypeRFC6902
    Meta      []byte    // Optional metadata
}
```

**MergeType constants** ([internal/index/encoding.go:13](internal/index/encoding.go#L13)):
```go
lake.MergeTypeReplace  // = 1: Simple field replacement
lake.MergeTypeRFC7396  // = 2: RFC 7396 JSON Merge Patch (null removes field)
lake.MergeTypeRFC6902  // = 3: RFC 6902 JSON Patch (operations array)
```

### Read Operations

| Function | File | Description |
|----------|------|-------------|
| `(*Client) List(ctx, catalog string, opts ...ListOption) *ListResult` | [list.go:115](list.go#L115) | Get catalog metadata and delta list |
| `WithStrictPending() ListOption` | [list.go:111](list.go#L111) | Any pending triggers HasPending (not just mid-delta) |
| `ReadBytes(ctx, *ListResult) ([]byte, error)` | [helpers.go:11](helpers.go#L11) | Read as raw bytes |
| `ReadString(ctx, *ListResult) (string, error)` | [helpers.go:15](helpers.go#L15) | Read as JSON string ⭐ Most common |
| `ReadMap(ctx, *ListResult) (map[string]any, error)` | [helpers.go:24](helpers.go#L24) | Read as map |
| `Read[T any](ctx, *ListResult) (*T, error)` | [helpers.go:38](helpers.go#L38) | Read with generic type |

**Common Read Pattern** (⭐ Most important):
```go
// Recommended: Read as string (most common)
list := client.List(ctx, "users")
if list.Err != nil {
    return list.Err
}
jsonStr, err := lake.ReadString(ctx, list)

// Alternative: Read with type inference
list := client.List(ctx, "users")
user, err := lake.Read[User](ctx, list)

// Alternative: Read as map
list := client.List(ctx, "users")
data, err := lake.ReadMap(ctx, list)
```

**ListResult struct** ([list.go:13](list.go#L13)):
```go
type ListResult struct {
    Err        error  // Non-nil on read errors (Redis/decode failures)
    HasPending bool   // True if pending write detected (< 120s)
}

// Key methods:
func (m ListResult) Exist() bool         // Returns true if data exists
func (m ListResult) LastUpdated() float64 // Returns timestamp of last update
```

### Metadata Operations

| Function | File | Description |
|----------|------|-------------|
| `(*Client) Meta(ctx, catalog string) (string, error)` | [meta.go:7](meta.go#L7) | Get catalog metadata |
| `(*Client) BatchMeta(ctx, catalogs []string) (map[string]string, error)` | [meta.go:14](meta.go#L14) | Get multiple catalog metadata |

### File Operations

| Function | File | Description |
|----------|------|-------------|
| `(*Client) WriteFile(ctx, WriteFileRequest) error` | [file.go:21](file.go#L21) | Write binary file |
| `(*Client) FileExists(ctx, catalog, path string) (bool, error)` | [file.go:63](file.go#L63) | Check if file exists |
| `(*Client) FilesAndMeta(ctx, catalog string) (string, error)` | [file.go:75](file.go#L75) | Get all files and metadata |

### Cleanup Operations

| Function | File | Description |
|----------|------|-------------|
| `(*Client) ClearHistory(ctx, catalog string) error` | [clear.go:10](clear.go#L10) | Clear all history, keep latest snapshot |
| `(*Client) ClearHistoryWithRetention(ctx, catalog string, keepSnaps int) error` | [clear.go:26](clear.go#L26) | Clear history, keep N snapshots |

### Sampling (Advanced)

| Function | File | Description |
|----------|------|-------------|
| `(*Client) MotionSample(ctx, catalog, indicator string, motionCatalogs []string, shouldUpdated func, callback func) (float64, error)` | [sample.go:20](sample.go#L20) | Incremental sampling with change detection |

### Complete Usage Examples

**Basic Write and Read**:
```go
client := lake.NewLake("redis://localhost:6379")

ctx, span := otel.Tracer("my-app").Start(context.Background(), "BasicExample")
defer span.End()

// Write
err := client.Write(ctx, lake.WriteRequest{
    Catalog:   "users",
    Path:      "/profile",
    Body:      []byte(`{"name":"Alice","age":30}`),
    MergeType: lake.MergeTypeReplace,
})

// Read (⭐ most common pattern)
list := client.List(ctx, "users")
if list.Err != nil {
    log.Fatal(list.Err)
}
jsonStr, err := lake.ReadString(ctx, list)
```

**RFC 7396 Merge Patch** (partial update, null removes field):
```go
err := client.Write(ctx, lake.WriteRequest{
    Catalog:   "users",
    Path:      "/profile",
    Body:      []byte(`{"age":31,"city":"NYC","oldField":null}`),
    MergeType: lake.MergeTypeRFC7396,
})
```

**RFC 6902 JSON Patch** (operations):
```go
err := client.Write(ctx, lake.WriteRequest{
    Catalog:   "users",
    Path:      "/",
    Body:      []byte(`[{"op":"add","path":"/tags","value":["vip"]}]`),
    MergeType: lake.MergeTypeRFC6902,
})
```

**Read with Type**:
```go
type UserProfile struct {
    Name string `json:"name"`
    Age  int    `json:"age"`
}

list := client.List(ctx, "users")
profile, err := lake.Read[UserProfile](ctx, list)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Name: %s, Age: %d\n", profile.Name, profile.Age)
```

**Handle Pending Writes**:
```go
// Default: only pending BEFORE delta triggers HasPending (tail pending is ignored)
list := client.List(ctx, "users")
if list.HasPending {
    time.Sleep(100 * time.Millisecond)
    list = client.List(ctx, "users")
}
data, err := lake.ReadString(ctx, list)

// Strict mode: ANY pending triggers HasPending (useful for strong consistency)
list := client.List(ctx, "users", lake.WithStrictPending())
```

**Patch with Empty Body (Meta-Only Fast Path)**:
```go
// RFC7396/RFC6902 with empty body ({}, [], "") skips the 3-step write,
// only updates metadata — no pending member, no storage write.
err := client.Write(ctx, lake.WriteRequest{
    Catalog:   "users",
    Path:      "/",
    Body:      []byte(`{}`),
    Meta:      []byte(`{"updated":"2024-01-01"}`),
    MergeType: lake.MergeTypeRFC7396,
})
```

### File Structure

```
lake/
├── lake.go          # Client creation, options
├── write.go         # Write(), WriteRequest
├── read.go          # Internal read implementation
├── list.go          # List(), ListResult
├── helpers.go       # ReadBytes, ReadString, ReadMap, Read[T]
├── file.go          # WriteFile, FileExists, FilesAndMeta
├── clear.go         # ClearHistory, ClearHistoryWithRetention
├── meta.go          # Meta, BatchMeta
├── sample.go        # MotionSample
├── snapshot.go      # Internal snapshot management
└── internal/
    ├── index/       # Redis index operations
    ├── storage/     # OSS, File, Memory storage
    ├── merge/       # RFC 7396, RFC 6902 merge
    ├── cache/       # Redis, Memory cache
    └── config/      # Configuration management
```

## ⚙️ Configuration

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

### 🔍 OpenTelemetry Tracing

Lake uses [OpenTelemetry](https://opentelemetry.io/) natively — no custom trace package needed. Spans are automatically created for all core operations: `Lake.Write`, `Lake.Read`, `Lake.List`, `Lake.WriteFile`, `Lake.MotionSample`, and `Lake.ClearHistory`.

**Integrating with Grafana Tempo (gRPC):**

```go
import (
    "context"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func initTracer() func() {
    ctx := context.Background()
    exporter, _ := otlptracegrpc.New(ctx,
        otlptracegrpc.WithEndpoint("tempo:4317"),
        otlptracegrpc.WithInsecure(),
    )
    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(exporter),
        sdktrace.WithResource(resource.NewWithAttributes(
            semconv.SchemaURL,
            semconv.ServiceNameKey.String("my-service"),
        )),
    )
    otel.SetTracerProvider(tp)
    otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
        propagation.TraceContext{},
        propagation.Baggage{},
    ))
    return func() { tp.Shutdown(context.Background()) }
}
```

**Integrating with Grafana Tempo (HTTP/HTTPS):**

```go
import "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"

// HTTP (insecure)
exporter, _ := otlptracehttp.New(ctx,
    otlptracehttp.WithEndpoint("tempo:4318"),
    otlptracehttp.WithInsecure(),
)

// HTTPS (e.g. Grafana Cloud)
exporter, _ := otlptracehttp.New(ctx,
    otlptracehttp.WithEndpoint("tempo.grafana.net"),
    otlptracehttp.WithURLPath("/otlp/v1/traces"),
)
// Then use exporter with sdktrace.NewTracerProvider as above
```

> **Note:** `otel.Tracer("lake")` (instrumentation library name) and `semconv.ServiceNameKey.String("my-service")` (service name) are different concepts and do not conflict. The service name identifies *your* application; the tracer name identifies the lake library. In Tempo they appear as `service.name=my-service` and `otel.library.name=lake`.

**Usage 1: Gin Middleware (recommended for HTTP services)**

Extract traceID from upstream request headers (`traceparent`), lake spans become children of the HTTP span:

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/codes"
    "go.opentelemetry.io/otel/propagation"
    "go.opentelemetry.io/otel/trace"
)

func TracingMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        // Extract trace context from upstream request headers (traceparent)
        ctx := otel.GetTextMapPropagator().Extract(
            c.Request.Context(),
            propagation.HeaderCarrier(c.Request.Header),
        )

        spanName := c.Request.Method + " " + c.FullPath()
        ctx, span := otel.Tracer("my-service").Start(ctx, spanName,
            trace.WithSpanKind(trace.SpanKindServer),
            trace.WithAttributes(
                attribute.String("http.method", c.Request.Method),
                attribute.String("http.target", c.Request.URL.Path),
            ),
        )
        defer span.End()

        // Inject span context back into request so handlers can access it
        c.Request = c.Request.WithContext(ctx)
        c.Next()

        span.SetAttributes(attribute.Int("http.status_code", c.Writer.Status()))
        if c.Writer.Status() >= 400 {
            span.SetStatus(codes.Error, c.Errors.String())
        }
    }
}

func main() {
    shutdown := initTracer()
    defer shutdown()

    r := gin.Default()
    r.Use(TracingMiddleware())

    r.GET("/api/users", func(c *gin.Context) {
        ctx := c.Request.Context() // ctx has parent span from middleware

        // Lake.List and Lake.Read automatically become child spans
        list := client.List(ctx, "users")
        data, _ := lake.ReadString(ctx, list)
        c.String(200, data)
    })
}
```

Tempo trace structure:
```
GET /api/users               (root span, traceID from traceparent or auto-generated)
  └─ Lake.List               (child span, library: lake)
       └─ Index.ReadRange
  └─ Lake.Read               (child span, library: lake)
       └─ Cache.Redis.Take
```

**Usage 2: Standalone (no HTTP, e.g. cron jobs, CLI tools)**

Manually create a root span, lake spans become its children:

```go
shutdown := initTracer()
defer shutdown()

client := lake.NewLake("redis://localhost:6379")

// Create root span manually
ctx, rootSpan := otel.Tracer("my-app").Start(context.Background(), "SyncJob")
defer rootSpan.End()

// Lake spans are children of SyncJob
list := client.List(ctx, "users")
data, _ := lake.ReadString(ctx, list)
```

Tempo trace structure:
```
SyncJob                      (root span, self-generated traceID)
  └─ Lake.List
       └─ Index.ReadRange
  └─ Lake.Read
       └─ Cache.Redis.Take
```

**Span Hierarchy:**

| Parent Span | Child Spans |
|-------------|-------------|
| `Lake.Write` | `Index.Commit` |
| `Lake.Read` | `Lake.FillDeltasBody`, `Cache.Redis.Take`, `Cache.Memory.Take` |
| `Lake.List` | `Index.ReadRange` |
| `Lake.MotionSample` | `Index.GetSampleScore`, `Index.UpdateSampleScore`, `Lake.List` |
| `Lake.ClearHistory` | — |
| `Lake.WriteFile` | — |

> 💡 If no `TracerProvider` is configured, OpenTelemetry uses a noop tracer with zero overhead.

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

### What's New in v2.3.x

- **OpenTelemetry Migration**: Replaced custom `trace` package with native OpenTelemetry instrumentation — spans are automatically created for all core operations (`Lake.Write`, `Lake.Read`, `Lake.List`, etc.)
- **Pending/Error Separation**: `HasPending` and `Err` are now independent — `Err` is only for real errors, pending state is conveyed solely via `HasPending`
- **Position-Aware Pending Detection**: By default, only pending members that appear before a delta trigger `HasPending` (tail pending is harmless and ignored)
- **`WithStrictPending()` Option**: Opt-in strict mode where any pending triggers `HasPending`, for strong consistency scenarios
- **Meta-Only Fast Path**: Patch writes (RFC7396/RFC6902) with empty body (`{}`, `[]`, `""`) skip the 3-step write entirely, only updating metadata — no pending member created, no storage I/O

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

# Run with verbose output
go test -v

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

**Read Behavior** (v2.3.x - Optimized):
- Uses **Redis TIME** for accurate age calculation (avoids server clock skew)
- Pending > 120s: **Ignored** (abandoned write, auto-cleaned)
- Pending < 120s (default): **HasPending=true only when pending appears before a delta** (tail pending is harmless)
- Pending < 120s (strict): **HasPending=true for any pending** (via `WithStrictPending()`)
- `Err` is reserved for real errors (Redis/decode failures), pending state uses `HasPending` only
- Background updater syncs Redis time every 5s (minimal overhead)

```go
list := client.List(ctx, catalog)
if list.HasPending {
    // Pending writes detected, retry later
    time.Sleep(100 * time.Millisecond)
    list = client.List(ctx, catalog)
}
data, _ := lake.ReadMap(ctx, list)
```

### Snapshot Save Failures

**Philosophy**: Snapshot is an optimization, not critical data.

**Behavior**:
- Snapshot save failure **does not fail Read operation**
- Error recorded as a span event for debugging (visible in OTel traces)
- Next read will regenerate snapshot
- Data consistency maintained (snapshots can be rebuilt)

### Write Safety & Orphan File Handling

Lake V2's two-phase commit ensures **zero data loss** even in failure scenarios. Here's how each failure case is handled:

#### Failure Scenarios

| Failure Point | State After Failure | Cleanup Method |
|---------------|---------------------|----------------|
| PreCommit fails | Clean - no dirty data | None needed |
| StoragePut fails | `pending` in Redis, no file | Rollback removes `pending` ✓ |
| Commit fails | `pending` in Redis + file in storage | `ClearHistory` handles it ✓ |
| Process crash (after StoragePut, before Commit) | `pending` in Redis + file in storage | `ClearHistory` handles it ✓ |

#### Why No Immediate Cleanup on Commit Failure?

When Commit fails, the system intentionally **does not** immediately delete the orphan file. This design is safer:

1. **Commit failure may be transient** - Network glitch, Redis failover, etc.
2. **Pending acts as a protection** - Read operations detect `pending` and return an error, preventing inconsistent reads
3. **ClearHistory is the correct cleanup point** - User explicitly calls cleanup when historical data is no longer needed, ensuring sufficient time has passed

#### How ClearHistory Cleans Orphan Files

The `ClearHistory` API handles all cleanup scenarios:

```go
// ClearHistory removes old deltas and snapshots
// This also cleans up any orphan files from failed commits
client.ClearHistory(ctx, "users")
```

**Internal flow:**
1. `ReadSafeRemoveRange` returns all delta members (including `pending` members)
2. For each delta/pending member, derive the storage path using `MakeDeltaKey(catalog, tsSeq, mergeType)`
3. Delete storage files in parallel (10 workers)
4. Batch delete Redis members via `ZREM`

**Key insight**: The `pending` member contains all information needed to reconstruct the storage file path:
```
pending|delta|{mergeType}|{path}|{timestamp}_{seqid}
              ↓              ↓
          mergeType        tsSeq
```

Combined with `catalog` (from the ZSet key), we can call:
```go
storageDeltaKey := storage.MakeDeltaKey(catalog, tsSeq, mergeType)
storage.Delete(ctx, storageDeltaKey)
```

#### No Fragment Tracking Needed

Unlike traditional two-phase commit systems, Lake V2 **does not require** a separate fragment/orphan tracking table because:

1. **Pending member IS the fragment tracker** - Contains all info to locate orphan files
2. **ClearHistory is comprehensive** - Cleans both committed deltas and uncommitted pending entries
3. **Simpler architecture** - No additional Redis keys or background cleanup tasks

#### Read Safety During Pending State

When a `pending` member exists:
- **Age < 120 seconds**: Read returns error (write in progress, client should retry)
- **Age > 120 seconds**: `pending` is ignored (considered abandoned, will be cleaned by `ClearHistory`)

```go
list := client.List(ctx, "users")
if list.HasPending {
    // Active write in progress, retry later
    return fmt.Errorf("pending write detected")
}
```

#### Best Practices

1. **Call `ClearHistory` periodically** - This is the unified cleanup mechanism
2. **Use `ClearHistoryWithRetention`** - Keep recent snapshots for performance
3. **Don't worry about orphan files** - They will be cleaned when you call `ClearHistory`
4. **Trust the pending mechanism** - It prevents inconsistent reads during failures

### Error Handling

**Panic Locations** (defensive programming):
1. `WithSnapCacheMetaURL()` - Invalid Redis URL at initialization
2. `makeCatalogKey()` - Prefix not set (internal invariant violation)

**Rationale**: These represent programming errors, not runtime errors. Fail-fast to catch bugs early.

## 📚 Examples

- [Basic Examples](./example_test.go) - Write, Read, RFC patches
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
