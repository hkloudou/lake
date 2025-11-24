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
    "github.com/hkloudou/lake/v2/internal/index"
)

func main() {
    // Create client (config loaded lazily)
    client := lake.NewLake("redis://localhost:6379")
    ctx := context.Background()
    
    // Write data
    _, err := client.Write(ctx, lake.WriteRequest{
        Catalog:   "users",
        Field:     "profile",
        Body:      []byte(`{"name":"Alice","age":30}`),
        MergeType: index.MergeTypeReplace,
    })
    
    // List catalog entries
    list, _ := client.List(ctx, "users")
    
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
    lake.WithRedisCache("redis://localhost:6379", 5*time.Minute),
)
```

### With Performance Tracing

```go
import "github.com/hkloudou/lake/v2/internal/trace"

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

### Merge Types

Lake V2 supports three merge strategies:

#### 1. MergeTypeReplace (Simple Replacement)
```go
client.Write(ctx, lake.WriteRequest{
    Field:     "user.name",
    Body:      []byte(`"Alice"`),
    MergeType: lake.MergeTypeReplace,
})
```

#### 2. MergeTypeRFC7396 (JSON Merge Patch)
[RFC 7396](https://datatracker.ietf.org/doc/html/rfc7396) - Declarative merging with null deletion:

```go
// Merge patch (adds city, removes age with null)
client.Write(ctx, lake.WriteRequest{
    Field:     "user",
    Body:      []byte(`{"city":"NYC","age":null}`),
    MergeType: lake.MergeTypeRFC7396,
})
```

#### 3. MergeTypeRFC6902 (JSON Patch)
[RFC 6902](https://datatracker.ietf.org/doc/html/rfc6902) - Imperative operations (add, remove, replace, move, copy):

```go
client.Write(ctx, lake.WriteRequest{
    Field:     "",  // Empty = root document
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
- **Read Performance**: 31% faster with parallel I/O
- **Delta Loading**: 10x faster with worker pool (10 concurrent)
- **Cache Hit Ratio**: ~90% typical workload
- **Atomic Overhead**: <2% (4ms for Redis operations)

### Timing Breakdown (typical write)

```
Init:        14ms  (first write only, config loading)
PreCommit:    2ms  (Redis Lua: generate ID + mark pending)
StoragePut: 180ms  (OSS write - main bottleneck)
Commit:       2ms  (Redis Lua: finalize)
───────────────────
Total:      198ms  (OSS-dominated, atomic overhead minimal)
```

## 🏗️ Architecture

### Data Format

```
Redis Index:
  {prefix}:delta:base64(catalog) -> ZADD
    score: timestamp.seqid
    member: delta|base64(field)|ts_seqid|mergetype

OSS Storage:
  {md5[0:4]}/{encoded}/delta/{ts}_{seqid}_{type}.json
  {md5[0:4]}/{encoded}/snap/{start}~{stop}.snap
```

### Flow Diagram

```
Write:
  1. Lua: GetTimeSeqID + ZADD pending|...
  2. OSS: PUT data file
  3. Lua: ZREM pending + ZADD delta|...

Read:
  1. Redis: Get snapshot info + delta index
  2. Parallel:
     - Cache/OSS: Load snapshot data
     - Worker Pool: Load delta bodies (10 concurrent)
  3. CPU: Merge all data
  4. OSS: Save new snapshot (if needed)
```

## 🧪 Testing

```bash
# Run all tests
go test ./...

# Run with trace
go test -v -run TestWriteWithTrace

# Specific package
go test -v ./internal/merge
```

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
