# Lake V2

> A high-performance JSON document system based on Redis ZADD + OSS storage.

## Features

- 🚀 **High Performance**: Concurrent writes with no locks
- 📊 **Incremental Reads**: Only read data since last snapshot
- 🔄 **Smart Snapshots**: Generate snapshots on-demand during reads
- 🛡️ **Data Consistency**: Redis ZADD for ordering, OSS for immutable storage
- ⚡ **JS Merge Engine**: Flexible JSON merging with embedded JavaScript (goja)
- 🎯 **Simple API**: Single entry point with lazy initialization
- ⚙️ **Lazy Config**: Config loaded automatically on first operation

## Architecture

### Storage Model

```
Redis Index (ZADD):
  catalog:{name} -> sorted set
    score: timestamp
    member: "field:uuid"

OSS Storage:
  /{catalog}/{uuid}.json

Redis Config:
  lake.setting -> JSON config
```

### Lazy Initialization

Client initialization is instant and never fails. Configuration is loaded automatically on first operation.

## Installation

```bash
go get github.com/hkloudou/lake/v2
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    
    "github.com/hkloudou/lake/v2"
)

func main() {
    // Create client - instant, no error
    // Config is loaded lazily on first operation
    client := lake.NewLake("redis://localhost:6379")
    
    ctx := context.Background()
    
    // First operation triggers config load from Redis
    // If no config exists, falls back to memory storage
    err := client.Write(ctx, lake.WriteRequest{
        Catalog: "users",
        Field:   "profile.name",
        Value:   map[string]any{"first": "John", "last": "Doe"},
    })
    
    // Read data with auto-snapshot
    result, err := client.Read(ctx, lake.ReadRequest{
        Catalog:      "users",
        GenerateSnap: true,
    })
    
    fmt.Println(result.Data) // Merged JSON document
}
```

## Configuration Management

Configuration is stored in Redis at key `lake.setting`:

```go
import "github.com/hkloudou/lake/v2/internal/config"

// Set config in Redis
cfg := &config.Config{
    Name:      "my-lake",
    Storage:   "oss",  // or "memory", "s3", "local"
    Bucket:    "my-bucket",
    AccessKey: "your-key",
    SecretKey: "your-secret",
    AESPwd:    "encryption-key",
}

client.UpdateConfig(ctx, cfg)

// Get current config
cfg, err := client.GetConfig(ctx)
```

## Custom Storage

You can provide custom storage at initialization:

```go
import "github.com/hkloudou/lake/v2/internal/storage"

client := lake.NewLake("localhost:6379", func(opt *lake.Option) {
    opt.Storage = storage.NewMemoryStorage()
})
```

## Design

See [DESIGN_V2.md](./DESIGN_V2.md) for detailed architecture design.

## Performance

- **Writes**: ~10,000 ops/sec (single Redis instance)
- **Reads**: ~5,000 ops/sec with snapshot
- **Snapshots**: Generated on-demand, no performance impact
- **Config Load**: Once per client lifecycle (uses SingleFlight)

## Key Design Principles

1. **Single Entry Point**: `NewLake(metaUrl, opts...)` - simple and intuitive
2. **Lazy Loading**: Config loaded automatically, no initialization errors
3. **Async Behavior**: Config becomes a pre-check for operations
4. **Graceful Fallback**: If config missing, falls back to memory storage
5. **Thread-Safe**: All operations are concurrent-safe

## License

MIT License

## Previous Version

For v1 (legacy), see the `v1` branch.
