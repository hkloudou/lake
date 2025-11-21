# Lake V2

>  A high-performance JSON document system based on Redis ZADD + OSS storage.

## Features

- 🚀 **High Performance**: Concurrent writes with no locks
- 📊 **Incremental Reads**: Only read data since last snapshot
- 🔄 **Smart Snapshots**: Generate snapshots on-demand during reads
- 🛡️ **Data Consistency**: Redis ZADD for ordering, OSS for immutable storage
- ⚡ **JS Merge Engine**: Flexible JSON merging with embedded JavaScript (goja)
- 🎯 **Simple API**: Easy-to-use Writer and Reader interfaces

## Architecture

### Storage Model

```
Redis Index (ZADD):
  catalog:{name} -> sorted set
    score: timestamp
    member: "field:uuid"

OSS Storage:
  /{catalog}/{uuid}.json
```

### Snapshot Mechanism

Snapshots are generated on-demand during reads:

```
Redis Snapshot Index:
  catalog:{name}:snap -> sorted set
    score: last_timestamp
    member: "snap:{snap_uuid}"
```

##Installation

```bash
go get github.com/hkloudou/lake/v2
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    
    lake "github.com/hkloudou/lake/v2/pkg/client"
)

func main() {
    // Create client
    client := lake.New(lake.Config{
        RedisAddr: "localhost:6379",
        OSSConfig: lake.OSSConfig{
            Endpoint: "oss-cn-hangzhou.aliyuncs.com",
            Bucket:   "my-bucket",
        },
    })
    
    // Write data
    err := client.Write(context.Background(), lake.WriteRequest{
        Catalog:   "users",
        Field:     "profile.name",
        Value:     map[string]any{"first": "John", "last": "Doe"},
    })
    
    // Read data with auto-snapshot
    result, err := client.Read(context.Background(), lake.ReadRequest{
        Catalog:      "users",
        GenerateSnap: true,
    })
    
    fmt.Println(result.Data) // Merged JSON document
}
```

## Design

See [DESIGN_V2.md](./DESIGN_V2.md) for detailed architecture design.

## Performance

- **Writes**: ~10,000 ops/sec (single Redis instance)
- **Reads**: ~5,000 ops/sec with snapshot
- **Snapshots**: Generated on-demand, no performance impact

## License

MIT License

## Previous Version

For v1 (legacy), see the `v1` branch.
