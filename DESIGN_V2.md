# Lake V2 Architecture Design

## 概述

Lake V2 是一个基于 Redis ZADD + OSS 的高性能 JSON 文档写入系统。

## 核心设计

### 1. 数据存储结构

#### Redis 索引层
```
ZADD {prefix}:data:{catalog_name} {score} "data|{base64_field}|{ts}_{seqid}|{mergetype}"
```

- **Key**: `{prefix}:data:{catalog_name}` (e.g., `oss:mylake:data:users`)
- **Score**: Float64 = timestamp + (seqid / 1000000)
  - Timestamp: Unix timestamp (seconds) from Redis TIME
  - SeqID: Auto-incremented sequence per second (1-999999)
  - Example: ts=1700000000, seqid=123 → score=1700000000.000123
- **Member**: `data|{base64_field}|{ts}_{seqid}|{mergetype}`
  - Format uses `|` delimiter (safe for parsing)
  - `base64_field`: Base64 URL-encoded JSON path (supports any characters including `:`)
  - `ts_seqid`: Unique identifier from Redis (e.g., `1700000000_123`)
  - `mergetype`: 0=Replace, 1=Merge
  
**Example:**
```
Field: "user.profile.name"
Base64: "dXNlci5wcm9maWxlLm5hbWU="
Member: "data|dXNlci5wcm9maWxlLm5hbWU=|1700000000_123|0"
```

#### OSS 存储层
```
/{catalog_name}/{ts}_{seqid}_{mergetype}.json
```

Example: `/users/1700000000_123_0.json`

### 2. 快照机制

快照在读取时按需生成，使用时间范围标记：

```
ZADD {prefix}:snap:{catalog_name} {stop_score} "snap|{startTsSeq}|{stopTsSeq}"
```

- **Member**: `snap|{startTsSeq}|{stopTsSeq}` 
  - `startTsSeq`: 快照起始时间序列（首个快照为 `0_0`）
  - `stopTsSeq`: 快照结束时间序列（最后一个数据的 TsSeqID）
- **Score**: 快照结束点的 score（从 stopTsSeq 计算）
- 快照后只需读取 score > snap_score 的增量数据

**示例：**
```
第一个快照: snap|0_0|1700000100_500 (score: 1700000100.0005)
第二个快照: snap|1700000100_500|1700000200_999 (score: 1700000200.000999)
```

**优势：**
- 明确的时间范围，便于数据追踪和审计
- 快照之间无缝衔接，startTsSeq = 前一个快照的 stopTsSeq
- 第一个快照从 0_0 开始，表示从头开始

### 3. 写入流程

```
1. 原子生成 timestamp + seqid (via Redis Lua script)
2. ZADD to Redis index with score and member
3. 写入 JSON 到 OSS: /{catalog}/{ts}_{seqid}_{mergetype}.json
4. 返回 WriteResult{TsSeqID, Timestamp, SeqID}
```

**Redis Lua Script for TimeSeq Generation (with Catalog Isolation):**
```lua
-- KEYS[1]: base64 encoded catalog name
local catalog = KEYS[1]
local timeResult = redis.call("TIME")
local timestamp = timeResult[1]

-- Sequence key includes catalog for isolation
local seqKey = "lake:seqid:" .. catalog .. ":" .. timestamp

-- Initialize sequence counter if not exists (expires in 5 seconds)
local setResult = redis.call("SETNX", seqKey, "0")
if setResult == 1 then
    redis.call("EXPIRE", seqKey, 5)
end

-- Increment and return
local seqid = redis.call("INCR", seqKey)

return {timestamp, seqid}
```

**Key Features:**
- ⏱️ **Server-side timestamp**: No client clock skew
- 🔢 **Unique seqid**: Supports up to 999,999 writes/second per catalog
- 🏷️ **Catalog isolation**: Each catalog has independent seqid sequence
- 🔐 **AES-GCM encryption**: Optional encryption at OSS layer (no performance impact)
- 🎯 **Merge strategies**: Replace (overwrite) or Merge (deep merge)

**Catalog Isolation:**
- Different catalogs (e.g., "users", "products") have independent seqid sequences
- Redis key format: `lake:seqid:{base64_catalog}:{timestamp}`
- Example: `lake:seqid:dXNlcnM=:1700000000` for "users" catalog
- Prevents seqid conflicts between different data types

### 4. 读取流程（两段式）

```
第一阶段：获取快照和增量索引信息
1. 检查最新快照: ZREVRANGEBYSCORE {prefix}:snap:{catalog} +inf -inf LIMIT 0 1
   返回: snap|{startTsSeq}|{stopTsSeq} score={stop_score}

2. 获取增量数据索引:
   如果有快照: ZRANGEBYSCORE {prefix}:data:{catalog} ({stop_score} +inf
   如果无快照: ZRANGEBYSCORE {prefix}:data:{catalog} 1 +inf  (score>0，排除已清理数据)

第二阶段：加载实际数据
3. 从 OSS 加载快照 JSON: catalog/{stopTsSeq}.json
4. 从 OSS 加载增量数据: catalog/{ts}_{seqid}_{mergetype}.json (for each entry)

第三阶段：合并数据
5. 合并: snapshot.data + incremental.data

第四阶段：生成新快照（可选）
6. 保存合并后的数据
7. 创建新快照: snap|{old_stopTsSeq}|{new_stopTsSeq}
```

**时间范围示例：**
```
初始状态:
  - data: 1, 2, 3, ..., 100 (TsSeqID: 1700000000_1 到 1700000100_100)
  
第一次读取并生成快照:
  - 快照: snap|0_0|1700000100_100
  - 包含所有数据 (1-100)
  
新数据写入:
  - data: 101, 102, 103 (TsSeqID: 1700000100_101 到 1700000100_103)
  
第二次读取:
  - 读取快照: snap|0_0|1700000100_100 (获取 1-100)
  - 读取增量: score > 1700000100.0001 (获取 101-103)
  - 合并得到完整数据 (1-103)
  
生成新快照:
  - 快照: snap|1700000100_100|1700000100_103
  - 包含所有数据 (1-103)
```

### 5. JSON 合并策略

支持三种标准的合并策略：

#### 5.1 MergeTypeReplace (0) - 完全替换
直接使用 `sjson.SetRawBytes` 设置字段值，完全覆盖原值。

```go
// Example
client.Write(ctx, WriteRequest{
    Field:     "user.name",
    Value:     "Alice",
    MergeType: MergeTypeReplace, // 完全替换
})
```

#### 5.2 MergeTypeMerge (1) - RFC 7396 JSON Merge Patch
实现 [RFC 7396](https://datatracker.ietf.org/doc/html/rfc7396) 标准的 JSON Merge Patch。

**特性：**
- 在 field 范围内进行局部合并
- null 值表示删除字段
- 递归合并对象
- 数组完全替换（不合并）

```go
// Example: Merge at field level
client.WriteRFC7396(ctx, "users", "profile", []byte(`{
    "age": 31,
    "city": "NYC",
    "oldField": null
}`))

// Or using Write with MergeTypeMerge
client.Write(ctx, WriteRequest{
    Field:     "user",
    Value:     map[string]any{"age": 31, "city": "NYC"},
    MergeType: MergeTypeMerge, // RFC 7396
})
```

**RFC 7396 测试用例（所有通过）：**
| Original | Patch | Result |
|----------|-------|--------|
| `{"a":"b"}` | `{"a":"c"}` | `{"a":"c"}` |
| `{"a":"b"}` | `{"b":"c"}` | `{"a":"b","b":"c"}` |
| `{"a":"b"}` | `{"a":null}` | `{}` |
| `{"a":{"b":"c"}}` | `{"a":{"b":"d","c":null}}` | `{"a":{"b":"d"}}` |

#### 5.3 MergeTypeRFC6902 (2) - RFC 6902 JSON Patch
实现 [RFC 6902](https://datatracker.ietf.org/doc/html/rfc6902) 标准的 JSON Patch。

**特性：**
- 支持复杂操作：add, remove, replace, move, copy, test
- 全文档级别操作
- 自动创建缺失的父路径（增强）

```go
client.WriteRFC6902(ctx, "users", []byte(`[
    { "op": "add", "path": "/a/b/c", "value": 42 },
    { "op": "move", "from": "/a/b/c", "path": "/a/b/d" }
]`))
```

### 6. 技术栈

- **Go**: 核心语言
- **Redis**: 索引和快照管理 (ZADD, ZRANGEBYSCORE)
- **OSS/S3**: JSON 文档存储
- **goja**: 不含 cgo 的 JS 引擎，用于 JSON 合并
- **SingleFlight**: 防止并发重复计算

### 7. 并发控制

- 写入：无锁，Redis ZADD 原子性保证
- 读取：SingleFlight 防止重复快照生成
- 快照：使用 Redis 事务保证一致性

## 目录结构

```
lake/
├── cmd/
│   └── server/              # HTTP/gRPC 服务
├── internal/
│   ├── catalog/             # Catalog 管理
│   ├── storage/             # OSS 存储抽象
│   │   ├── oss.go           # 阿里云 OSS
│   │   ├── s3.go            # AWS S3
│   │   └── local.go         # 本地文件 (测试)
│   ├── index/               # Redis 索引
│   │   ├── writer.go        # ZADD 写入
│   │   ├── reader.go        # ZRANGEBYSCORE 读取
│   │   └── encoding.go      # field:uuid 编码/解码
│   ├── merge/               # JSON 合并引擎
│   │   ├── engine.go        # goja JS 引擎封装
│   │   ├── strategies.go    # 合并策略
│   │   └── scripts/         # JS 脚本
│   ├── snapshot/            # 快照管理
│   │   ├── manager.go       # 快照生成/读取
│   │   └── strategy.go      # 快照触发策略
│   └── xsync/               # 并发工具
│       └── singleflight.go  # 防止重复计算
├── pkg/
│   └── client/              # 客户端 SDK
│       ├── writer.go        # 写入 API
│       └── reader.go        # 读取 API
├── go.mod
├── go.sum
├── README.md
└── DESIGN_V2.md
```

## 性能优化

1. **写入优化**
   - 异步写入 OSS
   - Redis 管道批量 ZADD
   - 无锁设计

2. **读取优化**
   - 快照缓存
   - SingleFlight 防止惊群
   - 增量读取

3. **快照优化**
   - 按需生成
   - 异步生成
   - 过期策略

## 数据一致性

1. **写入一致性**: 先写 OSS，后写 Redis（失败可重试）
2. **读取一致性**: Redis 作为真实数据源，OSS 不可变
3. **快照一致性**: Redis 事务保证原子性

## 容错设计

1. **写入失败**: 客户端重试
2. **OSS 故障**: 降级到 Redis 元数据
3. **Redis 故障**: 从 OSS 重建索引
4. **快照损坏**: 回退到增量合并

## 示例

### 写入示例
```go
result, err := client.Write(ctx, WriteRequest{
    Catalog:   "users",
    Field:     "profile.name",
    Value:     map[string]any{"first": "John", "last": "Doe"},
    MergeType: index.MergeTypeReplace, // 0=Replace, 1=Merge
})
// result.TsSeqID:   "1700000000_123"
// result.Timestamp: 1700000000
// result.SeqID:     123
```

### 读取示例
```go
result, err := client.Read(ctx, ReadRequest{
    Catalog:      "users",
    GenerateSnap: true, // 自动生成快照
})
// result.Data: 合并后的完整 JSON
// result.Snapshot: 快照信息
```

