# Lake V2 Architecture Design

## 概述

Lake V2 是一个基于 Redis ZADD + OSS 的高性能 JSON 文档写入系统。

## 核心设计

### 1. 数据存储结构

#### Redis 索引层
```
ZADD catalog:{catalog_name} {timestamp} "{field}:{uuid}"
```

- **Key**: `catalog:{catalog_name}`
- **Score**: Unix timestamp (写入时间)
- **Member**: `{field}:{uuid}` 编码的字符串
  - `field`: JSON path (如 `user.profile.name`)
  - `uuid`: 唯一标识符

#### OSS 存储层
```
/{catalog_name}/{uuid}.json
```

### 2. 快照机制

快照在读取时按需生成：

```
ZADD catalog:{catalog_name}:snap {last_timestamp} "snap:{snap_uuid}"
```

- 快照的 score = 最后一个 JSON 文档的 timestamp
- 快照后只需读取 score > snap_timestamp 的增量数据

### 3. 写入流程

```
1. 生成 UUID
2. 写入 JSON 到 OSS: /{catalog}/{uuid}.json
3. ZADD catalog:{catalog} {timestamp} "{field}:{uuid}"
4. 返回成功
```

### 4. 读取流程

```
1. 检查最新快照: ZREVRANGEBYSCORE catalog:{catalog}:snap +inf -inf LIMIT 0 1
2. 如果有快照:
   - 读取快照 JSON
   - ZRANGEBYSCORE catalog:{catalog} (snap_ts +inf
   - 合并增量数据
3. 如果无快照:
   - ZRANGEBYSCORE catalog:{catalog} -inf +inf
   - 从头合并所有数据
4. (可选) 生成新快照
```

### 5. JSON 合并策略

使用 JS 引擎 (goja) 执行合并逻辑：

```javascript
// 类似 MySQL JSON_INSERT, JSON_SET, JSON_REPLACE
function merge(base, field, value, strategy) {
  // INSERT: 只在不存在时插入
  // SET: 覆盖或插入
  // REPLACE: 只在存在时替换
}
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
client.Write(ctx, WriteRequest{
    Catalog:   "users",
    Field:     "profile.name",
    Value:     map[string]any{"first": "John", "last": "Doe"},
    Timestamp: time.Now(),
})
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

