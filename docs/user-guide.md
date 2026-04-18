# tsdb2 使用手册

## 安装与构建

### 前置要求

- Rust 1.80.0+
- Cargo

### 构建

```bash
git clone <repo-url> tsdb2
cd tsdb2
cargo build --release
```

### 运行测试

```bash
cargo test --all
cargo test -p tsdb-stress
cargo bench -p tsdb-bench
```

## 基本使用

### 1. 打开存储引擎

```rust
use tsdb_storage_arrow::engine::ArrowStorageEngine;
use tsdb_storage_arrow::config::ArrowStorageConfig;

let config = ArrowStorageConfig {
    wal_enabled: true,
    max_buffer_rows: 100_000,
    flush_interval_ms: 5000,
    ..Default::default()
};

let engine = ArrowStorageEngine::open("/data/tsdb", config)?;
```

### 2. 写入数据

```rust
use tsdb_arrow::schema::{DataPoint, FieldValue};

let dp = DataPoint::new("cpu", 1_700_000_000_000_000)
    .with_tag("host", "server01")
    .with_tag("region", "us-west")
    .with_field("usage", FieldValue::Float(0.75))
    .with_field("idle", FieldValue::Float(0.25))
    .with_field("count", FieldValue::Integer(42));

engine.write(&dp)?;

let dps: Vec<DataPoint> = (0..1000)
    .map(|i| {
        DataPoint::new("cpu", 1_700_000_000_000_000 + i as i64 * 10_000_000)
            .with_tag("host", "server01")
            .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.001))
    })
    .collect();

engine.write_batch(&dps)?;
engine.flush()?;
```

### 3. 读取数据

```rust
use tsdb_arrow::schema::Tags;

let tags = Tags::new();
let results = engine.read_range("cpu", &tags, 1_700_000_000_000_000, 1_700_003_600_000_000)?;

for dp in &results {
    println!("ts={}, host={:?}, usage={:?}",
        dp.timestamp,
        dp.tags.get("host"),
        dp.fields.get("usage"));
}
```

### 4. Tag 过滤读取

```rust
let mut tags = Tags::new();
tags.insert("host".to_string(), "server01".to_string());

let results = engine.read_range("cpu", &tags, start_ts, end_ts)?;
```

### 5. 单点查询

```rust
let tags = Tags::new();
let point = engine.get_point("cpu", &tags, 1_700_000_000_000_000)?;
```

### 6. SQL 查询

```rust
let result = engine.execute_sql(
    "SELECT AVG(usage) as avg_usage, COUNT(*) as cnt FROM cpu",
    "cpu",
    &datapoints,
).await?;

println!("columns: {:?}", result.columns);
for row in &result.rows {
    println!("{:?}", row);
}
```

### 7. time_bucket 聚合

```rust
let result = engine.execute_sql(
    "SELECT time_bucket(timestamp, 3600000000) as bucket, AVG(usage) as avg_usage FROM cpu GROUP BY bucket ORDER BY bucket",
    "cpu",
    &datapoints,
).await?;
```

## 配置参数详解

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `hot_days` | u64 | 7 | 热数据天数，热数据使用 Snappy 压缩 |
| `retention_days` | u64 | 30 | 数据保留天数，过期分区自动清理 |
| `max_buffer_rows` | usize | 100_000 | 写入缓冲区最大行数 |
| `flush_interval_ms` | u64 | 5000 | 缓冲区自动刷盘间隔（毫秒） |
| `max_rows_per_file` | usize | 1_000_000 | 单 Parquet 文件最大行数 |
| `row_group_size` | usize | 1_000_000 | Parquet Row Group 大小 |
| `wal_enabled` | bool | true | 是否启用 WAL 预写日志 |
| `wal_sync_interval_ms` | u64 | 100 | WAL sync 间隔（毫秒） |
| `memory_limit` | usize | 1_073_741_824 | 内存限制（字节），1GB |

## Compaction 策略

Compaction 将多个小 Parquet 文件合并为一个较大的文件：

- `min_files_to_compact`: 最小文件数（默认 4）
- `max_file_size`: 合并后文件最大大小（默认 64MB）
- 冷数据使用 ZSTD(level=3) 压缩
- 热数据使用 Snappy 压缩

```rust
let results = engine.compact()?;
```

## WAL 配置与恢复

WAL (Write-Ahead Log) 确保 crash 后数据不丢失：

1. 每次 `write()` / `write_batch()` 时，数据先写入 WAL
2. 缓冲区刷盘后，WAL 条目标记为已持久化
3. 引擎重启时，自动从 WAL 恢复未刷盘的数据

```rust
let config = ArrowStorageConfig {
    wal_enabled: true,
    wal_sync_interval_ms: 100,
    ..Default::default()
};
```

## 性能调优

### 写入优化

- 增大 `max_buffer_rows` 减少刷盘频率
- 使用 `write_batch()` 代替逐点 `write()`
- 禁用 WAL 可提升写入吞吐（牺牲 crash 安全性）

### 读取优化

- 使用 tag 过滤减少扫描数据量
- 使用 `read_range_arrow()` + projection 进行列裁剪
- 定期执行 `compact()` 减少小文件数量

### 存储优化

- 冷热数据分层编码：热数据 Snappy（快速解压），冷数据 ZSTD（高压缩比）
- 调大 `row_group_size` 提高列式读取效率
- 设置合理的 `retention_days` 自动清理过期数据

## 常见问题

### Q: 写入后读取不到数据？

确保调用了 `flush()` 将缓冲区数据刷盘。`read_range()` 会自动 flush 缓冲区，但直接使用 `TsdbParquetReader` 不会。

### Q: SQL 查询返回空结果？

`execute_sql()` 需要先注册表（`register_from_datapoints`），且数据需要已写入磁盘。

### Q: Compaction 后数据丢失？

Compaction 是合并操作，合并前后数据点总数应一致。如果发现数据丢失，请检查 `min_files_to_compact` 配置。

### Q: 跨平台兼容性？

tsdb2 纯 Rust 实现，无平台特定代码，支持 Linux / macOS / Windows，支持 x86_64 和 AArch64。
