# TSDB2

高性能时序数据库引擎，基于 Rust 构建，融合 RocksDB LSM-Tree 与 Arrow 列式内存两大技术栈。

[![CI](https://github.com/user/tsdb2/actions/workflows/ci.yml/badge.svg)](https://github.com/user/tsdb2/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust 1.85+](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)

## 📥 下载安装

| 平台 | 架构 | 下载链接 |
|------|------|---------|
| Linux | x86_64 | [tsdb-cli-v0.1.0-linux-amd64.tar.gz](https://github.com/user/tsdb2/releases/download/v0.1.0/tsdb-cli-v0.1.0-linux-amd64.tar.gz) |
| Linux | aarch64 | [tsdb-cli-v0.1.0-linux-arm64.tar.gz](https://github.com/user/tsdb2/releases/download/v0.1.0/tsdb-cli-v0.1.0-linux-arm64.tar.gz) |
| macOS | Apple Silicon | [tsdb-cli-v0.1.0-macos-arm64.tar.gz](https://github.com/user/tsdb2/releases/download/v0.1.0/tsdb-cli-v0.1.0-macos-arm64.tar.gz) |
| macOS | x86_64 | [tsdb-cli-v0.1.0-macos-amd64.tar.gz](https://github.com/user/tsdb2/releases/download/v0.1.0/tsdb-cli-v0.1.0-macos-amd64.tar.gz) |
| Windows | x86_64 | [tsdb-cli-v0.1.0-windows-amd64.zip](https://github.com/user/tsdb2/releases/download/v0.1.0/tsdb-cli-v0.1.0-windows-amd64.zip) |

> 📌 所有二进制文件均由 GitHub Actions 自动构建，附带 SHA256 校验和。

### 从源码构建

```bash
git clone https://github.com/user/tsdb2.git
cd tsdb2
cargo build --release -p tsdb-cli
# 二进制文件位于 target/release/tsdb-cli
```

---

## ✨ 功能特性

- **双存储引擎**: RocksDB (实时写入) + Parquet (批量分析) 可切换
- **SQL 查询**: 内置 DataFusion SQL 引擎，支持 SELECT / WHERE / GROUP BY / 聚合函数
- **三级 TTL 清理**: 天级 drop_cf → 范围级 delete_range → 行级 CompactionFilter
- **一致性快照**: 基于 RocksDB Snapshot 的 MVCC 读取
- **字段合并 (Merge)**: 同一 Key 多次写入自动合并字段 (union 语义)
- **Arrow 原生**: DataPoint ↔ RecordBatch 零拷贝转换，SIMD 加速
- **CLI 工具**: status / query / bench / compact / import / archive / export / doctor
- **多平台**: Linux (amd64/arm64/armv7) / macOS (arm64/amd64) / Windows

---

## 🏗️ 架构设计

### 分层架构

```
┌─────────────────────────────────────────────────────────────┐
│                    查询层 (Query Layer)                       │
│  CLI / HTTP API → DataFusion SQL → Logical Plan → Optimizer │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                  执行引擎 (Execution Engine)                  │
│  Arrow RecordBatch Stream: Filter → Project → Aggregate     │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                  存储引擎 (Storage Engine)                    │
│  ┌──────────────────────┐  ┌──────────────────────────────┐ │
│  │  RocksDB Engine      │  │  Parquet Engine              │ │
│  │  CF: ts_{m}_{date}   │  │  Partition: date-based       │ │
│  │  CF: _series_meta    │  │  Compaction: hot/cold tiers  │ │
│  │  Key: hash+timestamp │  │  Format: Parquet columnar    │ │
│  └──────────────────────┘  └──────────────────────────────┘ │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                  文件系统层 (File System)                     │
│  WAL Files / SST Files / Manifest / Object Storage (S3)     │
└─────────────────────────────────────────────────────────────┘
```

### RocksDB 引擎核心设计

#### Key 编码 (16 bytes, big-endian)

```
┌──────────────────────┬──────────────────────┐
│   tags_hash (u64)    │   timestamp (i64)    │
│     8 bytes          │     8 bytes          │
└──────────────────────┴──────────────────────┘
排序: 先 tags_hash 升序, 再 timestamp 升序
前缀查询: prefix_encode(tags_hash) = 前 8 字节
```

#### Column Family 策略

```
_series_meta              — 全局唯一, 存储 tags_hash → tags 映射
ts_{measurement}_{date}   — 按度量名+日期分区
```

#### 三级 TTL 清理

| 级别 | 方法 | 粒度 | 速度 |
|------|------|------|------|
| L1 | `drop_expired_cfs()` | 整个 CF (天级) | 最快 |
| L2 | `delete_range_in_cf()` | 时间范围 | 中等 |
| L3 | `CompactionFilter` | 单行 | 最精细 |

---

## 🔧 技术架构

### Crate 依赖关系

```
tsdb-cli
├── tsdb-rocksdb ──── rocksdb (0.24, multi-threaded-cf)
├── tsdb-datafusion ── datafusion (46)
├── tsdb-arrow ─────── arrow (54)
└── tsdb-parquet ───── parquet (54)

tsdb-rocksdb
├── tsdb-arrow
├── rocksdb
├── fxhash (0.2)
└── chrono (0.4)

tsdb-datafusion
├── tsdb-arrow
├── datafusion
└── arrow
```

### 核心技术栈

| 组件 | 技术 | 版本 | 用途 |
|------|------|------|------|
| 存储引擎 | RocksDB | 0.24 | LSM-Tree 高吞吐写入 |
| 列式内存 | Apache Arrow | 54 | 零拷贝向量化计算 |
| 列式存储 | Apache Parquet | 54 | 冷数据压缩存储 |
| SQL 引擎 | DataFusion | 46 | ANSI SQL 查询 |
| CLI 框架 | Clap | 4 | 命令行解析 |
| 异步运行时 | Tokio | 1 | 异步 I/O |
| 序列化 | Serde + serde_json | 1 | JSON 导入/导出 |

---

## 📊 性能基准

> 测试环境: Apple M-series / 16GB RAM / Release 模式 / 单线程

### RocksDB 引擎

| 操作 | 数据量 | QPS | 说明 |
|------|--------|-----|------|
| 逐条写入 (put) | 100K points | ~8K pts/s | 单线程逐条写入 |
| 批量写入 (write_batch) | 100K points | ~50K pts/s | 批量写入, batch=100K |
| 范围读取 (read_range) | 100K points | ~200K pts/s | 单次范围查询 |
| 前缀扫描 (prefix_scan) | 100 series | ~150K pts/s | 指定标签前缀 |
| 跨天扫描 | 7 days | ~100K pts/s | 跨多个 CF 合并 |

### 批量写入对比

| Batch Size | 10K points 耗时 | 吞吐 |
|-----------|----------------|------|
| 1 | ~1.2s | ~8K pts/s |
| 100 | ~0.3s | ~33K pts/s |
| 1,000 | ~0.15s | ~67K pts/s |
| 10,000 | ~0.1s | ~100K pts/s |

### 并发安全

| 场景 | 线程数 | 数据完整性 |
|------|--------|-----------|
| 并发写入 | 4 threads × 250K | ✅ 无丢失 |
| 并发写入 | 8 threads × 2.5K | ✅ 无丢失 |
| 读写混合 | 1W + 1R | ✅ 无 deadlock |
| 并发 Merge | 4 threads | ✅ 结果确定 |

---

## 🚀 使用说明

### CLI 命令

```bash
# 查看数据库状态
tsdb-cli status --data-dir ./data

# SQL 查询 (JSON 输出)
tsdb-cli query --data-dir ./data --sql "SELECT AVG(usage) FROM cpu" --format json

# SQL 查询 (CSV 输出)
tsdb-cli query --data-dir ./data --sql "SELECT * FROM cpu WHERE host='server01'" --format csv

# 基准测试
tsdb-cli bench --mode write --data-dir ./bench-data --points 100000
tsdb-cli bench --mode read --data-dir ./bench-data --points 100000

# 手动 Compaction
tsdb-cli compact --data-dir ./data
tsdb-cli compact --data-dir ./data --cf ts_cpu_20260418

# 导入数据
tsdb-cli import --data-dir ./data --file metrics.json --format json --batch-size 10000

# 导出数据
tsdb-cli export --data-dir ./data --measurement cpu --output cpu_export.json --format json

# 归档管理
tsdb-cli archive list --data-dir ./data --archive-dir ./archive
tsdb-cli archive create --data-dir ./data --older-than 30d --output-dir ./archive

# 健康检查
tsdb-cli doctor --data-dir ./data
```

### Rust API

```rust
use tsdb_rocksdb::{TsdbRocksDb, RocksDbConfig};
use tsdb_arrow::schema::{DataPoint, FieldValue, Tags};

// 打开数据库
let db = TsdbRocksDb::open("./data", RocksDbConfig::default())?;

// 写入数据点
let mut tags = Tags::new();
tags.insert("host".to_string(), "server01".to_string());

let dp = DataPoint::new("cpu", chrono::Utc::now().timestamp_micros())
    .with_tag("host", "server01")
    .with_field("usage", FieldValue::Float(0.85))
    .with_field("idle", FieldValue::Float(0.15))
    .with_field("count", FieldValue::Integer(100));

db.put("cpu", &dp.tags, dp.timestamp, &dp.fields)?;

// 批量写入
db.write_batch(&datapoints)?;

// 范围读取
let result = db.read_range("cpu", start_ts, end_ts)?;

// 前缀扫描 (指定标签)
let tags = Tags::from_iter([("host".to_string(), "server01".to_string())]);
let result = db.prefix_scan("cpu", &tags, start_ts, end_ts)?;

// SQL 查询 (通过 DataFusion)
let engine = tsdb_datafusion::DataFusionQueryEngine::new("./data");
engine.register_from_datapoints("cpu", &result)?;
let query_result = engine.execute("SELECT AVG(usage) FROM cpu").await?;
```

### JSON 导入格式

```json
[
  {
    "measurement": "cpu",
    "tags": {"host": "server01", "region": "us-east"},
    "fields": {
      "usage": 0.85,
      "idle": 0.15,
      "count": 100
    },
    "timestamp": 1713427200000000
  }
]
```

---

## 🧪 测试体系

### 测试金字塔

| 层级 | 数量 | 覆盖范围 |
|------|------|---------|
| 单元测试 | 104 | 编解码、比较器、合并、配置、TTL、快照 |
| 属性测试 (proptest) | 11 | Key/Value 编解码不变量、排序不变量 |
| 集成测试 | 22 | 写入→读取全链路、SQL 查询、引擎对比 |
| 压力测试 | 20 | 100K+ 写入、并发安全、恢复、TTL |
| CLI 测试 | 7 | 子命令参数解析、输出验证 |

### 运行测试

```bash
# 单元 + 集成测试
cargo test --workspace --exclude tsdb-stress-rocksdb

# 压力测试 (耗时较长)
cargo test -p tsdb-stress-rocksdb

# 基准测试
cargo bench --workspace

# 代码覆盖率
cargo tarpaulin --workspace --exclude tsdb-stress-rocksdb --fail-under 80
```

---

## 📁 项目结构

```
tsdb2/
├── crates/
│   ├── tsdb-rocksdb/         # RocksDB 存储引擎 (核心)
│   ├── tsdb-arrow/           # Arrow 数据模型 + 转换
│   ├── tsdb-parquet/         # Parquet 存储引擎
│   ├── tsdb-storage-arrow/   # Arrow 存储引擎 (整合层)
│   ├── tsdb-datafusion/      # DataFusion SQL 查询引擎
│   ├── tsdb-cli/             # CLI 工具
│   ├── tsdb-test-utils/      # 测试工具 (数据生成器 + 断言)
│   ├── tsdb-integration-tests/ # 集成测试
│   ├── tsdb-stress/          # Parquet 压力测试
│   ├── tsdb-stress-rocksdb/  # RocksDB 压力测试
│   └── tsdb-bench/           # Criterion 基准测试
├── spec/                     # 规格文档
├── .github/workflows/        # CI/CD
├── Dockerfile                # Docker 多阶段构建
└── docker-bake.hcl           # Docker 多架构配置
```

---

## 🤝 贡献

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

### 开发要求

- Rust 1.85+
- `cargo clippy --all-targets -- -D warnings` 零警告
- `cargo fmt --all -- --check` 格式正确
- `cargo test --workspace --exclude tsdb-stress-rocksdb` 全部通过
- 覆盖率 ≥80%

---

## 📄 许可证

MIT License. 详见 [LICENSE](LICENSE) 文件。
