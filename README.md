# TSDB2

高性能时序数据库引擎，基于 Rust 构建，融合 RocksDB LSM-Tree 与 Arrow 列式内存两大技术栈，支持 Flight SQL 远程查询。

[![CI](https://github.com/supermy/tsdb2/actions/workflows/ci.yml/badge.svg)](https://github.com/supermy/tsdb2/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust 1.85+](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)

## 📥 下载安装

| 平台 | 架构 | 下载链接 |
|------|------|---------|
| Linux | x86_64 | [tsdb-cli-v0.2.0-linux-amd64.tar.gz](https://github.com/supermy/tsdb2/releases/download/v0.2.0/tsdb-cli-v0.2.0-linux-amd64.tar.gz) |
| Linux | aarch64 | [tsdb-cli-v0.2.0-linux-arm64.tar.gz](https://github.com/supermy/tsdb2/releases/download/v0.2.0/tsdb-cli-v0.2.0-linux-arm64.tar.gz) |
| macOS | Apple Silicon | [tsdb-cli-v0.2.0-macos-arm64.tar.gz](https://github.com/supermy/tsdb2/releases/download/v0.2.0/tsdb-cli-v0.2.0-macos-arm64.tar.gz) |
| macOS | x86_64 | [tsdb-cli-v0.2.0-macos-amd64.tar.gz](https://github.com/supermy/tsdb2/releases/download/v0.2.0/tsdb-cli-v0.2.0-macos-amd64.tar.gz) |
| Windows | x86_64 | [tsdb-cli-v0.2.0-windows-amd64.zip](https://github.com/supermy/tsdb2/releases/download/v0.2.0/tsdb-cli-v0.2.0-windows-amd64.zip) |

> 📌 所有二进制文件均由 GitHub Actions 自动构建，附带 SHA256 校验和。

### 从源码构建

```bash
git clone https://github.com/supermy/tsdb2.git
cd tsdb2
cargo build --release -p tsdb-cli
# 二进制文件位于 target/release/tsdb-cli
```

---

## ✨ 功能特性

- **双存储引擎**: RocksDB (实时写入) + Parquet (批量分析) 可切换，数据互通
- **Flight SQL 服务**: Arrow Flight gRPC 协议，支持远程 SQL 查询与数据写入
- **SQL 查询引擎**: 内置 DataFusion SQL，支持 SELECT / WHERE / GROUP BY / 聚合 / time_bucket UDF
- **列投影下推**: 查询时只读取所需列，减少内存占用和 I/O
- **谓词下推**: 时间范围过滤下推到存储层，减少数据传输
- **批量读取 (MultiGET)**: 利用 RocksDB multi_get_cf 批量点查，比逐个 get 快 3x
- **三级 TTL 清理**: 天级 drop_cf → 范围级 delete_range → 行级 CompactionFilter
- **分层压缩**: L0-L2 无压缩 (热点) + L3+ Zstd (冷数据)，兼顾写入延迟与存储效率
- **一致性快照**: 基于 RocksDB Snapshot 的 MVCC 读取
- **字段合并 (Merge)**: 同一 Key 多次写入自动合并字段 (union 语义)
- **Arrow 原生**: DataPoint ↔ RecordBatch 零拷贝转换，SIMD 加速
- **WAL 预写日志**: BufWriter 常驻文件句柄，保证数据持久性
- **原子 Compaction**: 临时文件 + rename 原子替换，防止崩溃后数据重复
- **INI 配置策略**: write-heavy / read-heavy / balanced / memory-limited / iot 五种预设配置
- **CLI 工具**: status / query / bench / compact / import / archive / export / doctor
- **多平台**: Linux (amd64/arm64/armv7) / macOS (arm64/amd64) / Windows

---

## 🏗️ 架构设计

### 分层架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                      接入层 (Access Layer)                           │
│  CLI ─── DataFusion SQL          Flight SQL ─── gRPC (do_get/do_put)│
└────────────────────────────┬────────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────────┐
│                    执行引擎 (Execution Engine)                        │
│  DataFusion: 谓词下推 + 列投影 + Limit 下推 + time_bucket UDF       │
└────────────────────────────┬────────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────────┐
│                    存储引擎 (Dual Storage Engine)                     │
│  ┌──────────────────────────┐  ┌──────────────────────────────────┐ │
│  │  RocksDB Engine (热数据)  │  │  Parquet Engine (冷数据)         │ │
│  │  CF: ts_{m}_{date}       │  │  Partition: date-based           │ │
│  │  CF: _series_meta        │  │  Compaction: hot/cold tiers      │ │
│  │  Key: hash+timestamp     │  │  Encoding: SNAPPY(hot)/ZSTD(cold)│ │
│  │  Merge: field union      │  │  WAL: BufWriter + CRC32          │ │
│  │  TTL: 3-level cleanup    │  │  Atomic: tmp+rename              │ │
│  └──────────┬───────────────┘  └──────────┬───────────────────────┘ │
│             │         DataArchiver         │                         │
│             │◄────── JSONL Bridge ────────►│                         │
└─────────────┼──────────────────────────────┼─────────────────────────┘
              │                              │
┌─────────────▼──────────────────────────────▼─────────────────────────┐
│                    数据模型层 (Arrow Data Model)                       │
│  DataPoint ↔ RecordBatch 双向转换 | Schema Builder | Memory Pool     │
└─────────────────────────────────────────────────────────────────────┘
```

### 双引擎数据流

```
写入路径: DataPoint → WAL(可选) → WriteBuffer → RocksDB CF / Parquet File
读取路径: RocksDB read_range / Parquet read_range → DataPoint → RecordBatch
查询路径: SQL → DataFusion → TableProvider → 列投影+谓词下推 → RecordBatch
归档路径: RocksDB expired CF → JSONL → Parquet File (冷数据归档)
远程路径: Flight SQL do_get → DataFusion SQL → RecordBatch → FlightData Stream
远程写入: FlightData Stream → RecordBatch → DataPoint → RocksDB write_batch
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

tsdb-flight
├── tsdb-rocksdb
├── tsdb-datafusion
├── arrow-flight (54)
├── tonic (0.12)
└── arrow-ipc (54)

tsdb-rocksdb
├── tsdb-arrow
├── rocksdb
├── fxhash (0.2)
└── chrono (0.4)

tsdb-datafusion
├── tsdb-arrow
├── tsdb-parquet
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
| 远程协议 | Arrow Flight | 54 | gRPC 高性能数据传输 |
| RPC 框架 | Tonic | 0.12 | gRPC 服务端/客户端 |
| CLI 框架 | Clap | 4 | 命令行解析 |
| 异步运行时 | Tokio | 1 | 异步 I/O |
| 序列化 | Serde + serde_json | 1 | JSON 导入/导出 |

---

## 📊 性能基准

> 测试环境: Apple M-series / 16GB RAM / Release 模式 / 单线程

### RocksDB 引擎

| 操作 | 数据量 | QPS | 说明 |
|------|--------|-----|------|
| 批量写入 (write_batch) | 200K points | ~175K pts/s | Tags 去重 + WriteBatch 原子提交 |
| 范围读取 (read_range) | 200K points | ~209K pts/s | 全范围扫描 + 时间过滤 |
| 批量点查 (multi_get) | 1K keys | ~67K pts/s | RocksDB multi_get_cf, 比逐个 get 快 3x |
| 单点查询 (get) | 1 key | ~50K ops/s | get_pinned_cf 零拷贝 |
| 前缀扫描 (prefix_scan) | 100 series | ~150K pts/s | 指定标签前缀 |
| 跨天扫描 | 7 days | ~100K pts/s | 跨多个 CF 合并 |
| 列投影读取 | 200K points | ~250K pts/s | 只解码指定字段 |

### 写入优化对比

| 优化项 | 优化前 | 优化后 | 提升 |
|--------|--------|--------|------|
| WriteBatch Tags 去重 | 100K pts/s | 175K pts/s | 1.75x |
| MultiGET vs for-get | ~20K pts/s | ~67K pts/s | 3.3x |
| 分层压缩 (L0-L2 None, L3+ Zstd) | — | 写入延迟降低 | — |
| WAL BufWriter 常驻句柄 | 每次 append 重新打开 | 常驻 BufWriter | ~10x |

### 分层压缩策略

| LSM Level | 压缩算法 | 说明 |
|-----------|---------|------|
| L0-L2 | None (无压缩) | 热点数据, 降低写入延迟和 CPU 开销 |
| L3+ | Zstd | 冷数据, 最大化压缩比, 减少磁盘占用 |

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
let dp = DataPoint::new("cpu", chrono::Utc::now().timestamp_micros())
    .with_tag("host", "server01")
    .with_field("usage", FieldValue::Float(0.85))
    .with_field("idle", FieldValue::Float(0.15))
    .with_field("count", FieldValue::Integer(100));

db.put("cpu", &dp.tags, dp.timestamp, &dp.fields)?;

// 批量写入
db.write_batch(&datapoints)?;

// 单点查询
let result = db.get("cpu", &tags, timestamp)?;

// 批量点查 (MultiGET, 比逐个 get 快 3x)
let keys = vec![(tags1.clone(), ts1), (tags2.clone(), ts2)];
let results = db.multi_get("cpu", &keys)?;

// 范围读取
let result = db.read_range("cpu", start_ts, end_ts)?;

// 列投影读取 (只解码指定字段)
let result = db.read_range_projection("cpu", start_ts, end_ts, &["usage", "idle"])?;

// 前缀扫描 (指定标签)
let tags = Tags::from_iter([("host".to_string(), "server01".to_string())]);
let result = db.prefix_scan("cpu", &tags, start_ts, end_ts)?;

// SQL 查询 (通过 DataFusion)
let engine = tsdb_datafusion::DataFusionQueryEngine::new("./data");
engine.register_from_datapoints("cpu", &result)?;
let query_result = engine.execute("SELECT AVG(usage) FROM cpu").await?;
```

### Flight SQL 服务

```rust
use tsdb_flight::TsdbFlightServer;
use tsdb_rocksdb::RocksDbConfig;

// 启动 Flight SQL 服务 (自动注册已有 measurement)
let server = TsdbFlightServer::with_rocksdb(
    "./data",
    RocksDbConfig::default(),
    "0.0.0.0",
    50051,
)?;

// 通过 gRPC 客户端查询
// do_get(Ticket { ticket: b"SELECT AVG(usage) FROM cpu" }) → FlightData Stream
// do_put(FlightData Stream) → 写入数据到 RocksDB
// do_action("list_measurements") → 列出所有 measurement
```

### 配置策略

```bash
# 使用预设配置
# configs/balanced.ini    — 均衡配置 (默认)
# configs/write-heavy.ini — 写密集场景
# configs/read-heavy.ini  — 读密集场景
# configs/memory-limited.ini — 内存受限场景
# configs/iot.ini         — IoT 高频采集场景
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
| 单元测试 | 228 | 编解码、比较器、合并、配置、TTL、快照、MultiGET、投影 |
| 属性测试 (proptest) | 11 | Key/Value 编解码不变量、排序不变量 |
| 集成测试 | 65 | 写入→读取全链路、SQL 查询、引擎对比、Flight SQL |
| 业务测试 | 27 | IoT 监控、金融行情、运维 APM 三大场景 |
| 压力测试 | 22 | 100K+ 写入、并发安全、恢复、TTL |
| CLI 测试 | 7 | 子命令参数解析、输出验证 |
| **总计** | **360** | **✅ 全部通过** |

### 业务场景测试

| 场景 | 测试数 | 覆盖路径 |
|------|--------|---------|
| IoT 监控 | 8 | 高频写入、多设备查询、位置聚合、列投影、全链路、TTL |
| 金融行情 | 8 | Tick 写入、最新价查询、OHLC 聚合、Merge 合并、双引擎一致性 |
| 运维 APM | 9 | 高基数写入、错误率 SQL、P99 延迟、资源聚合、Compaction 全链路 |

### 运行测试

```bash
# 单元 + 集成测试
cargo test --workspace --exclude tsdb-stress-rocksdb

# 业务场景测试
cargo test -p tsdb-integration-tests -- business

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
│   ├── tsdb-rocksdb/           # RocksDB 存储引擎 (核心)
│   ├── tsdb-arrow/             # Arrow 数据模型 + 转换
│   ├── tsdb-parquet/           # Parquet 存储引擎
│   ├── tsdb-storage-arrow/     # Arrow 存储引擎 (整合层)
│   ├── tsdb-datafusion/        # DataFusion SQL 查询引擎
│   ├── tsdb-flight/            # Arrow Flight SQL 服务端
│   ├── tsdb-cli/               # CLI 工具
│   ├── tsdb-test-utils/        # 测试工具 (数据生成器 + 断言)
│   ├── tsdb-integration-tests/ # 集成测试 + 业务场景测试
│   ├── tsdb-stress/            # Parquet 压力测试
│   ├── tsdb-stress-rocksdb/    # RocksDB 压力测试
│   └── tsdb-bench/             # Criterion 基准测试
├── configs/                    # 预设配置策略
│   ├── balanced.ini
│   ├── write-heavy.ini
│   ├── read-heavy.ini
│   ├── memory-limited.ini
│   └── iot.ini
├── docs/                       # 文档
│   ├── architecture.md         # 技术架构文档
│   ├── data-structures.md      # 数据结构详述
│   ├── key-technologies.md     # 关键技术详述
│   ├── test-report.md          # 测试报告
│   └── user-guide.md           # 使用手册
├── spec/                       # 规格文档
├── .github/workflows/          # CI/CD
├── Dockerfile                  # Docker 多阶段构建
└── docker-bake.hcl             # Docker 多架构配置
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
