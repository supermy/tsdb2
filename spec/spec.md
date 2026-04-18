# TSDB2 完整规格说明书

> 基于 `Cargo.toml` (工作区结构)、`kimi-rocksdb.md` (架构设计)、`plan.md` (实施计划) 三份参考文档编制

---

## 1. 项目概览

### 1.1 项目定位

TSDB2 是一个基于 RocksDB 的时序数据库引擎，对标 Arrow + Parquet + DataFusion 技术栈，核心优势：

- **RocksDB LSM-Tree**：高吞吐实时写入、天然覆盖写/墓碑删除
- **Arrow 列式内存**：零拷贝扫描、SIMD 加速
- **Column Family 分离**：索引与数据分离、按日期分区 CF
- **三级 TTL 清理**：天级 drop_cf → 范围级 delete_range → 行级 CompactionFilter

### 1.2 工作区结构 (Cargo.toml)

```
tsdb2/
├── crates/
│   ├── tsdb-arrow/           # Arrow 数据模型 + DataPoint↔RecordBatch 转换
│   ├── tsdb-parquet/         # Parquet 存储引擎 (写入/读取/Compaction/WAL)
│   ├── tsdb-storage-arrow/   # Arrow 存储引擎 (WriteBuffer + Parquet)
│   ├── tsdb-datafusion/      # DataFusion SQL 查询引擎
│   ├── tsdb-rocksdb/         # RocksDB 存储引擎 (核心 crate)
│   ├── tsdb-cli/             # CLI 工具 (子命令框架已有，业务逻辑占位)
│   ├── tsdb-test-utils/      # 测试工具 (数据生成器 + 断言)
│   ├── tsdb-integration-tests/ # 集成测试 (Parquet E2E + RocksDB E2E)
│   ├── tsdb-stress/          # Parquet 压力测试
│   ├── tsdb-stress-rocksdb/  # RocksDB 压力测试
│   └── tsdb-bench/           # Criterion 基准测试
├── .github/workflows/ci.yml # CI 流水线
└── spec/                     # 规格文档
```

### 1.3 当前实现状态

| Crate | 源文件数 | 代码行数 | 单元测试 | 集成测试 | 压力测试 | 状态 |
|-------|---------|---------|---------|---------|---------|------|
| `tsdb-rocksdb` | 13 | ~2418 | 62 | 10 (rocksdb_e2e) | 15 | ✅ 核心完成 |
| `tsdb-arrow` | 5 | ~786 | 5+ | - | - | ✅ 基础完成 |
| `tsdb-parquet` | 8 | ~1437 | 多 | - | - | ✅ 基础完成 |
| `tsdb-datafusion` | 6 | ~565 | 6 | - | - | ✅ 基础完成 |
| `tsdb-storage-arrow` | 5 | ~591 | 多 | 7 (e2e) | 5 | ✅ Parquet 完成 |
| `tsdb-cli` | 1 | 202 | 0 | - | - | ⚠️ 框架占位 |
| `tsdb-test-utils` | 3 | ~181 | 0 | - | - | ✅ 工具层 |
| `tsdb-integration-tests` | 3 | ~477 | - | 17 | - | ✅ 基础完成 |
| `tsdb-stress-rocksdb` | 6 | ~595 | - | - | 15 | ✅ 基础完成 |

---

## 2. 架构设计 (基于 kimi-rocksdb.md)

### 2.1 分层架构

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

### 2.2 RocksDB 引擎核心设计

#### 2.2.1 Key 编码 (key.rs)

```
Key Layout (16 bytes, big-endian):
┌──────────────────────┬──────────────────────┐
│   tags_hash (u64)    │   timestamp (i64)    │
│     8 bytes          │     8 bytes          │
└──────────────────────┴──────────────────────┘

排序语义: 先按 tags_hash 升序，再按 timestamp 降序 (最新数据优先)
前缀查询: prefix_encode(tags_hash) = tags_hash 的 8 字节大端序
```

#### 2.2.2 Value 编码 (value.rs)

```
Value Layout (变长):
┌──────────┬─────────────┬──────────────┬─────────────┬─────┐
│ field_count│ field1_name │ field1_type  │ field1_value │ ... │
│ (u16 BE)  │ (len+bytes) │ (1 byte)     │ (variable)  │     │
└──────────┴─────────────┴──────────────┴─────────────┴─────┘

字段类型编码:
  0x00 = Float64 (8 bytes BE)
  0x01 = Int64   (8 bytes BE)
  0x02 = Utf8    (len:u16 + bytes)
  0x03 = Boolean (1 byte, 0x00/0x01)
```

#### 2.2.3 Column Family 策略

```
CF 命名规则:
  _series_meta              — 全局唯一，存储 tags_hash → tags 映射
  ts_{measurement}_{date}   — 按度量名+日期分区，如 ts_cpu_20260417

优势:
  - 按日期删除: drop_cf 即可清理过期数据
  - 查询隔离: 不同日期的查询互不干扰
  - Compaction 独立: 每个 CF 独立 Compaction，不影响其他日期
```

#### 2.2.4 三级 TTL 清理 (cleanup.rs + compaction_filter.rs)

```
Level 1 (天级): TtlManager::drop_expired_cfs()
  → 整体删除过期日期的 CF (如 ts_cpu_20260410 在 7 天后删除)
  → 最快、最彻底

Level 2 (范围级): TtlManager::delete_range_in_cf()
  → 在 CF 内按时间戳范围删除 (delete_range API)
  → 适用于同一天内部分过期数据

Level 3 (行级): TsdbTtlFilterFactory + CompactionFilter
  → Compaction 时逐行检查 timestamp，过期则 Remove
  → 最精细，依赖 Compaction 触发
```

#### 2.2.5 Merge 策略 (merge.rs)

```
字段合并语义 (union + 后者覆盖):
  existing: {cpu: 80.0, mem: 60.0}
  operand:  {cpu: 90.0, disk: 40.0}
  result:   {cpu: 90.0, mem: 60.0, disk: 40.0}

full_merge:  有 existing_value 时使用
partial_merge: 安全降级，返回 None (交给 full_merge 处理)
```

### 2.3 与 Parquet 引擎的对比

| 特性 | RocksDB Engine | Parquet Engine |
|------|---------------|----------------|
| 写入模型 | LSM-Tree，高频小写入 | 批量导入为主 |
| 更新/删除 | 天然支持 (覆盖写 + 墓碑) | 不友好 (需重写文件) |
| 时间范围查询 | CF 分区 + 前缀扫描 | 文件级 Min/Max 跳过 |
| TTL/过期 | 三级自动清理 | 外部管理 |
| Compaction | RocksDB 内置 | 自定义 hot/cold 分层 |
| 跨天查询 | 需合并多个 CF | 需合并多个 Parquet 文件 |

---

## 3. 测试体系规格

### 3.1 测试金字塔

```
                 ┌──────────────┐
                 │   E2E Tests  │  ← 17 tests (rocksdb_e2e:10 + parquet_e2e:7)
                 └──────────────┘
                ┌┴──────────────┴┐
                │  Stress Tests  │  ← 20 tests (rocksdb:15 + parquet:5)
                └────────────────┘
               ┌┴────────────────┴┐
               │  Integration     │  ← 跨模块交互测试
               └──────────────────┘
              ┌┴──────────────────┴┐
              │   Unit Tests       │  ← 62+ tests (rocksdb alone)
              └────────────────────┘
             ┌┴────────────────────┴┐
             │  Property-Based      │  ← 待补充: proptest 编解码不变量
             └──────────────────────┘
```

### 3.2 覆盖率目标

| 层级 | 目标覆盖率 | 当前估计 | 差距 |
|------|-----------|---------|------|
| 核心编解码 (key/value/tags/comparator) | ≥95% | ~90% | 需补充属性测试 |
| DB 引擎核心 (db.rs put/get/range/batch) | ≥90% | ~85% | 需补充边界场景 |
| 插件机制 (merge/filter/snapshot) | ≥85% | ~70% | merge.rs 无独立测试 |
| Arrow 适配层 | ≥80% | ~75% | 需补充空结果/错误路径 |
| CLI / 配置 | ≥75% | ~0% | CLI 无测试 |
| **整体项目加权平均** | **≥80%** | **~65%** | **CI gate 待启用** |

### 3.3 测试命名规范

```
test_{module}_{function}_{scenario}

示例:
  test_key_encode_roundtrip           — 单元: 正常编解码
  test_key_encode_empty_input         — 单元: 边界空输入
  test_db_put_and_get_single_point    — 集成: 写入单点读取
  stress_rocksdb_write_100k           — 压力: 10万点写入
  test_e2e_rocksdb_write_read_large   — E2E: 全链路
```

### 3.4 现有测试清单

#### tsdb-rocksdb 单元测试 (62 tests)

| 模块 | 测试数 | 关键测试 |
|------|--------|---------|
| db.rs | 17 | open, put/get, read_range, write_batch, merge, prefix_scan, snapshot, drop_cf, compact_cf, multi_measurement |
| key.rs | 8 | encode/decode roundtrip, prefix_encode, compute_tags_hash, boundary values |
| value.rs | 8 | encode/decode roundtrip, float/int/str/bool types, empty fields, large fields |
| tags.rs | 6 | encode/decode roundtrip, empty tags, special chars, unicode |
| comparator.rs | 5 | ordering semantics, same hash different ts, different hash |
| compaction_filter.rs | 6 | filter expired, keep valid, boundary, factory |
| config.rs | 7 | default values, presets validation |
| cleanup.rs | 2 | drop_expired_cfs, delete_range |
| arrow_adapter.rs | 3 | read_range_arrow, prefix_scan_arrow, empty_result |

#### tsdb-integration-tests (17 tests)

| 文件 | 测试数 | 关键测试 |
|------|--------|---------|
| rocksdb_e2e.rs | 10 | write_read_large, cross_day, tags_dedup, arrow, compaction, ttl, drop_cf, merge, snapshot |
| end_to_end.rs | 7 | write_read_large, tag_filtering, wal_recovery, sql_aggregation, compaction, mixed_fields |

#### tsdb-stress-rocksdb (15 tests)

| 模块 | 测试数 | 关键测试 |
|------|--------|---------|
| write_stress.rs | 3 | 100k single, 100k batch, multi_measurement |
| read_stress.rs | 3 | range_scan_100k, prefix_scan_100_series, cross_day_7days |
| concurrent_stress.rs | 3 | 4_threads_write, merge_same_key, mixed_read_write |
| recovery_stress.rs | 3 | restart_recovery, 5x_restart, compaction_then_read |
| ttl_stress.rs | 3 | mixed_data, drop_expired_cfs, high_cardinality_1000 |

### 3.5 缺失测试项

- [ ] **merge.rs 独立测试**: 当前 merge 仅在 db.rs 的 merge 测试中覆盖，缺少 full_merge/partial_merge 的独立单元测试
- [ ] **snapshot.rs 独立测试**: 当前仅在 db.rs 的 snapshot 一致性测试中覆盖
- [ ] **error.rs 路径覆盖**: 所有 Error 变体的构造和 Display 输出
- [ ] **属性测试 (proptest)**: 编解码不变量、排序不变量、合并幂等性
- [ ] **DataFusion + RocksDB SQL 集成**: 当前 E2E 中无 SQL 查询 RocksDB 数据的测试
- [ ] **引擎对比测试**: 同一数据写入 Parquet 和 RocksDB，查询结果一致性验证
- [ ] **真实场景数据生成器**: DevOps/IoT/金融场景数据生成
- [ ] **CLI 测试**: 0 个测试，所有子命令无测试覆盖

---

## 4. CLI 规格 (基于当前 tsdb-cli 状态)

### 4.1 当前状态

tsdb-cli 已有基础框架：
- `Cli` struct (clap derive): `data_dir`, `storage_engine` 参数
- `Commands` 枚举: Status, Query, Bench, Compact, Import, Archive
- `ArchiveActions` 枚举: Create, Restore, List
- **所有子命令均为占位实现** (println 输出，无实际业务逻辑)
- 依赖已配置: clap 4, serde, serde_json, toml, tracing, tokio, anyhow

### 4.2 子命令实现规格

#### `tsdb-cli status` — 数据库状态

```bash
tsdb-cli status --data-dir ./data
# 输出:
# TSDB Status (RocksDB Engine)
# Version:    tsdb2 v0.1.0
# Data Dir:   ./data
# Column Families: 31
#   _series_meta:    1,234 entries
#   ts_cpu_20260417:  1.2M entries
# Memory:
#   Block Cache:  448MB / 512MB
#   MemTables:    128MB / 256MB
```

实现要点:
- 打开 TsdbRocksDb (只读模式)
- 调用 `list_ts_cfs()` + `stats()` / `cf_stats()`
- 格式化输出到终端

#### `tsdb-cli query` — SQL 查询

```bash
tsdb-cli query --data-dir ./data --sql "SELECT AVG(usage) FROM cpu WHERE time > now() - 1h"
# 输出: JSON 格式查询结果
```

实现要点:
- 打开 TsdbRocksDb → read_range → DataPoint
- DataPoint → RecordBatch (ArrowAdapter)
- 注册到 DataFusionQueryEngine → execute SQL
- 输出格式: json (默认) / table / csv

#### `tsdb-cli bench` — 基准测试

```bash
tsdb-cli bench write --data-dir ./bench-data --points 100000
tsdb-cli bench read --data-dir ./bench-data --queries 1000
```

实现要点:
- 复用 tsdb-test-utils 数据生成器
- 测量 ops/sec, p50/p99 latency
- 输出 JSON 报告

#### `tsdb-cli compact` — 手动 Compaction

```bash
tsdb-cli compact --data-dir ./data                    # 全部 CF
tsdb-cli compact --data-dir ./data --cf ts_cpu_20260417  # 指定 CF
```

实现要点:
- 打开 TsdbRocksDb → `compact_cf(cf_name)` 或遍历 `list_ts_cfs()` 逐个 compact

#### `tsdb-cli import` — 数据导入

```bash
tsdb-cli import --data-dir ./data --file metrics.json --format json
tsdb-cli import --data-dir ./data --file metrics.lp --format line-protocol
tsdb-cli import --data-dir ./data --file metrics.csv --format csv
```

实现要点:
- 解析输入文件 → Vec<DataPoint>
- 调用 `write_batch()` 批量写入
- 进度条显示 (indicatif)

#### `tsdb-cli archive` — 归档管理

```bash
tsdb-cli archive create --data-dir ./data --older-than 30d --output-dir ./archive
tsdb-cli archive restore --data-dir ./data --archive-file ./archive/ts_cpu_20260301.sst
tsdb-cli archive list --archive-dir ./archive
```

实现要点:
- create: 遍历过期 CF → IngestExternalFile (RocksDB SST export)
- restore: IngestExternalFile 导入
- list: 扫描归档目录

### 4.3 技术选型

| 组件 | 选择 | 状态 |
|------|------|------|
| CLI 框架 | `clap` v4 (derive) | ✅ 已集成 |
| 异步运行时 | `tokio` | ✅ 已集成 |
| 错误处理 | `anyhow` | ✅ 已集成 |
| 序列化 | `serde` + `serde_json` | ✅ 已集成 |
| 配置 | `toml` | ✅ 已集成 |
| 日志 | `tracing` + `tracing-subscriber` | ✅ 已集成 |
| 进度条 | `indicatif` | ❌ 待添加 |
| Shell 补全 | `clap_complete` | ❌ 待添加 |

---

## 5. 多平台兼容性规格

### 5.1 支持矩阵

| 平台 | 架构 | 编译目标 | 优先级 | CI 状态 |
|------|------|---------|--------|---------|
| Linux x86_64 | x86_64 | x86_64-unknown-linux-gnu | P0 | ✅ 已有 |
| macOS Apple Silicon | aarch64 | aarch64-apple-darwin | P0 | ✅ 已有 |
| macOS x86_64 | x86_64 | x86_64-apple-darwin | P0 | ✅ 已有 |
| Windows x86_64 | x86_64 | x86_64-pc-windows-msvc | P0 | ✅ 已有 |
| Linux AArch64 | aarch64 | aarch64-unknown-linux-gnu | P1 | ✅ 已有 |
| Linux ARMv7 | armv7 | armv7-unknown-linux-gnueabihf | P2 | ✅ 已有 |
| Linux RISC-V 64 | riscv64gc | riscv64gc-unknown-linux-gnu | P3 | ❌ 待添加 |

### 5.2 条件编译策略

```rust
#[cfg(target_arch = "aarch64")]
pub fn optimized_config() -> RocksDbConfig {
    RocksDbConfig { block_size: 4096, .. }  // NEON 友好
}

#[cfg(target_arch = "x86_64")]
pub fn optimized_config() -> RocksDbConfig {
    RocksDbConfig { block_size: 8192, .. }  // AVX2 友好
}

#[cfg(target_arch = "arm")]
const MEMTABLE_ALIGNMENT: usize = 256;  // ARM 严格对齐
#[cfg(not(target_arch = "arm"))]
const MEMTABLE_ALIGNMENT: usize = 64;
```

### 5.3 CPU 特性检测

```rust
pub fn detect_cpu_features() -> CpuFeatures {
    CpuFeatures {
        has_avx2: is_x86_feature_detected!("avx2"),
        has_neon: cfg!(target_arch = "aarch64"),
        has_sse42: is_x86_feature_detected!("sse4.2"),
        cpu_count: num_cpus::get(),
    }
}
```

---

## 6. CI/CD 规格增强

### 6.1 当前 CI 状态

已有 jobs: check, test, clippy, fmt, audit, msrv, cross-compile, coverage

### 6.2 待增强项

| 增强项 | 当前状态 | 目标 |
|--------|---------|------|
| 覆盖率阈值门禁 | coverage job 存在但无阈值 | `--fail-under 80` |
| 性能回归检测 | 无 | criterion baseline 对比 |
| 压力测试 Gate | 手动运行 | main 分支自动运行 |
| RISC-V cross-compile | 无 | nightly check |
| Dependabot | 无 | weekly 依赖更新 |
| Docker 多架构 | 无 | amd64 + arm64 + armv7 |

---

## 7. 真实数据场景规格

### 7.1 场景 A: DevOps 监控数据

```rust
struct DevOpsDataGenerator {
    hosts: usize,              // 1000..10000
    measurements: Vec<String>, // ["cpu", "memory", "disk", "network"]
    interval_secs: u64,        // 10..60
    duration_days: u64,        // 7..30
    fields_per_metric: usize,  // 3..8
}
```

特征: 1000~10000 hosts, 10~50 measurements, 10s~60s 采集间隔

### 7.2 场景 B: IoT 传感器数据

```rust
struct IotDataGenerator {
    devices: usize,         // 10000..100000
    measurements: Vec<String>,
    interval_secs: u64,     // 1..5
    duration_days: u64,     // 1..7
}
```

特征: 10000~100000 devices, 1s~5s 高频采集

### 7.3 场景 C: 金融 K线数据

```rust
struct FinancialDataGenerator {
    tickers: usize,      // 100..5000
    bar_interval: BarInterval, // 1m/5m/15m/1h/1d
    duration_years: u64, // 1..10
}
```

特征: 100~5000 tickers, OHLCV 5 字段, 时间戳对齐到 bar interval

---

## 8. 代码质量规范

### 8.1 注释规范

```rust
/// 模块/结构体/函数的中文文档注释
///
/// 详细说明（可选）
///
/// # 参数
/// - `param` - 参数说明
///
/// # 返回
/// 返回值说明
```

- 所有 `pub fn` / `pub struct` / `pub enum` / `pub const` 添加 `///` 文档注释
- 关键编解码格式添加二进制布局图注释
- 关键逻辑添加 `//` 行内注释
- 不添加多余注释，保持代码简洁

### 8.2 日期处理规范

- 测试代码中的日期必须使用 `chrono::Utc::now()` 动态获取
- 辅助函数: `today_date()`, `ts_today()`, `ts_days_ago(n)`
- 生产代码中的时长常量 (如 `7 * 24 * 3600`) 不修改
- 测试中的固定参考时间点 (如 `1_000_000_000_000_000i64`) 不修改

### 8.3 Lint 规则

- `cargo clippy --all-targets -- -D warnings` 零警告
- `cargo fmt --all -- --check` 格式正确
- 无 `unwrap()` 在生产代码中 (测试代码允许)
- 错误处理使用 `Result<T, TsdbRocksDbError>`
