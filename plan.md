# 中文注释 + 修复硬编码日期 实施计划

## 一、目标

1. **所有 Rust 源文件添加中文注释**: 60 个 .rs 文件，约 5588 行代码
2. **修复硬编码日期时间变量**: 测试代码中的硬编码日期改为动态获取当前日期

## 二、已完成工作

以下文件已添加中文注释（上一轮会话完成）：

| 文件 | 中文注释行数 | 状态 |
|------|------------|------|
| `tsdb-rocksdb/src/db.rs` | 549 | ✅ 已完成 |
| `tsdb-rocksdb/src/key.rs` | 233 | ✅ 已完成 |
| `tsdb-rocksdb/src/merge.rs` | 134 | ✅ 已完成 |
| `tsdb-rocksdb/src/lib.rs` | 166 | ✅ 已完成 |
| `tsdb-rocksdb/src/error.rs` | 87 | ✅ 已完成 |

## 三、待完成工作

### 批次 1: tsdb-rocksdb 剩余文件 (8 文件)

| 文件 | 行数 | 注释重点 | 硬编码日期修复 |
|------|------|---------|--------------|
| `value.rs` | 254 | 字段编解码格式、四种类型编码细节 | 无 |
| `tags.rs` | 133 | 标签编解码格式 | 无 |
| `compaction_filter.rs` | 129 | TTL 过滤逻辑、Factory 模式 | 无 (测试中的 `1_000_000_000_000_000i64` 是测试参考时间，无需修改) |
| `cleanup.rs` | 106 | 三级清理策略、drop_expired_cfs | ✅ `ts_for_date(2026,4,10)` → `now - 7天`, `ts_for_date(2026,4,17)` → `now` |
| `comparator.rs` | 79 | 排序语义 (先 tags_hash 后 timestamp) | 无 |
| `config.rs` | 93 | 各字段含义和推荐值 | 无 (默认值是时长，不是日期) |
| `arrow_adapter.rs` | 136 | ArrowAdapter 三个方法 | ✅ `ts_for_date(2026,4,17)` → `now` |
| `snapshot.rs` | 74 | 一致性语义、read_range 实现 | 无 |

### 批次 2: tsdb-arrow (5 文件)

| 文件 | 行数 | 注释重点 | 硬编码日期修复 |
|------|------|---------|--------------|
| `converter.rs` | 437 | DataPoint↔RecordBatch 转换、tags_hash 计算、列构建逻辑 | 无 |
| `schema.rs` | 229 | tsdb_schema/compact_tsdb_schema、TsdbSchemaBuilder | 无 |
| `memory.rs` | 90 | TsdbMemoryPool CAS 分配/释放 | 无 |
| `error.rs` | 21 | TsdbArrowError 各变体 | 无 |
| `lib.rs` | 9 | crate 级文档 | 无 |

### 批次 3: tsdb-parquet (8 文件)

| 文件 | 行数 | 注释重点 | 硬编码日期修复 |
|------|------|---------|--------------|
| `compaction.rs` | 207 | CompactionConfig、ParquetCompactor 合并流程 | ✅ `from_ymd_opt(2026,4,17)` → `Utc::now().date_naive()` |
| `encoding.rs` | 84 | hot/cold writer 属性配置 | 无 |
| `error.rs` | 33 | TsdbParquetError 各变体 | 无 |
| `lib.rs` | 16 | crate 级文档 | 无 |
| `partition.rs` | 326 | PartitionManager、日期分区逻辑、micros_to_date | ✅ `1_704_067_200_000_000i64` → 动态计算 |
| `reader.rs` | 238 | TsdbParquetReader、范围读取 | 无 |
| `wal.rs` | 265 | WAL 条目类型、编码格式、恢复流程 | 无 |
| `writer.rs` | 268 | TsdbParquetWriter、WriteBufferConfig | 无 |

### 批次 4: tsdb-storage-arrow (5 文件)

| 文件 | 行数 | 注释重点 | 硬编码日期修复 |
|------|------|---------|--------------|
| `buffer.rs` | 225 | WriteBuffer/AsyncWriteBuffer 缓冲机制 | 无 |
| `config.rs` | 30 | ArrowStorageConfig 各字段 | 无 |
| `engine.rs` | 303 | ArrowStorageEngine 组合所有组件 | 无 |
| `error.rs` | 24 | TsdbStorageArrowError 各变体 | 无 |
| `lib.rs` | 9 | crate 级文档 | 无 |

### 批次 5: tsdb-datafusion (6 文件)

| 文件 | 行数 | 注释重点 | 硬编码日期修复 |
|------|------|---------|--------------|
| `engine.rs` | 246 | DataFusionQueryEngine、SQL 执行流程 | 无 |
| `error.rs` | 24 | TsdbDatafusionError 各变体 | 无 |
| `lib.rs` | 9 | crate 级文档 | 无 |
| `schema.rs` | 79 | 时序 Schema 映射 | 无 |
| `table_provider.rs` | 173 | TsdbTableProvider、Scan 实现 | 无 |
| `udf.rs` | 184 | 时序 UDF (time_bucket, rate, delta) | 无 |

### 批次 6: tsdb-cli + tsdb-test-utils + tsdb-bench (9 文件)

| 文件 | 行数 | 注释重点 | 硬编码日期修复 |
|------|------|---------|--------------|
| `tsdb-cli/main.rs` | 202 | 子命令用法、bench 模式 | 无 |
| `tsdb-test-utils/data_gen.rs` | 99 | 数据生成器 | ✅ `1_700_000_000_000_000i64` → `Utc::now().timestamp_micros()` |
| `tsdb-test-utils/assertions.rs` | 60 | 断言工具 | 无 |
| `tsdb-test-utils/lib.rs` | 4 | crate 级文档 | 无 |
| `tsdb-bench/convert_bench.rs` | 48 | 转换基准测试 | 无 |
| `tsdb-bench/read_bench.rs` | 30 | 读取基准测试 | 无 |
| `tsdb-bench/sql_bench.rs` | 35 | SQL 基准测试 | 无 |
| `tsdb-bench/write_bench.rs` | 40 | 写入基准测试 | 无 |

### 批次 7: tsdb-integration-tests + tsdb-stress + tsdb-stress-rocksdb (14 文件)

| 文件 | 行数 | 注释重点 | 硬编码日期修复 |
|------|------|---------|--------------|
| `integration-tests/end_to_end.rs` | 211 | 端到端测试场景 | ✅ `1_700_000_000_000_000i64` → 动态 |
| `integration-tests/rocksdb_e2e.rs` | 257 | RocksDB 端到端测试 | ✅ `ts_for_date(2026,4,17)` → `now`, `ts_for_date(2026,1,1)` → `now - 90天` |
| `integration-tests/lib.rs` | 2 | crate 级文档 | 无 |
| `stress/compaction_stress.rs` | 69 | Compaction 压力测试 | 无 |
| `stress/concurrent_stress.rs` | 62 | 并发压力测试 | 无 |
| `stress/read_stress.rs` | 39 | 读取压力测试 | 无 |
| `stress/wal_stress.rs` | 40 | WAL 压力测试 | 无 |
| `stress/write_stress.rs` | 33 | 写入压力测试 | 无 |
| `stress/lib.rs` | 5 | crate 级文档 | 无 |
| `stress-rocksdb/concurrent_stress.rs` | 140 | 并发压力测试 | ✅ `ts_for_date(2026,4,17)` → `now` |
| `stress-rocksdb/read_stress.rs` | 126 | 读取压力测试 | ✅ `ts_for_date(2026,4,17)` → `now`, `ts_for_date(2026,4,11)` → `now - 6天` |
| `stress-rocksdb/recovery_stress.rs` | 97 | 恢复压力测试 | ✅ `ts_for_date(2026,4,17)` → `now` |
| `stress-rocksdb/ttl_stress.rs` | 90 | TTL 压力测试 | ✅ `ts_for_date(2026,3,1)` → `now - 30天`, `ts_for_date(2026,3,15)` → `now - 15天` |
| `stress-rocksdb/write_stress.rs` | 115 | 写入压力测试 | ✅ `ts_for_date(2026,4,17)` → `now` |
| `stress-rocksdb/lib.rs` | 5 | crate 级文档 | 无 |

## 四、硬编码日期修复策略

### 原则

1. **测试代码中的硬编码日历日期** → 改为基于 `chrono::Utc::now()` 的动态日期
2. **测试代码中的硬编码时间戳** → 改为基于当前时间的动态计算
3. **生产代码中的时长常量** (如 `7 * 24 * 3600`) → **不修改**，这些是合理的默认时长
4. **测试中的参考 "now" 值** (如 compaction_filter 中的 `1_000_000_000_000_000i64`) → **不修改**，这是测试的固定参考点，测试逻辑是相对于此值计算的

### 具体替换规则

| 硬编码值 | 替换方式 | 适用场景 |
|---------|---------|---------|
| `ts_for_date(2026, 4, 17)` | `ts_for_date_today()` = `chrono::Utc::now().date_naive()` | 表示"今天"的测试数据 |
| `ts_for_date(2026, 4, 10)` | `today - chrono::Duration::days(7)` | 表示"7天前"的测试数据 |
| `ts_for_date(2026, 4, 11)` | `today - chrono::Duration::days(6)` | 表示"6天前"的测试数据 |
| `ts_for_date(2026, 1, 1)` | `today - chrono::Duration::days(90)` | 表示"90天前"的过期数据 |
| `ts_for_date(2026, 3, 1)` | `today - chrono::Duration::days(30)` | 表示"30天前"的过期数据 |
| `ts_for_date(2026, 3, 15)` | `today - chrono::Duration::days(15)` | 表示"15天前"的数据 |
| `1_700_000_000_000_000i64` | `chrono::Utc::now().timestamp_micros()` | 基准时间戳 |
| `1_704_067_200_000_000i64` | 动态计算当天零点的微秒时间戳 | partition 测试 |
| `NaiveDate::from_ymd_opt(2026,4,17)` | `chrono::Utc::now().date_naive()` | compaction 测试 |

### 辅助函数

在各测试模块中统一添加以下辅助函数：

```rust
fn today_date() -> chrono::NaiveDate {
    chrono::Utc::now().date_naive()
}

fn ts_for_today() -> i64 {
    today_date()
        .and_hms_opt(12, 0, 0).unwrap()
        .and_utc()
        .timestamp_micros()
}
```

已有的 `ts_for_date` 函数保留，仅将硬编码日期参数改为动态计算。

## 五、注释规范

```
/// 模块/结构体/函数的中文文档注释
///
/// 详细说明（可选）
///
/// # 参数
/// - `param` - 参数说明
///
/// # 返回
/// 返回值说明
///
/// # 示例
/// ```ignore
/// 示例代码
/// ```
```

- 所有 `pub fn` / `pub struct` / `pub enum` / `pub const` 添加 `///` 文档注释
- 关键编解码格式添加二进制布局图注释
- 关键逻辑添加 `//` 行内注释
- 不添加多余注释，保持代码简洁

## 六、验证

每个批次完成后运行：
```bash
cargo test --workspace
cargo clippy --all-targets -- -D warnings
cargo doc --workspace --no-deps
```

## 七、执行顺序

批次1 → 批次2 → 批次3 → 批次4 → 批次5 → 批次6 → 批次7 → 最终验证
