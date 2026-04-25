# tsdb2 测试报告

> 生成时间: 2026-04-18
> 测试环境: Apple M-series / 16GB RAM / Release 模式

## 1. 测试概览

| 层级 | 数量 | 状态 |
|------|------|------|
| 单元测试 | 111 | ✅ 全部通过 |
| 属性测试 (proptest) | 11 | ✅ 全部通过 |
| 集成测试 | 22 | ✅ 全部通过 |
| CLI 测试 | 7 | ✅ 全部通过 |
| **总计** | **151** | **✅ 全部通过** |

## 2. 单元测试明细

### 2.1 tsdb-arrow (8 tests)

| 测试名 | 说明 |
|--------|------|
| test_datapoints_to_record_batch_compact | Compact Schema 转换 |
| test_datapoints_to_record_batch_extended | Extended Schema 转换 |
| test_empty_datapoints | 空数据点处理 |
| test_null_fields | NULL 字段处理 |
| test_roundtrip_compact | Compact Schema 往返验证 |
| test_memory_pool_basic | 内存池基本操作 |
| test_memory_pool_limit | 内存池容量限制 |
| test_memory_pool_concurrent | 内存池并发安全 |

### 2.2 tsdb-rocksdb (111 tests)

**核心模块**:

| 模块 | 测试数 | 关键测试 |
|------|--------|---------|
| db | 21 | put/get, write_batch, multi_get, prefix_scan, snapshot |
| key | 10 + 4 proptest | 编解码, 排序不变量, 前缀编码 |
| value | 8 + 3 proptest | 编解码, 类型处理, 截断错误 |
| merge | 11 | Full/Partial Merge, 字段覆盖, 多操作数 |
| comparator | 8 | 自反性, 反对称性, 传递性 |
| compaction_filter | 7 | TTL 过滤, 边界条件 |
| snapshot | 5 | 一致性, 跨 CF 读取, 写入不可见 |
| tags | 6 | 编解码, Unicode, 截断错误 |
| config | 7 | 默认值, 自定义值, 正数约束 |
| error | 9 | 错误链, From 转换 |
| cpu_features | 3 | 检测, Debug 输出, 优化配置 |
| cleanup | 2 | drop_expired_cfs |
| arrow_adapter | 3 | RecordBatch 适配, 前缀扫描 |

**新增 MultiGET 测试**:

| 测试名 | 说明 |
|--------|------|
| test_multi_get_basic | 基本多键查询 |
| test_multi_get_missing_keys | 缺失键返回 None |
| test_multi_get_empty_keys | 空键列表 |
| test_multi_get_nonexistent_cf | 不存在的 CF |
| test_multi_get_consistency_with_get | 与 get() 结果一致性 |

**新增 WriteBatch 优化测试**:

| 测试名 | 说明 |
|--------|------|
| test_write_batch_empty | 空输入快速返回 |
| test_write_batch_tags_deduplication | Tags 元数据去重验证 |

### 2.3 tsdb-parquet (21 tests)

| 模块 | 测试数 |
|------|--------|
| writer | 3 |
| reader | 4 |
| compaction | 3 |
| wal | 5 |
| partition | 6 |

### 2.4 tsdb-datafusion (13 tests)

| 模块 | 测试数 |
|------|--------|
| engine | 6 |
| table_provider | 3 |
| schema | 2 |
| udf | 2 |

### 2.5 tsdb-storage-arrow (7 tests)

| 模块 | 测试数 |
|------|--------|
| buffer | 3 |
| engine | 4 |

## 3. CLI 全流程测试

### 3.1 写入基准测试

```
$ tsdb-cli --data-dir /tmp/tsdb-perf-test bench --mode write --points 100000
Write Benchmark Results
=======================
Points:     100000
Workers:    1
Elapsed:    0.50s
Throughput: 200127 points/sec
```

### 3.2 读取基准测试

```
$ tsdb-cli --data-dir /tmp/tsdb-perf-test bench --mode read --points 100000
Read Benchmark Results
======================
Points:     198515
Elapsed:    1.13s
Throughput: 175458 points/sec
```

### 3.3 数据库状态

```
$ tsdb-cli --data-dir /tmp/tsdb-perf-test status
TSDB Status (RocksDB Engine)
============================
Data Dir:         /tmp/tsdb-perf-test
Column Families:  1
CF Name:          ts_cpu_20260419
```

### 3.4 SQL 查询

```
$ tsdb-cli --data-dir /tmp/tsdb-perf-test query --sql "SELECT COUNT(*) as cnt FROM cpu" --format json
[{"cnt": 31537}]

$ tsdb-cli --data-dir /tmp/tsdb-perf-test query --sql "SELECT tag_host, AVG(usage) FROM cpu GROUP BY tag_host LIMIT 3" --format csv
tag_host,avg_usage
host_0002,82.439940
host_0007,82.489940
host_0009,82.509940
```

### 3.5 多指标写入与查询

```
$ tsdb-cli --data-dir /tmp/tsdb-cli-test4 import --file multi_metric.json --format json
Imported 7 data points from multi_metric.json

$ tsdb-cli --data-dir /tmp/tsdb-cli-test4 query --sql "SELECT * FROM memory" --format json
[
  {"free_mb": 4096, "tag_host": "server01", "tag_region": "east", "timestamp": ..., "used_percent": 65.5},
  {"free_mb": 3072, "tag_host": "server02", "tag_region": "west", "timestamp": ..., "used_percent": 72.3},
  {"free_mb": 3840, "tag_host": "server01", "tag_region": "east", "timestamp": ..., "used_percent": 68.1}
]

$ tsdb-cli --data-dir /tmp/tsdb-cli-test4 query --sql "SELECT tag_host, AVG(used_percent) FROM memory GROUP BY tag_host" --format json
[
  {"avg_usage": 72.3, "tag_host": "server02"},
  {"avg_usage": 66.8, "tag_host": "server01"}
]
```

### 3.6 数据导出/导入

```
$ tsdb-cli --data-dir /tmp/tsdb-cli-test export --measurement cpu --output cpu_export.json --format json
Exported 200000 data points to cpu_export.json

$ tsdb-cli --data-dir /tmp/tsdb-cli-test export --measurement cpu --output cpu_export.csv --format csv
Exported 200000 data points to cpu_export.csv

$ tsdb-cli --data-dir /tmp/tsdb-cli-test3 import --file cpu_export.csv --format csv --batch-size 10000
Imported 200000 data points from cpu_export.csv

$ tsdb-cli --data-dir /tmp/tsdb-cli-test3 query --sql "SELECT COUNT(*) as cnt FROM cpu" --format json
[{"cnt": 200000}]
```

### 3.7 Compaction

```
$ tsdb-cli --data-dir /tmp/tsdb-perf-test compact
Compacting 1 column families...
  Compacted: ts_cpu_20260419
Done.

$ tsdb-cli --data-dir /tmp/tsdb-perf-test compact --cf ts_cpu_20260419
Compacted CF: ts_cpu_20260419
```

### 3.8 归档管理

```
$ tsdb-cli --data-dir /tmp/tsdb-cli-test archive create --older-than 30d --output-dir ./archive
No CFs older than 30 days found.

$ tsdb-cli --data-dir /tmp/tsdb-cli-test archive list --archive-dir ./archive
No archives found.
```

### 3.9 健康检查

```
$ tsdb-cli --data-dir /tmp/tsdb-perf-test doctor
TSDB Doctor
==========
[OK]   Data directory exists: /tmp/tsdb-perf-test
[OK]   Database opened successfully
[OK]   Column Families: 1
[OK]   _series_meta CF exists
[OK]   Compaction check passed

Doctor check complete. No critical issues found.
```

## 4. Bug 修复记录

### 4.1 CSV 导出 Bug

**问题**: `DataPoint` 的 `tags`/`fields` 是 `BTreeMap`，csv crate 无法序列化 map
**修复**: 手动构建 CSV 表头和行数据，tags 展开为 `tag_xxx` 列，fields 展开为普通列

### 4.2 CSV 导入 Bug

**问题**: csv crate 无法反序列化为 `BTreeMap`
**修复**: 手动解析 CSV 行，根据表头前缀 `tag_` 识别标签列，自动推断字段类型

### 4.3 FieldValue 反序列化 Bug

**问题**: 用户手写 JSON 使用普通值 (`65.5`)，但 serde 默认枚举格式为 `{"Float": 65.5}`
**修复**: 为 `FieldValue` 实现自定义 `Deserialize`，同时支持两种格式

## 5. 测试命令参考

```bash
# 单元 + 集成测试
cargo test --workspace --exclude tsdb-stress-rocksdb

# 压力测试
cargo test -p tsdb-stress-rocksdb

# Clippy 检查
cargo clippy --workspace -- -D warnings

# 格式检查
cargo fmt --all -- --check

# Release 构建
cargo build --release -p tsdb-cli
```
