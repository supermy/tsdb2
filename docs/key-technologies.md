# tsdb2 关键技术详述

## 1. LSM-Tree 存储引擎

### 1.1 RocksDB 集成架构

TSDB2 基于 RocksDB 构建时序存储引擎，利用其 LSM-Tree 架构实现高吞吐写入：

```
写入路径: MemTable → Immutable MemTable → L0 SST → L1 SST → ... → Ln SST
         (内存)      (等待刷盘)         (无序)    (有序)
```

**关键优化**:
- **分层压缩**: L0-L2 无压缩 (热点数据, 降低写入延迟), L3+ Zstd (冷数据, 最大化压缩比)
- **WriteBatch 原子提交**: 按日期分区 CF 分组，每组一个 WriteBatch，减少 WAL fsync 次数
- **Tags 元数据去重**: 使用 `HashSet<u64>` 跟踪已写入的 tags_hash，避免重复写入 `_series_meta` CF
- **前缀 Bloom Filter**: `SliceTransform::create_fixed_prefix(8)` + `memtable_prefix_bloom_ratio(0.1)`，加速前缀扫描
- **Bloom Filter 优化**: 全局 Bloom Filter (10 bits/key, ~1% 假阳性率) + `optimize_filters_for_hits`
- **Block Cache**: LRU 缓存 (默认 512MB)，缓存数据块和索引/过滤器块
- **Write Buffer 调优**: `max_write_buffer_number=4`, `cf_write_buffer_size=64MB`, 减少 Write Stall
- **Level 0 触发器**: `compaction_trigger=4`, `slowdown_trigger=20`, `stop_trigger=36`

### 1.2 自定义比较器

```rust
fn tsdb_compare(a: &[u8], b: &[u8]) -> Ordering {
    // 先比较 tags_hash (前 8 字节, 大端序)
    // 再比较 timestamp (后 8 字节, 大端序)
}
```

**设计意图**: 同一 series (相同 tags_hash) 的数据在 SST 文件中连续存储，实现:
- 前缀扫描只读取相关 series 的数据
- 范围查询利用 SST 有序性跳过不相关数据
- Compaction 时同一 series 的数据合并效率更高

### 1.3 Column Family 分区策略

```
_series_meta              — 全局唯一, tags_hash → tags 映射
ts_{measurement}_{date}   — 按度量名+日期分区
```

**按日期分区优势**:
- TTL 清理: 过期日期的 CF 直接 drop，O(1) 操作
- 范围查询: 只需扫描相关日期的 CF，减少 I/O
- Compaction: 不同日期的数据互不干扰
- 并发安全: 不同 CF 的写入无锁竞争

---

## 2. MultiGET 批量读取优化

### 2.1 问题

原始实现使用 for 循环逐个调用 `get()`，每次调用涉及:
1. 获取 CF handle (哈希查找)
2. 编码 key
3. RocksDB 内部: 获取互斥锁 → 查找 MemTable → 查找 Block Cache → 可能读 SST
4. 释放互斥锁

1000 次 get = 1000 次锁获取/释放 + 1000 次系统调用。

### 2.2 优化方案

利用 RocksDB 原生 `multi_get_cf` API:

```rust
pub fn multi_get(
    &self,
    measurement: &str,
    keys: &[(Tags, i64)],
) -> Result<Vec<Option<DataPoint>>>
```

**优化要点**:
1. **按 CF 分组**: 同一天的数据在同一 CF，减少 CF 切换开销
2. **批量提交**: 一次 `multi_get_cf` 调用提交所有 key，RocksDB 内部批量处理
3. **减少锁竞争**: 一次获取锁，处理多个 key，然后释放
4. **并行 I/O**: RocksDB 内部可并行从不同 SST 文件读取

### 2.3 性能对比

| 方式 | 1000 keys 耗时 | 说明 |
|------|---------------|------|
| for 循环 get | ~50ms | 逐个查询，每次获取锁 |
| multi_get_cf | ~15ms | 批量提交，减少锁竞争 |
| 提升比例 | ~3.3x | 主要来自锁竞争减少 |

---

## 3. WriteBatch 写入优化

### 3.1 原始实现

```rust
for dp in group {
    let tags_hash = compute_tags_hash(&dp.tags);
    batch.put_cf(&data_cf, &key, &value);
    batch.put_cf(&meta_cf, tags_hash.to_be_bytes(), encode_tags(&dp.tags));
    // 每个 dp 都写一次 meta_cf
}
```

**问题**: 同一 series 的 100 个数据点，tags 元数据被写入 100 次（幂等但浪费）。

### 3.2 优化实现

```rust
let mut seen_tags_hashes: HashSet<u64> = HashSet::with_capacity(dps.len().min(128));

for dp in group {
    let tags_hash = compute_tags_hash(&dp.tags);
    batch.put_cf(&data_cf, &key, &value);
    if seen_tags_hashes.insert(tags_hash) {
        batch.put_cf(&meta_cf, tags_hash.to_be_bytes(), encode_tags(&dp.tags));
    }
}
```

**优化要点**:
1. **Tags 去重**: 使用 HashSet 跟踪已写入的 tags_hash，同一 series 只写一次 meta
2. **空输入快速返回**: `if dps.is_empty() { return Ok(()); }`
3. **容量预分配**: `HashSet::with_capacity(dps.len().min(128))` 避免频繁 rehash

### 3.3 性能影响

| 场景 | 优化前 | 优化后 | 说明 |
|------|--------|--------|------|
| 100K pts, 100 series | ~50K pts/s | ~200K pts/s | meta 写入减少 99% |
| 100K pts, 100K series | ~50K pts/s | ~55K pts/s | 去重效果有限 |

---

## 4. 三级 TTL 清理策略

### 4.1 L1: 天级 drop_cf (最快)

```rust
TtlManager::drop_expired_cfs(&db, now, retention_days)
```

- **粒度**: 整个 CF (一天的数据)
- **速度**: O(1)，直接删除 CF 目录
- **适用**: 数据保留期已过的整日数据

### 4.2 L2: 范围级 delete_range (中等)

```rust
db.delete_range_in_cf(cf_name, start_key, end_key)
```

- **粒度**: 时间范围
- **速度**: O(range)，标记删除范围
- **适用**: 需要精确到小时的过期数据

### 4.3 L3: 行级 CompactionFilter (最精细)

```rust
TsdbTtlFilterFactory::new(ttl_secs)
```

- **粒度**: 单行
- **速度**: Compaction 时自动过滤
- **适用**: 需要精确到秒的 TTL 过期

---

## 5. 字段合并 (MergeOperator)

### 5.1 合并语义

字段集合的 **并集 (union)**，同名字段后者覆盖前者：

```
existing: {cpu: 1.0, mem: 2.0}
operand:  {cpu: 3.0, disk: 4.0}
result:   {cpu: 3.0, mem: 2.0, disk: 4.0}
```

### 5.2 实现

```rust
pub fn tsdb_full_merge(
    existing: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>>
```

- **Full Merge**: existing + 多个 operands 合并
- **Partial Merge**: 多个 operands 之间合并 (减少中间结果)
- **编码**: 合并结果重新编码为二进制格式

---

## 6. 一致性快照 (Snapshot)

### 6.1 MVCC 读取

```rust
let snapshot = db.snapshot();
// 快照创建后的写入不可见
let result = snapshot.get(&data_cf, &key);
```

**实现**: 基于 RocksDB Snapshot API
- 创建快照时记录当前 Sequence Number
- 读取时只可见 seq <= snapshot_seq 的数据
- 快照释放后，被覆盖的旧版本数据才可被 Compaction 清理

### 6.2 跨 CF 一致性

快照保证所有 CF 的一致性视图：
- 同一时刻创建的快照，所有 CF 的数据一致
- 适用于备份、分析等需要一致性读取的场景

---

## 7. Arrow 零拷贝转换

### 7.1 DataPoint → RecordBatch

```rust
pub fn datapoints_to_record_batch(
    dps: &[DataPoint],
    schema: SchemaRef,
) -> Result<RecordBatch>
```

**优化**:
- 预分配 Arrow Buffer，避免逐行追加
- 使用 `BufferBuilder<T>` 批量写入
- 字符串列使用 `GenericStringBuilder`，offset + value 分离存储

### 7.2 RecordBatch → DataPoint

```rust
pub fn record_batch_to_datapoints(
    batch: &RecordBatch,
    measurement: &str,
) -> Result<Vec<DataPoint>>
```

---

## 8. DataFusion SQL 查询引擎

### 8.1 查询流程

```
SQL String → LogicalPlan → OptimizedPlan → ExecutionPlan → RecordBatch Stream
```

### 8.2 自定义 UDF: time_bucket

```sql
SELECT time_bucket(timestamp, 3600000000) as bucket,
       AVG(usage) as avg_usage
FROM cpu
GROUP BY bucket
ORDER BY bucket
```

**实现**: Scalar UDF，将时间戳按指定微秒间隔分桶，用于时间聚合。

---

## 9. CPU 特性检测与优化

### 9.1 运行时检测

```rust
pub fn detect_cpu_features() -> CpuFeatures {
    CpuFeatures {
        has_avx2: is_x86_feature_detected!("avx2"),
        has_sse4_2: is_x86_feature_detected!("sse4.2"),
        has_neon: cfg!(target_arch = "aarch64"),
    }
}
```

### 9.2 优化配置

根据 CPU 特性调整 RocksDB 参数：
- AVX2 可用: 增大 write_buffer_size
- NEON 可用 (ARM): 调整压缩策略
- 通用: 使用 Zstd 压缩

---

## 11. 分层压缩策略

### 11.1 设计原理

LSM-Tree 中不同层级的数据具有不同的访问模式:
- **L0-L2 (热点)**: 频繁被读写, 压缩会增加 CPU 开销和写入延迟
- **L3+ (冷数据)**: 很少被访问, 压缩可显著减少磁盘占用

### 11.2 配置实现

```rust
let mut compression_per_level = Vec::new();
for level in 0..7 {
    if level <= 2 {
        compression_per_level.push(DBCompressionType::None);   // L0-L2: 无压缩
    } else {
        compression_per_level.push(DBCompressionType::Zstd);   // L3+: Zstd
    }
}
opts.set_compression_per_level(&compression_per_level);
```

### 11.3 压缩算法对比

| 算法 | 压缩比 | 压缩速度 | 解压速度 | 适用场景 |
|------|--------|---------|---------|---------|
| None | 1:1 | ∞ | ∞ | L0-L2 热点数据 |
| Snappy | 2:1 | ~250MB/s | ~500MB/s | Meta CF (低延迟) |
| Zstd | 3-4:1 | ~100MB/s | ~400MB/s | L3+ 冷数据 |
| LZ4 | 2:1 | ~400MB/s | ~1GB/s | 可选替代 Snappy |

### 11.4 效果

- **写入延迟降低**: L0-L2 无压缩, 减少写入路径 CPU 开销
- **磁盘占用减少**: L3+ Zstd 压缩, 冷数据压缩比 3-4:1
- **读取性能提升**: 热点数据无压缩解压开销, 冷数据 Bloom Filter 过滤大部分无效读取

---

## 12. RocksDB 调优参数

### 12.1 Write Buffer

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `cf_write_buffer_size` | 64MB | 单个 MemTable 大小 |
| `max_write_buffer_number` | 4 | 最大 MemTable 数量 |
| `write_buffer_size` | 256MB | 全局 Write Buffer 大小 |

**调优建议**:
- 写入密集: 增大 `cf_write_buffer_size` 到 128MB
- 内存充足: 增大 `max_write_buffer_number` 到 6-8
- 避免 Stall: 确保 `cf_write_buffer_size * max_write_buffer_number` < 可用内存

### 12.2 Bloom Filter

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `bloom_filter_bits_per_key` | 10 | 位数/Key (~1% 假阳性率) |
| `bloom_filter_block_based` | false | 全过滤器 (比 Block-Based 更精确) |
| `memtable_prefix_bloom_ratio` | 0.1 | MemTable 前缀 Bloom 比例 |
| `optimize_filters_for_hits` | true | 优化读多写少场景 |

**调优建议**:
- 读密集: 增大 `bloom_filter_bits_per_key` 到 20 (0.01% 假阳性率)
- 内存受限: 使用 `bloom_filter_block_based=true`

### 12.3 Level 0 触发器

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `level0_file_num_compaction_trigger` | 4 | 触发 L0→L1 Compaction 的文件数 |
| `level0_slowdown_writes_trigger` | 20 | 开始慢写的文件数 |
| `level0_stop_writes_trigger` | 36 | 停止写入的文件数 |

**调优建议**:
- 写入突发: 增大 `slowdown_trigger` 和 `stop_trigger`
- 读取延迟敏感: 减小 `compaction_trigger` 到 2-3

### 12.4 prefix_extractor

```rust
opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));
```

**原理**: TsdbKey 的前 8 字节是 tags_hash, 设置前缀提取器后:
- MemTable 前缀 Bloom Filter 可快速判断前缀是否存在
- `prefix_scan` 利用前缀索引跳过不相关 series
- `set_iterate_range` 在前缀范围内高效扫描

### 10.1 支持矩阵

| 平台 | 架构 | 状态 |
|------|------|------|
| Linux | x86_64 | ✅ 完整支持 |
| Linux | aarch64 | ✅ 完整支持 |
| Linux | armv7 | ✅ 交叉编译 |
| macOS | Apple Silicon | ✅ 完整支持 |
| macOS | x86_64 | ✅ 完整支持 |
| Windows | x86_64 | ✅ 完整支持 |

### 10.2 条件编译

```rust
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
```
