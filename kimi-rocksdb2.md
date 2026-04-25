这是一个非常经典且高效的**实时分析混合架构（Hybrid Transactional/Analytical Processing, HTAP）**设计模式。让我为你详细解析这个架构的最佳实践，以及 DataPoint 共享模型如何作为关键粘合剂。

---

## 架构全景图

```
┌─────────────────────────────────────────────────────────────┐
│                      应用层 (Application)                     │
│                   实时查询 / 批量报表 / ML 推理                  │
└──────────────┬──────────────────────────────┬─────────────────┘
               │                              │
    ┌──────────▼──────────┐      ┌───────────▼────────────┐
    │   RocksDB (热数据)   │      │   Parquet (冷/温数据)   │
    │  • 实时写入 (<10ms)   │      │  • 批量分析 (高吞吐)     │
    │  • 点查/小范围扫描    │      │  • 列投影/谓词下推        │
    │  • 行存优化          │      │  • 高压缩比 (Snappy/Zstd) │
    │  • WAL + MemTable    │      │  • 分区/排序策略         │
    └──────────┬──────────┘      └───────────┬────────────┘
               │                              │
               └──────────────┬───────────────┘
                              │
                    ┌─────────▼─────────┐
                    │   DataPoint 模型    │
                    │  (统一数据抽象层)    │
                    │ • Schema 共享       │
                    │ • 版本兼容性        │
                    │ • 序列化/反序列化    │
                    └───────────────────┘
```

---

## 1. 职责分离与选型依据

### RocksDB：实时读写引擎

| 特性 | 实践配置 | 原理 |
|------|---------|------|
| **写入优化** | `unordered_write=1` + `allow_concurrent_memtable_write` | 最大化写入吞吐，牺牲部分读一致性 |
| **点查优化** | ` bloom_bits=10`, `block_size=16KB` | 减少 I/O 放大，快速过滤不存在键 |
| **范围扫描** | `prefix_extractor` + `memtable_prefix_bloom_size_ratio` | 针对时间序列前缀优化 |
| **列投影下推** | 自定义 `MergeOperator` 或 `CompactionFilter` | 在 Compaction 阶段过滤无效列版本 |

**关键配置示例**：
```cpp
// 针对 DataPoint 模型优化的 RocksDB 选项
options.create_if_missing = true;
options.write_buffer_size = 64 * 1024 * 1024;  // 64MB MemTable
options.max_write_buffer_number = 3;
options.target_file_size_base = 64 * 1024 * 1024;
options.level_compaction_dynamic_level_bytes = true;

// 列投影下推：在 Compaction 时裁剪历史版本
options.compaction_filter_factory = 
    std::make_shared<DataPointCompactionFilterFactory>();
```

### Parquet：批量分析引擎

| 特性 | 实践配置 | 原理 |
|------|---------|------|
| **列投影** | `row_group_size=128MB`, `page_size=1MB` | 与 RocksDB SST 文件大小对齐，减少跨格式开销 |
| **压缩策略** | 时间戳列: `DELTA_BINARY_PACKED` + `Zstd` | 时序数据高压缩比 |
| **排序策略** | 按 `(device_id, timestamp)` 排序 | 最大化列式压缩效果 |
| **谓词下推** | `min/max` 统计信息 + Bloom Filter | 跳过不满足条件的 Row Group |

---

## 2. DataPoint 共享模型设计

这是架构的核心——**统一数据抽象层**，确保两种存储格式的语义一致性。

### 模型定义（Protocol Buffers / FlatBuffers 示例）

```protobuf
// DataPoint 统一模型
message DataPoint {
  // 主键维度
  string device_id = 1;      // 实体标识
  int64 timestamp_ms = 2;    // 时间戳（毫秒级）
  
  // 指标维度（列投影下推的目标）
  map<string, MetricValue> metrics = 3;
  
  // 元数据（用于生命周期管理）
  DataQuality quality = 4;
  map<string, string> tags = 5;
  
  // 版本控制（解决 RocksDB→Parquet 迁移时的并发问题）
  int64 schema_version = 6;
  bytes checksum = 7;
}

message MetricValue {
  oneof value {
    double float64 = 1;
    int64 int64 = 2;
    string string = 3;
    bytes binary = 4;
  }
  // 列式存储的元信息
  ColumnEncoding encoding = 5;
}
```

### 共享模型的关键设计原则

1. **Schema 演进兼容性**
   - RocksDB 侧：使用 `schema_version` 字段，旧版本数据在读取时动态转换
   - Parquet 侧：利用 Parquet 原生 Schema 演化能力，新增列自动填充 NULL

2. **零拷贝序列化**
   - RocksDB Value 直接存储 FlatBuffers 二进制，避免解析开销
   - Parquet 写入时，FlatBuffers → Arrow RecordBatch → Parquet 零拷贝转换

3. **校验和一致性**
   - 跨格式校验：`checksum = hash(device_id + timestamp + sorted_metrics)`
   - 防止 RocksDB Compaction 或 Parquet 转储过程中的静默数据损坏

---

## 3. 列投影下推的协同实现

### RocksDB 侧的列投影

RocksDB 本身是行存，但通过以下技巧实现**逻辑列投影**：

```cpp
// 自定义 Compaction Filter，实现 TTL + 列裁剪
class DataPointCompactionFilter : public CompactionFilter {
public:
  bool Filter(int level, const Slice& key, const Slice& existing_value,
              std::string* new_value, bool* value_changed) const override {
    
    // 1. 解析 DataPoint（零拷贝 FlatBuffers）
    auto dp = flatbuffers::GetRoot<DataPoint>(existing_value.data());
    
    // 2. 检查列版本：如果某些列已被标记删除，裁剪之
    if (dp->schema_version() < current_schema_version_) {
      auto mutated = MutateDataPoint(dp, projected_columns_);
      *new_value = Serialize(mutated);
      *value_changed = true;
    }
    
    // 3. TTL 检查：冷数据标记为待迁移
    if (IsColdData(dp->timestamp_ms())) {
      return true; // 删除（后续由后台任务归档到 Parquet）
    }
    
    return false;
  }
};
```

### Parquet 侧的列投影

```cpp
// 使用 Arrow + Parquet C++ API
std::shared_ptr<arrow::Table> ReadProjected(
    const std::string& parquet_file,
    const std::vector<std::string>& column_projection,
    const arrow::compute::Expression& predicate) {
  
  parquet::ArrowReaderProperties properties;
  properties.set_pre_buffer(true);
  
  // 1. 列投影下推：只读取需要的列
  auto column_indices = ResolveColumnIndices(column_projection);
  
  // 2. 谓词下推：利用 Row Group 统计信息
  auto row_groups = FilterRowGroups(parquet_file, predicate);
  
  // 3. 构建读取器
  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(file, pool, properties, &reader));
  
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(
      reader->RowGroups(row_groups)->ReadTable(column_indices, &table));
  
  return table;
}
```

---

## 4. 数据流与生命周期管理

### 热→温→冷 迁移管道

```
RocksDB (热, 0-7天)        Parquet (温, 7-90天)       Parquet (冷, 90天+)
┌─────────────┐           ┌─────────────┐            ┌─────────────┐
│ 实时写入    │ ───┐      │ 按小时分区   │            │ 按天分区     │
│ 点查 <5ms   │    │      │ Zstd 压缩   │ ────────▶  │ Zstd 压缩   │
│ 最新值缓存  │    └────▶ │ 排序: (device, time)     │ 聚合: 小时级 │
└─────────────┘  后台Flush └─────────────┘   后台Compaction  └─────────────┘
```

**迁移触发条件**：
- **时间触发**：数据超过 `hot_retention_days`（如 7 天）
- **容量触发**：RocksDB 总大小超过阈值，优先转储最早的分区
- **查询模式触发**：某设备数据连续 24 小时无点查，标记为可迁移

### 原子性保证

使用 **Watermark机制** 确保迁移过程中查询一致性：

```python
# 伪代码：迁移协调器
class MigrationCoordinator:
    def archive_to_parquet(self, device_id, time_range):
        # 1. 设置逻辑水位线，阻止该范围的写入
        watermark = self.set_write_watermark(device_id, time_range.end)
        
        # 2. 扫描 RocksDB，转换为 Arrow RecordBatch
        batches = self.scan_rocksdb(device_id, time_range)
        
        # 3. 写入 Parquet（临时文件）
        temp_file = self.write_parquet(batches, schema_version=watermark.schema_version)
        
        # 4. 原子性替换：注册 Parquet 元数据，删除 RocksDB 键
        self.catalog.register_parquet(device_id, time_range, temp_file)
        self.delete_from_rocksdb(device_id, time_range)
        
        # 5. 释放水位线
        self.clear_watermark(watermark)
```

---

## 5. 查询路由层（智能下推）

```python
class HybridQueryRouter:
    def query(self, sql_or_api_request):
        # 1. 解析查询特征
        query_type = self.classify(request)  # POINT / RANGE / AGGREGATION
        
        # 2. 时间范围分析
        time_range = extract_time_range(request)
        
        # 3. 路由决策
        if query_type == "POINT" and time_range.is_recent(7d):
            # 点查 + 热数据 → RocksDB
            return self.rocksdb_engine.query(request)
            
        elif query_type == "AGGREGATION" and time_range.is_historical():
            # 聚合 + 冷数据 → Parquet（列投影 + 谓词下推）
            return self.parquet_engine.query(request)
            
        else:
            # 混合查询：RocksDB 查近期，Parquet 查历史，Union 合并
            hot_part = self.rocksdb_engine.query(hot_range)
            cold_part = self.parquet_engine.query(cold_range)
            return self.merge_results(hot_part, cold_part, union_strategy="timestamp")
```

---

## 6. 性能基准与调优 checklist

| 场景 | 目标延迟 | 关键优化 |
|------|---------|---------|
| RocksDB 点查 | < 5ms | MemTable + BlockCache 命中率 > 95% |
| RocksDB 范围扫描 (1k 行) | < 50ms | `prefix_same_as_start`, `total_order_seek=false` |
| Parquet 单列聚合 (1TB) | < 30s | `row_group_size` 匹配 HDFS block (128MB), 谓词下推 |
| 跨格式 Join | < 100ms | DataPoint 模型预对齐排序键，避免 Shuffle |

### 监控指标

- **RocksDB**：`rocksdb.number.deletes.stalled`, `rocksdb.memtable.hit`, `rocksdb.compaction.times`
- **Parquet**：`parquet.page_index.rows.skipped`, `parquet.bloom.filter.rows.skipped`
- **迁移管道**：`lag_between_watermark_and_archive`, `format_conversion.throughput_mbps`

---

## 7. 常见陷阱与规避

| 陷阱 | 规避方案 |
|------|---------|
| **Schema 漂移** | DataPoint 模型强制 `schema_version` 字段，RocksDB 侧使用 `ColumnFamily` 隔离不同版本 |
| **小文件问题** | Parquet 侧设置 `min_file_size=64MB`，RocksDB Flush 时按时间窗口聚合而非单条写入 |
| **跨格式事务** | 采用最终一致性 + 补偿机制，避免分布式事务；关键业务使用 `write_watermark` 阻塞 |
| **压缩算法冲突** | RocksDB 使用 `LZ4`（速度优先），Parquet 使用 `Zstd`（压缩比优先），DataPoint 序列化使用 `FlatBuffers`（零拷贝优先） |

---

## 总结

这个架构的精髓在于：**通过 DataPoint 共享模型实现语义统一，通过存储格式差异实现物理优化**。

- **RocksDB** 负责 **时间维度上的近期数据** 和 **访问模式上的点查/小范围扫描**
- **Parquet** 负责 **时间维度上的历史数据** 和 **访问模式上的批量分析/聚合**
- **DataPoint** 作为 **Schema 契约、序列化协议、版本控制、校验机制** 的载体，消除两种格式间的阻抗失配

如果你的场景涉及**高频写入 + 低频批量分析**，或者需要**实时指标与历史趋势的统一视图**，这个混合架构几乎是当前开源方案中的最优解。