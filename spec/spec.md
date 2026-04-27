# TSDB2 Parquet 存储优化规格

## 1. 概述

本规格定义了 TSDB2 Parquet 存储层的三大优化目标：
1. **时间分区 + 标签排序**：Parquet 文件按时间分区组织，文件内数据按 `(tags_hash, timestamp)` 排序
2. **范围查询剪枝**：利用文件级 min/max 统计信息，范围查询只读取边界文件
3. **自定义 Parquet 页索引**：为标签列生成 Column Index + Offset Index，加速标签过滤

## 2. 现状分析

### 2.1 当前问题

| 问题 | 影响 | 严重性 |
|------|------|--------|
| 写入时无排序，数据按到达顺序写入 | 范围查询无法跳过不相关行组 | P0 |
| 无 Parquet 页索引（Column Index / Offset Index） | 标签过滤需全量解码 | P0 |
| 无文件级 min/max 统计用于剪枝 | 范围查询读取所有文件 | P0 |
| 查询全量加载到 MemTable | 内存占用大，无法流式处理 | P1 |
| 无 Parquet 谓词下推 | 所有过滤在内存中完成 | P1 |
| 两种目录布局不统一 | 维护复杂，查询路径分裂 | P2 |

### 2.2 当前架构

```
写入路径:
  DataPoint → TsdbParquetWriter → 按日期缓冲 → ArrowWriter(无排序/无索引)

读取路径:
  SQL → SqlApi → TsdbTableProvider → TsdbParquetReader → 全量读取 → MemTable → 内存过滤
```

### 2.3 目标架构

```
写入路径:
  DataPoint → TsdbParquetWriter → 按日期缓冲 → 排序(tags_hash, timestamp)
    → ArrowWriter(sorting_columns + column_index + offset_index)
    → 文件元数据记录 min/max 统计

读取路径:
  SQL → SqlApi → TsdbTableProvider → ParquetExec(原生)
    → 文件级剪枝(min/max统计) → 行组级剪枝(Column Index) → 页级剪枝(Offset Index)
```

## 3. 详细设计

### 3.1 时间分区 + 标签排序

#### 3.1.1 分区目录结构

统一现有两种目录布局，采用以下结构：

```
{parquet_dir}/
  {tier}/                              # warm / cold / archive
    {measurement}/                     # 指标名称，如 cpu, memory
      {YYYYMMDD}/                      # 日期分区
        {measurement}_{YYYYMMDD}_{seq:06}.parquet
```

示例：
```
data_parquet/
  warm/
    cpu/
      20260427/
        cpu_20260427_000001.parquet    # 包含 min/max 统计
        cpu_20260427_000002.parquet
    memory/
      20260427/
        memory_20260427_000001.parquet
  cold/
    cpu/
      20260315/
        cpu_20260315_000001.parquet
```

#### 3.1.2 文件内排序

Parquet 文件内数据按 `(tags_hash, timestamp)` 排序：

```rust
// WriterProperties 配置
let sorting_columns = vec![
    SortingColumn {
        column_idx: tags_hash_column_idx,
        descending: false,
        nulls_first: false,
    },
    SortingColumn {
        column_idx: timestamp_column_idx,
        descending: false,
        nulls_first: false,
    },
];
```

#### 3.1.3 Schema 变更

在 compact schema 基础上增加 `tags_hash` 列用于排序：

```
timestamp:     Timestamp(Microsecond) NOT NULL    # 列 0
tags_hash:     UInt64 NOT NULL                    # 列 1 (新增，用于排序和索引)
tag_{key}:     Utf8 (nullable)                    # 列 2..N
{field_name}:  {DataType} (nullable)              # 列 N+1..
```

`tags_hash` 列值由 `compute_tags_hash()` 生成，与 RocksDB 中使用的哈希算法一致。

#### 3.1.4 写入流程

```
1. DataPoints 按日期分组缓冲
2. 缓冲区满或 flush 时：
   a. 计算 tags_hash
   b. 转换为 RecordBatch
   c. 按 (tags_hash, timestamp) 排序
   d. 配置 WriterProperties:
      - set_sorting_columns([(tags_hash, ASC), (timestamp, ASC)])
      - set_column_index_enabled(true)     # 启用列索引
      - set_offset_index_enabled(true)     # 启用偏移索引
      - set_statistics_enabled(EnabledStatistics::Page)  # 页级统计
   e. 写入 Parquet 文件
   f. 读取文件元数据，提取各列 min/max 统计
   g. 将统计信息写入 sidecar metadata 文件
```

### 3.2 范围查询剪枝

#### 3.2.1 文件级统计信息

每个 Parquet 文件写入后，提取行组级统计信息并保存到 sidecar 文件：

```rust
struct FileStats {
    file_path: String,
    measurement: String,
    date: String,
    tier: String,
    row_count: u64,
    size_bytes: u64,
    timestamp_min: i64,           // 微秒
    timestamp_max: i64,           // 微秒
    tags_hash_min: u64,
    tags_hash_max: u64,
    tag_values: HashMap<String, ValueStats>,  // 每个标签列的统计
}

struct ValueStats {
    min: Option<String>,
    max: Option<String>,
    null_count: u64,
    distinct_count: Option<u64>,  // 近似基数
}
```

Sidecar 文件路径：`{parquet_file}.stats.json`

#### 3.2.2 分区清单文件

每个日期分区维护一个 `_manifest.json` 文件，记录该分区所有 Parquet 文件的统计信息：

```json
{
  "measurement": "cpu",
  "date": "20260427",
  "tier": "warm",
  "files": [
    {
      "path": "cpu_20260427_000001.parquet",
      "row_count": 100000,
      "timestamp_min": 1742774400000000,
      "timestamp_max": 1742860799999000,
      "tags_hash_min": 1234567890,
      "tags_hash_max": 9876543210,
      "tag_host": { "min": "server01", "max": "server99", "null_count": 0 },
      "tag_region": { "min": "east", "max": "west", "null_count": 0 }
    }
  ]
}
```

#### 3.2.3 查询剪枝逻辑

```rust
fn prune_files(
    files: &[FileStats],
    time_range: Option<(i64, i64)>,
    tag_filters: &HashMap<String, String>,
) -> Vec<FileStats> {
    files.iter().filter(|f| {
        // 时间范围剪枝
        if let Some((start, end)) = time_range {
            if f.timestamp_max < start || f.timestamp_min > end {
                return false;
            }
        }
        // 标签值剪枝
        for (tag_key, tag_value) in tag_filters {
            if let Some(stats) = f.tag_values.get(tag_key) {
                if let (Some(min), Some(max)) = (&stats.min, &stats.max) {
                    if tag_value < min || tag_value > max {
                        return false;
                    }
                }
            }
        }
        true
    }).cloned().collect()
}
```

### 3.3 自定义 Parquet 页索引

#### 3.3.1 Column Index（列索引）

Parquet 格式原生支持 Column Index，存储每个数据页的统计信息：

```
ColumnIndex {
    null_pages: Vec<bool>,        // 该页是否全为 null
    min_values: Vec<Vec<u8>>,     // 每页最小值（编码后）
    max_values: Vec<Vec<u8>>,     // 每页最大值（编码后）
    boundary_order: BoundaryOrder, // 升序/降序/无序
    null_counts: Vec<i64>,        // 每页 null 计数
}
```

启用方式：
```rust
let props = WriterProperties::builder()
    .set_column_index_enabled(true)
    .set_offset_index_enabled(true)
    .set_statistics_enabled(EnabledStatistics::Page)
    .build();
```

#### 3.3.2 标签列索引优化

对标签列（`tag_*` 列）启用页级统计，使标签过滤可以跳过不相关的数据页：

```
Parquet 文件结构:
  Row Group 0:
    Column Chunk (tag_host):
      Page 0: min="server01", max="server10"  ← 查询 server50 可跳过
      Page 1: min="server40", max="server60"  ← 查询 server50 需读取
      Page 2: min="server70", max="server99"  ← 查询 server50 可跳过
    Column Chunk (timestamp):
      Page 0: min=08:00, max=10:00            ← 时间范围剪枝
      Page 1: min=10:00, max=14:00
      Page 2: min=14:00, max=18:00
```

#### 3.3.3 读取端利用页索引

```rust
fn read_with_page_index(
    file_path: &Path,
    time_range: (i64, i64),
    tag_filters: &HashMap<String, String>,
    projection: Option<&[String]>,
) -> Result<Vec<RecordBatch>> {
    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::new(file)?;

    // 1. 利用行组统计信息跳过整个行组
    let metadata = builder.metadata();
    let row_groups = filter_row_groups(metadata, time_range, tag_filters);

    // 2. 利用 Column Index 跳过数据页
    let row_filter = build_row_filter(tag_filters, metadata);

    // 3. 构建带谓词下推的读取器
    let reader = builder
        .with_row_groups(row_groups)
        .with_row_filter(row_filter)
        .with_projection(projection)
        .build()?;

    reader.collect()
}
```

#### 3.3.4 RowFilter 实现

```rust
fn build_row_filter(
    tag_filters: &HashMap<String, String>,
    schema: &Schema,
) -> Option<RowFilter> {
    let mut filters = Vec::new();
    for (tag_key, tag_value) in tag_filters {
        let col_name = format!("tag_{}", tag_key);
        if let Ok(idx) = schema.index_of(&col_name) {
            let filter = ArrowPredicateFn::new(
                ProjectionMask::from_indices(schema.clone(), [idx]),
                move |batch| {
                    let col = batch.column(0);
                    let string_col = col.as_any().downcast_ref::<StringArray>().unwrap();
                    Ok(Arc::new(BooleanArray::from_unary(string_col, |s| s == tag_value.as_str())) as _)
                },
            );
            filters.push(Arc::new(filter) as Arc<dyn ArrowPredicate>);
        }
    }
    if filters.is_empty() { None } else { Some(RowFilter::new(filters)) }
}
```

### 3.4 DataFusion 原生 Parquet 集成

#### 3.4.1 替换 MemTable 为 ParquetExec

当前 `TsdbTableProvider::scan()` 将所有数据加载到 `MemTable`，改为使用 DataFusion 原生 `ParquetExec`：

```rust
impl TableProvider for TsdbTableProvider {
    fn scan(&self, ...filters, projection) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. 从 filters 提取时间范围和标签过滤
        let (time_range, tag_filters) = extract_filters(&filters);

        // 2. 利用文件统计信息剪枝
        let pruned_files = prune_files(&self.file_stats, time_range, &tag_filters);

        // 3. 构建 ParquetExec 执行计划
        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store_url: ...,
                file_groups: partition_files_into_groups(pruned_files),
                statistics: ...,
                projection: ...,
                limit: ...,
                table_partition_cols: vec![],
            },
            Some(parquet_predicate),
            None,
        );

        Ok(Arc::new(parquet_exec))
    }
}
```

#### 3.4.2 ObjectStore 适配

为 DataFusion 提供本地文件系统的 `ObjectStore` 实现：

```rust
let object_store = LocalFileSystem::new();
let store_url = ObjectStoreUrl::local_filesystem();
ctx.register_object_store(&store_url.as_ref(), Arc::new(object_store));
```

### 3.5 兼容性设计

#### 3.5.1 向后兼容

- 旧格式 Parquet 文件（无排序、无索引）仍可读取
- 读取时检测文件是否包含 Column Index，有则利用，无则回退到全量读取
- 旧格式文件在下次 compaction 时自动升级为新格式

#### 3.5.2 迁移策略

- 新写入的文件自动使用新格式
- 提供 `make parquet-upgrade` 命令批量重写旧文件
- compaction 过程中自动应用排序和索引

## 4. 性能预期

| 场景 | 当前 | 优化后 | 提升 |
|------|------|--------|------|
| 时间范围查询（1天/30天数据） | 读取30天所有文件 | 只读取1天文件 | ~30x |
| 标签过滤查询（1个标签值/100个） | 全量解码 | 页级剪枝跳过99% | ~100x |
| 列投影查询（2列/20列） | 读取20列 | 只读2列 | ~10x |
| 大范围查询内存占用 | 全量加载 | 流式处理 | ~10x |

## 5. 关键实现细节

### 5.1 RecordBatch 排序实现

```rust
fn sort_batch_by_tags_hash_timestamp(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let tags_hash_idx = schema.index_of("tags_hash")
        .map_err(|_| TsdbParquetError::Conversion("tags_hash column not found".into()))?;
    let ts_idx = schema.index_of("timestamp")
        .map_err(|_| TsdbParquetError::Conversion("timestamp column not found".into()))?;

    let tags_hash_col = batch.column(tags_hash_idx)
        .as_any().downcast_ref::<UInt64Array>().unwrap();
    let ts_col = batch.column(ts_idx)
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();

    let mut indices: Vec<usize> = (0..batch.num_rows()).collect();
    indices.sort_by(|&a, &b| {
        tags_hash_col.value(a).cmp(&tags_hash_col.value(b))
            .then(ts_col.value(a).cmp(&ts_col.value(b)))
    });

    let index_array = UInt32Array::from_iter(indices.iter().map(|&i| i as u32));
    let sorted_cols: Vec<Arc<dyn Array>> = schema.fields().iter().enumerate()
        .map(|(col_idx, _)| arrow::compute::take(batch.column(col_idx), &index_array, None).unwrap())
        .collect();

    Ok(RecordBatch::try_new(schema.clone(), sorted_cols)?)
}
```

### 5.2 SortingColumn 配置

```rust
use parquet::file::properties::SortingColumn;

fn build_sorting_columns(schema: &SchemaRef) -> Vec<SortingColumn> {
    let tags_hash_idx = schema.index_of("tags_hash").unwrap() as i32;
    let ts_idx = schema.index_of("timestamp").unwrap() as i32;
    vec![
        SortingColumn::new(tags_hash_idx, false, false), // desc=false, nulls_first=false
        SortingColumn::new(ts_idx, false, false),
    ]
}
```

### 5.3 行组统计信息提取

```rust
fn extract_row_group_stats(metadata: &ParquetMetaData) -> FileStats {
    let row_groups = metadata.row_groups();
    let mut ts_min = i64::MAX;
    let mut ts_max = i64::MIN;
    let mut th_min = u64::MAX;
    let mut th_max = u64::MIN;
    let mut total_rows = 0u64;

    for rg in row_groups {
        total_rows += rg.num_rows() as u64;
        for col in rg.columns() {
            if let Some(stats) = col.statistics() {
                // 解码 min/max 根据列类型
            }
        }
    }

    FileStats { timestamp_min: ts_min, timestamp_max: ts_max, tags_hash_min: th_min, tags_hash_max: th_max, row_count: total_rows, ... }
}
```

### 5.4 读取端行组剪枝

```rust
fn filter_row_groups(
    metadata: &ParquetMetaData,
    time_range: (i64, i64),
) -> Vec<usize> {
    let ts_col_idx = 0; // timestamp is always column 0
    metadata.row_groups().iter().enumerate()
        .filter(|(_, rg)| {
            if let Some(col) = rg.columns().get(ts_col_idx) {
                if let Some(stats) = col.statistics() {
                    // 检查时间范围是否重叠
                    let min_ts = decode_timestamp_min(stats);
                    let max_ts = decode_timestamp_max(stats);
                    return max_ts >= time_range.0 && min_ts <= time_range.1;
                }
            }
            true // 无统计信息时不跳过
        })
        .map(|(idx, _)| idx)
        .collect()
}
```

### 5.5 向后兼容读取

```rust
fn read_parquet_file_compat(path: &Path, ...) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    // 检查是否有 Column Index
    let has_column_index = builder.metadata()
        .row_groups().iter()
        .any(|rg| rg.columns().iter().any(|c| c.column_index().is_some()));

    if has_column_index {
        // 利用 Column Index 进行页级剪枝
        read_with_page_index(builder, ...)
    } else {
        // 回退到全量读取
        let reader = builder.build()?;
        reader.collect::<Result<Vec<_>, _>>()
    }
}
```

## 6. 涉及文件

### 修改文件

| 文件 | 变更内容 |
|------|----------|
| `crates/tsdb-arrow/src/schema.rs` | compact schema 增加 tags_hash 列 |
| `crates/tsdb-arrow/src/converter.rs` | DataPoint 转换时计算 tags_hash |
| `crates/tsdb-parquet/src/writer.rs` | 写入前排序 + 启用索引 + 写统计文件 |
| `crates/tsdb-parquet/src/reader.rs` | 利用页索引 + 行组剪枝 + 谓词下推 |
| `crates/tsdb-parquet/src/encoding.rs` | WriterProperties 配置更新 |
| `crates/tsdb-parquet/src/partition.rs` | 统一目录布局 + manifest 管理 |
| `crates/tsdb-parquet/src/compaction.rs` | compaction 时保持排序 + 生成索引 |
| `crates/tsdb-datafusion/src/table_provider.rs` | 替换 MemTable 为 ParquetExec |
| `crates/tsdb-datafusion/src/engine.rs` | 注册 ObjectStore |
| `crates/tsdb-admin/src/lifecycle_api.rs` | 导出时使用新格式 |
| `crates/tsdb-admin/src/sql_api.rs` | 利用文件剪枝优化查询 |
| `crates/tsdb-rocksdb/src/archiver.rs` | 导出时排序 + 索引 |

### 新增文件

| 文件 | 内容 |
|------|------|
| `crates/tsdb-parquet/src/file_stats.rs` | 文件统计信息提取与持久化 |
| `crates/tsdb-parquet/src/manifest.rs` | 分区清单文件管理 |
| `crates/tsdb-parquet/src/pruning.rs` | 文件/行组/页级剪枝逻辑 |
| `crates/tsdb-datafusion/src/predicate.rs` | DataFusion 过滤表达式转换 |
