# TSDB2 Parquet 存储优化 — 任务分解

## Phase 1: Schema 与写入优化（排序 + 索引）

### Task 1.1: compact schema 增加 tags_hash 列
- **文件**: `crates/tsdb-arrow/src/schema.rs`
- **内容**:
  - `compact_tsdb_schema()` 在 timestamp 之后增加 `tags_hash: UInt64 NOT NULL` 列
  - 更新 `TsdbSchemaBuilder` 支持 tags_hash 列
  - 确保与 RocksDB 的 `compute_tags_hash()` 算法一致
- **验证**: 单元测试验证 schema 列顺序

### Task 1.2: DataPoint 转换时计算 tags_hash
- **文件**: `crates/tsdb-arrow/src/converter.rs`
- **内容**:
  - `datapoints_to_record_batch()` 在转换时填充 tags_hash 列
  - `record_batch_to_datapoints()` 读取 tags_hash 列（可选，用于反向转换）
- **验证**: 单元测试验证 tags_hash 值与 RocksDB 一致

### Task 1.3: 写入前排序
- **文件**: `crates/tsdb-parquet/src/writer.rs`
- **内容**:
  - `flush_buffer()` 在写入前对 RecordBatch 按 (tags_hash, timestamp) 排序
  - 实现 `sort_record_batch()` 函数
- **验证**: 单元测试验证输出文件数据有序

### Task 1.4: 启用 Parquet Column Index + Offset Index
- **文件**: `crates/tsdb-parquet/src/encoding.rs`, `crates/tsdb-parquet/src/writer.rs`
- **内容**:
  - `WriterProperties` 配置:
    - `set_sorting_columns([(tags_hash, ASC), (timestamp, ASC)])`
    - `set_column_index_enabled(true)`
    - `set_offset_index_enabled(true)`
    - `set_statistics_enabled(EnabledStatistics::Page)`
  - 更新 `hot_encoding()`, `cold_encoding()`, `default_encoding()` 函数
- **验证**: 写入文件后检查 Parquet 元数据包含 column_index 和 offset_index

### Task 1.5: 统一目录布局
- **文件**: `crates/tsdb-parquet/src/partition.rs`, `crates/tsdb-admin/src/lifecycle_api.rs`
- **内容**:
  - 新目录结构: `{tier}/{measurement}/{YYYYMMDD}/{measurement}_{YYYYMMDD}_{seq:06}.parquet`
  - `PartitionManager` 支持新的目录结构
  - `lifecycle_api.rs` 导出时使用新目录结构
  - 保持对旧目录结构的读取兼容
- **验证**: 集成测试验证新目录结构写入和读取

### Task 1.6: RocksDB 导出时排序 + 索引
- **文件**: `crates/tsdb-rocksdb/src/archiver.rs`
- **内容**:
  - `export_cf_to_parquet()` 使用新 schema（含 tags_hash）
  - 导出前按 (tags_hash, timestamp) 排序
  - 启用 Column Index + Offset Index
  - 写入 sidecar 统计文件
- **验证**: 集成测试验证导出文件包含排序和索引

## Phase 2: 文件统计信息与剪枝

### Task 2.1: 文件统计信息提取
- **文件**: `crates/tsdb-parquet/src/file_stats.rs` (新增)
- **内容**:
  - `FileStats` 结构体定义
  - `ValueStats` 结构体定义
  - `extract_file_stats()` 从 Parquet 元数据提取统计信息
  - `write_stats_file()` / `read_stats_file()` 读写 sidecar 文件
- **验证**: 单元测试验证统计信息提取正确

### Task 2.2: 分区清单文件管理
- **文件**: `crates/tsdb-parquet/src/manifest.rs` (新增)
- **内容**:
  - `PartitionManifest` 结构体定义
  - `read_manifest()` / `write_manifest()` 读写 `_manifest.json`
  - `update_manifest()` 写入新文件后更新清单
  - `refresh_manifest()` 扫描目录重建清单
- **验证**: 单元测试验证清单文件读写

### Task 2.3: 文件级剪枝逻辑
- **文件**: `crates/tsdb-parquet/src/pruning.rs` (新增)
- **内容**:
  - `prune_files()` 基于时间范围和标签过滤剪枝
  - `prune_row_groups()` 基于行组统计信息剪枝
  - `prune_pages()` 基于 Column Index 剪枝（预留接口）
- **验证**: 单元测试验证剪枝逻辑正确

### Task 2.4: 写入后自动生成统计文件
- **文件**: `crates/tsdb-parquet/src/writer.rs`
- **内容**:
  - `flush_buffer()` 写入 Parquet 后调用 `extract_file_stats()`
  - 调用 `write_stats_file()` 生成 sidecar 文件
  - 调用 `update_manifest()` 更新分区清单
- **验证**: 集成测试验证 sidecar 文件和清单自动生成

## Phase 3: 读取优化（谓词下推 + 页索引利用）

### Task 3.1: Parquet 读取器支持行组剪枝
- **文件**: `crates/tsdb-parquet/src/reader.rs`
- **内容**:
  - `read_range_arrow()` 使用文件统计信息剪枝
  - `read_parquet_file()` 使用行组统计信息跳过不相关行组
  - `ParquetRecordBatchReaderBuilder::with_row_groups()` 集成
- **验证**: 性能测试验证行组剪枝效果

### Task 3.2: Parquet 读取器支持谓词下推
- **文件**: `crates/tsdb-parquet/src/reader.rs`
- **内容**:
  - 实现 `build_row_filter()` 构建标签过滤 RowFilter
  - `ParquetRecordBatchReaderBuilder::with_row_filter()` 集成
  - 支持列投影 `with_projection()`
- **验证**: 性能测试验证谓词下推效果

### Task 3.3: DataFusion 过滤表达式转换
- **文件**: `crates/tsdb-datafusion/src/predicate.rs` (新增)
- **内容**:
  - `extract_filters()` 从 DataFusion Expr 提取时间范围和标签过滤
  - 支持 `tag_{key} = 'value'` 形式的过滤
  - 支持 `timestamp BETWEEN` 形式的范围查询
  - 支持 AND 组合条件
- **验证**: 单元测试验证各种过滤表达式解析

### Task 3.4: TsdbTableProvider 集成剪枝
- **文件**: `crates/tsdb-datafusion/src/table_provider.rs`
- **内容**:
  - `scan()` 使用 `extract_filters()` 提取过滤条件
  - 使用 `prune_files()` 剪枝文件列表
  - 使用 `build_row_filter()` 构建谓词下推
  - 保持 MemTable 回退路径（兼容旧格式）
- **验证**: 集成测试验证查询性能提升

## Phase 4: DataFusion 原生 Parquet 集成（可选，Phase 3 完成后评估）

### Task 4.1: ObjectStore 适配
- **文件**: `crates/tsdb-datafusion/src/engine.rs`
- **内容**:
  - 注册 `LocalFileSystem` ObjectStore
  - 配置 `SessionContext` 使用 Parquet 文件格式
- **验证**: 集成测试验证 ObjectStore 读取

### Task 4.2: 替换 MemTable 为 ListingTable
- **文件**: `crates/tsdb-datafusion/src/table_provider.rs`
- **内容**:
  - 使用 `ListingTable` + `ParquetFormat` 替代 `MemTable`
  - 配置 `ParquetFormat` 启用谓词下推和统计信息
  - 配置文件路径模式匹配新目录结构
- **验证**: 端到端测试验证 SQL 查询正确性

### Task 4.3: SqlApi 优化
- **文件**: `crates/tsdb-admin/src/sql_api.rs`
- **内容**:
  - 移除手动 RocksDB + Parquet 合并逻辑
  - 使用 DataFusion 原生查询引擎
  - 保留去重逻辑（RocksDB 热数据优先）
- **验证**: 端到端测试验证 SQL 查询结果正确

## Phase 5: Compaction 与迁移

### Task 5.1: Compaction 保持排序 + 生成索引
- **文件**: `crates/tsdb-parquet/src/compaction.rs`
- **内容**:
  - compaction 输出文件保持 (tags_hash, timestamp) 排序
  - 启用 Column Index + Offset Index
  - 生成 sidecar 统计文件
  - 更新分区清单
- **验证**: 集成测试验证 compaction 输出格式

### Task 5.2: 旧文件迁移工具
- **文件**: `crates/tsdb-parquet/src/migration.rs` (新增)
- **内容**:
  - `migrate_parquet_file()` 读取旧格式文件，排序后写入新格式
  - `migrate_partition()` 批量迁移一个日期分区
  - 添加 `make parquet-upgrade` Makefile target
- **验证**: 迁移后查询结果与迁移前一致

### Task 5.3: Makefile 与 CI 更新
- **文件**: `Makefile`, `.github/workflows/ci.yml`
- **内容**:
  - 添加 `parquet-upgrade` target
  - 添加 `parquet-stats` target（查看文件统计信息）
  - CI 中添加 Parquet 格式验证步骤
- **验证**: CI 通过
