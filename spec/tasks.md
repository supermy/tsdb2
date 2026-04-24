# TSDB2 Iceberg 模拟功能 — 实施任务清单

> 基于 spec/spec.md 规格，按优先级和依赖关系排列

---

## 任务总览

| Phase | 名称 | 任务数 | 优先级 | 依赖 |
|-------|------|--------|--------|------|
| P1 | Catalog + TableMetadata + Schema | 8 | P0 | 无 |
| P2 | Snapshot + Manifest + DataFile | 8 | P0 | P1 |
| P3 | Append 写入 + 列统计收集 | 7 | P0 | P2 |
| P4 | Scan + 谓词下推 (三级过滤) | 7 | P0 | P3 |
| P5 | IcebergTableProvider (DataFusion) | 6 | P0 | P4 |
| P6 | Time Travel 查询 | 5 | P1 | P5 |
| P7 | Compaction (Iceberg 风格) | 5 | P1 | P3 |
| P8 | Snapshot 过期清理 | 4 | P1 | P6 |
| P9 | Schema 演化 | 5 | P2 | P5 |
| P10 | 分区演化 | 4 | P2 | P9 |
| **合计** | | **59** | | |

---

## P1: Catalog + TableMetadata + Schema [P0]

### P1.1 创建 tsdb-iceberg crate 骨架

**文件**: `crates/tsdb-iceberg/`

- [ ] 创建 `Cargo.toml` (依赖: tsdb-arrow, tsdb-rocksdb, tsdb-parquet, rocksdb, serde, serde_json, uuid, thiserror, chrono, tracing)
- [ ] 创建 `src/lib.rs` (crate 入口 + 重导出)
- [ ] 创建 `src/error.rs` (IcebergError 枚举: Catalog, TableNotFound, CommitConflict, Schema, Manifest, Io, Json, Rocksdb)
- [ ] 在 workspace `Cargo.toml` 中添加 `[patch]` 或 `[workspace.dependencies]` 条目
- [ ] `cargo check` 编译通过

- **验收**: `cargo check -p tsdb-iceberg` 无错误

### P1.2 Schema 数据结构

**文件**: `crates/tsdb-iceberg/src/schema.rs`

- [ ] 定义 `Schema` 结构体 (schema_id, fields)
- [ ] 定义 `Field` 结构体 (id, name, required, field_type, doc, initial_default, write_default)
- [ ] 定义 `IcebergType` 枚举 (Boolean, Int, Long, Float, Double, Decimal, Date, Time, Timestamp, Timestamptz, String, Uuid, Binary, Struct, List, Map)
- [ ] 实现 `Schema::arrow_schema()` — Iceberg Schema → Arrow Schema 转换
- [ ] 实现 `Schema::field_by_id()` / `field_by_name()` 查找方法
- [ ] 实现 Serialize/Deserialize for Schema, Field, IcebergType
- [ ] 单元测试: `test_schema_serde_roundtrip`, `test_schema_to_arrow`, `test_field_lookup`

- **验收**: Schema 结构体完整，序列化/反序列化正确，Arrow 转换正确

### P1.3 PartitionSpec 数据结构

**文件**: `crates/tsdb-iceberg/src/partition.rs`

- [ ] 定义 `PartitionSpec` 结构体 (spec_id, fields)
- [ ] 定义 `PartitionField` 结构体 (source_id, field_id, name, transform)
- [ ] 定义 `Transform` 枚举 (Identity, Bucket, Truncate, Year, Month, Day, Hour, Void)
- [ ] 实现 `Transform::apply()` — 对值执行 Transform
- [ ] 实现 `PartitionSpec::partition_path()` — 生成分区路径字符串
- [ ] 单元测试: `test_transform_day`, `test_transform_identity`, `test_partition_path`

- **验收**: PartitionSpec 和 Transform 完整，分区路径生成正确

### P1.4 TableMetadata 数据结构

**文件**: `crates/tsdb-iceberg/src/catalog.rs` (部分)

- [ ] 定义 `TableMetadata` 结构体 (format_version, table_uuid, location, last_sequence_number, last_updated_ms, last_column_id, current_schema_id, schemas, default_spec_id, partition_specs, current_snapshot_id, snapshots, snapshot_log, properties)
- [ ] 定义 `SnapshotLogEntry` 结构体 (snapshot_id, timestamp_ms)
- [ ] 实现 Serialize/Deserialize for TableMetadata
- [ ] 实现 `TableMetadata::new()` — 创建空表元数据
- [ ] 单元测试: `test_table_metadata_new`, `test_table_metadata_serde_roundtrip`

- **验收**: TableMetadata 结构完整，序列化正确

### P1.5 IcebergCatalog 实现

**文件**: `crates/tsdb-iceberg/src/catalog.rs`

- [ ] 定义 `IcebergCatalog` 结构体 (db: rocksdb::DB, base_dir: PathBuf)
- [ ] 实现 RocksDB CF 创建: `_catalog`, `_table_meta`, `_snapshot`, `_manifest_list`, `_manifest`, `_refs`
- [ ] 实现 `IcebergCatalog::open(path)` — 打开/创建 Catalog
- [ ] 实现 `IcebergCatalog::create_table(name, schema, partition_spec)` — 创建表
- [ ] 实现 `IcebergCatalog::load_table(name)` — 加载表
- [ ] 实现 `IcebergCatalog::drop_table(name)` — 删除表
- [ ] 实现 `IcebergCatalog::list_tables()` — 列出所有表
- [ ] 实现 `IcebergCatalog::rename_table(old, new)` — 重命名表
- [ ] 单元测试: `test_catalog_create_load_table`, `test_catalog_list_tables`, `test_catalog_drop_table`, `test_catalog_rename_table`

- **验收**: Catalog CRUD 全部可用，RocksDB CF 正确创建

### P1.6 Catalog 内部辅助方法

**文件**: `crates/tsdb-iceberg/src/catalog.rs`

- [ ] 实现 `load_metadata(table_name)` — 从 `_table_meta` CF 读取 TableMetadata
- [ ] 实现 `save_metadata(table_name, metadata)` — 写入 `_table_meta` CF
- [ ] 实现 `next_sequence_number()` — 单调递增序列号
- [ ] 实现 Key 编码函数: `meta_key(table)`, `schema_key(table, id)`, `part_spec_key(table, id)`
- [ ] 单元测试: `test_key_encoding`, `test_sequence_number_monotonic`

- **验收**: 内部方法正确，Key 编码一致

### P1.7 DataPoint ↔ Iceberg Schema 映射

**文件**: `crates/tsdb-iceberg/src/schema.rs`

- [ ] 实现 `Schema::from_datapoint_schema(measurement, tags, fields)` — 从 DataPoint 结构推导 Iceberg Schema
- [ ] 实现 `DataPoint → Arrow RecordBatch` 转换 (复用 tsdb-arrow)
- [ ] 定义列 ID 分配规则: timestamp=1, tags 从 2 开始, fields 从 1000 开始
- [ ] 单元测试: `test_schema_from_datapoint`, `test_column_id_assignment`

- **验收**: DataPoint 结构可正确映射为 Iceberg Schema

### P1.8 P1 集成测试

**文件**: `crates/tsdb-iceberg/src/` 或 `tests/`

- [ ] `test_catalog_full_lifecycle`: create_table → load_table → list → rename → drop
- [ ] `test_catalog_persistence`: 创建 Catalog → 关闭 → 重新打开 → 数据一致
- [ ] `test_schema_datapoint_roundtrip`: DataPoint → Schema → Arrow Schema → 兼容
- [ ] `test_partition_spec_day_transform`: day(timestamp) 分区正确

- **验收**: P1 全部功能集成测试通过

---

## P2: Snapshot + Manifest + DataFile [P0]

### P2.1 Snapshot 数据结构

**文件**: `crates/tsdb-iceberg/src/snapshot.rs`

- [ ] 定义 `Snapshot` 结构体 (snapshot_id, parent_snapshot_id, sequence_number, timestamp_ms, manifest_list, summary, schema_id)
- [ ] 定义 `SnapshotSummary` 结构体 (operation, added_data_files, deleted_data_files, added_records, deleted_records, total_data_files, total_records, extra)
- [ ] 定义 `SnapshotOperation` 枚举 (Append, Replace, Overwrite, Delete)
- [ ] 实现 Serialize/Deserialize for Snapshot, SnapshotSummary
- [ ] 实现 `Snapshot::new_append()` / `Snapshot::new_replace()` 构造方法
- [ ] 单元测试: `test_snapshot_serde_roundtrip`, `test_snapshot_new_append`, `test_snapshot_new_replace`

- **验收**: Snapshot 结构完整，序列化正确

### P2.2 DataFile 数据结构

**文件**: `crates/tsdb-iceberg/src/manifest.rs`

- [ ] 定义 `DataFile` 结构体 (content, file_path, file_format, partition, record_count, file_size_in_bytes, column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds, split_offsets, sort_order_id)
- [ ] 定义 `DataContentType` 枚举 (Data, PositionDeletes, EqualityDeletes)
- [ ] 定义 `PartitionData` 类型 (BTreeMap<i32, serde_json::Value>)
- [ ] 实现 `DataFile` 的列统计访问方法
- [ ] 实现 Serialize/Deserialize for DataFile
- [ ] 单元测试: `test_data_file_serde_roundtrip`, `test_data_file_bounds_access`

- **验收**: DataFile 结构完整，列统计可访问

### P2.3 ManifestEntry 数据结构

**文件**: `crates/tsdb-iceberg/src/manifest.rs`

- [ ] 定义 `ManifestEntry` 结构体 (status, snapshot_id, sequence_number, file_sequence_number, data_file)
- [ ] 定义 `EntryStatus` 枚举 (Existing=0, Added=1, Deleted=2)
- [ ] 定义 `ManifestMeta` 结构体 (manifest_id, schema_id, partition_spec_id, added_files_count, existing_files_count, deleted_files_count, partitions_summary)
- [ ] 实现 Serialize/Deserialize for ManifestEntry, ManifestMeta
- [ ] 单元测试: `test_manifest_entry_serde_roundtrip`, `test_entry_status_values`

- **验收**: ManifestEntry 和 ManifestMeta 结构完整

### P2.4 Manifest 读写

**文件**: `crates/tsdb-iceberg/src/manifest.rs`

- [ ] 实现 `ManifestWriter` — 将 ManifestEntry 列表写入 RocksDB `_manifest` CF
- [ ] 实现 `ManifestReader` — 从 RocksDB `_manifest` CF 读取 ManifestEntry 列表
- [ ] 实现 `ManifestListWriter` — 将 ManifestMeta 列表写入 `_manifest_list` CF
- [ ] 实现 `ManifestListReader` — 从 `_manifest_list` CF 读取 ManifestMeta 列表
- [ ] Key 编码: `{table}\x00{manifest_id}\x00{file_idx}` (manifest), `{table}\x00{snap_id}\x00{seq}` (manifest_list)
- [ ] 单元测试: `test_manifest_write_read_roundtrip`, `test_manifest_list_write_read_roundtrip`

- **验收**: Manifest 读写正确，Key 编码一致

### P2.5 Snapshot 管理

**文件**: `crates/tsdb-iceberg/src/snapshot.rs`

- [ ] 实现 `SnapshotManager` — 管理快照的创建、提交、查询
- [ ] 实现 `create_snapshot(table, operation, manifest_entries, parent_snapshot_id)` — 创建新快照
- [ ] 实现 `commit_snapshot(catalog, table, old_meta, new_snapshot)` — 提交快照到 RocksDB
- [ ] 实现 `load_snapshot(catalog, table, snapshot_id)` — 加载指定快照
- [ ] 实现 `load_current_snapshot(catalog, table)` — 加载当前快照
- [ ] 单元测试: `test_create_snapshot`, `test_commit_snapshot`, `test_load_snapshot`

- **验收**: 快照创建和提交正确

### P2.6 Refs (Branch/Tag) 管理

**文件**: `crates/tsdb-iceberg/src/snapshot.rs`

- [ ] 定义 `Ref` 结构体 (snapshot_id, type: main/branch/tag, min_snapshots_to_keep, max_snapshot_age_ms, max_ref_age_ms)
- [ ] 实现 `RefsManager` — 管理快照引用
- [ ] 实现 `create_ref(catalog, table, ref_name, snapshot_id, ref_type)` — 创建引用
- [ ] 实现 `load_ref(catalog, table, ref_name)` — 加载引用
- [ ] Key 编码: `{table}\x00{ref_name}` → `_refs` CF
- [ ] 单元测试: `test_create_ref`, `test_load_ref`, `test_main_ref_auto_created`

- **验收**: 引用管理正确，main 引用自动创建

### P2.7 原子提交协议

**文件**: `crates/tsdb-iceberg/src/commit.rs`

- [ ] 定义 `CommitConflict` 错误类型
- [ ] 实现 `atomic_commit(catalog, table, old_meta, new_meta, new_entries)` — 乐观并发提交
- [ ] 使用 RocksDB WriteBatch 保证原子性:
  - 写入新 Snapshot → `_snapshot` CF
  - 写入新 ManifestList → `_manifest_list` CF
  - 写入新 Manifest entries → `_manifest` CF
  - 更新 TableMetadata → `_table_meta` CF
- [ ] 实现冲突检测: `current.last_updated_ms != old.last_updated_ms` → CommitConflict
- [ ] 实现重试逻辑: 冲突时重新读取并重试 (最多 3 次)
- [ ] 单元测试: `test_atomic_commit_success`, `test_atomic_commit_conflict_retry`, `test_atomic_commit_batch_integrity`

- **验收**: 原子提交正确，冲突检测和重试有效

### P2.8 P2 集成测试

**文件**: `crates/tsdb-iceberg/src/` 或 `tests/`

- [ ] `test_snapshot_lifecycle`: 创建表 → 创建快照 → 提交 → 加载 → 验证
- [ ] `test_manifest_write_read`: 写入 ManifestEntry → 读取 → 验证
- [ ] `test_atomic_commit_full`: 完整提交流程 (Snapshot + Manifest + Metadata)
- [ ] `test_concurrent_commit_conflict`: 两个并发提交 → 一个成功一个冲突重试
- [ ] `test_refs_management`: 创建 main/branch/tag 引用 → 查询

- **验收**: P2 全部功能集成测试通过

---

## P3: Append 写入 + 列统计收集 [P0]

### P3.1 IcebergTable 基础

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 定义 `IcebergTable` 结构体 (catalog: Arc<IcebergCatalog>, name: String, metadata: TableMetadata)
- [ ] 实现 `IcebergTable::name()` / `schema()` / `partition_spec()` / `current_snapshot()` / `snapshots()` / `history()`
- [ ] 单元测试: `test_table_accessors`

- **验收**: IcebergTable 基础访问方法正确

### P3.2 列统计收集

**文件**: `crates/tsdb-iceberg/src/stats.rs`

- [ ] 实现 `collect_column_stats(record_batch: &RecordBatch, schema: &Schema) -> ColumnStats`
- [ ] 定义 `ColumnStats` 结构体 (column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds)
- [ ] 实现 `value_to_iceberg_bytes()` — 将值编码为 Iceberg 统计字节格式
- [ ] 支持类型: Int/Long/Float/Double/Timestamp/String/Boolean
- [ ] 单元测试: `test_collect_stats_numeric`, `test_collect_stats_string`, `test_collect_stats_nulls`, `test_collect_stats_empty_batch`

- **验收**: 列统计收集正确，支持所有基本类型

### P3.3 Parquet 文件写入 (Iceberg 风格)

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `write_parquet_file(table, datapoints, partition) -> DataFile`:
  - DataPoint → Arrow RecordBatch (复用 tsdb-arrow)
  - RecordBatch → Parquet 文件 (复用 tsdb-parquet writer)
  - 收集列统计 → 构建 DataFile
  - 文件路径: `{location}/data/{partition_path}/part-{uuid}.parquet`
- [ ] 实现 `build_partition_data(datapoints, partition_spec) -> PartitionData`
- [ ] 单元测试: `test_write_parquet_file`, `test_build_partition_data`

- **验收**: Parquet 文件写入正确，DataFile 统计完整

### P3.4 Append 写入

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `IcebergTable::append(datapoints)`:
  1. 按 partition_spec 分组 DataPoint
  2. 每组写入 Parquet 文件 → 收集 DataFile
  3. 创建 ManifestEntry (status=Added)
  4. 创建新 Snapshot (operation=append)
  5. 原子提交 (WriteBatch)
- [ ] 处理空表首次 append (current_snapshot_id = -1)
- [ ] 更新 TableMetadata: last_sequence_number, last_updated_ms, current_snapshot_id, snapshots, snapshot_log
- [ ] 单元测试: `test_append_to_empty_table`, `test_append_multiple_times`, `test_append_with_partition`

- **验收**: Append 写入正确，快照链完整

### P3.5 WriteBuffer 批量写入优化

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `IcebergTable::append_batch(datapoints)` — 大批量写入优化
- [ ] 按 target_file_size (默认 128MB) 切分数据为多个 Parquet 文件
- [ ] 单个 WriteBatch 包含所有新 ManifestEntry + Snapshot + Metadata
- [ ] 单元测试: `test_append_batch_large_dataset`, `test_append_batch_file_size_limit`

- **验收**: 批量写入正确，文件大小合理

### P3.6 DataPoint 读取验证

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `IcebergTable::read_data_file(data_file) -> Vec<RecordBatch>` — 读取 Parquet 文件
- [ ] 验证 append 后数据可读: 写入 → 读取 → 比对
- [ ] 单元测试: `test_read_data_file`, `test_append_then_read_roundtrip`

- **验收**: 写入的数据可正确读回

### P3.7 P3 集成测试

**文件**: `crates/tsdb-iceberg/src/` 或 `tests/`

- [ ] `test_append_full_lifecycle`: create_table → append → 读取验证
- [ ] `test_append_multiple_snapshots`: 多次 append → 快照链正确
- [ ] `test_append_partitioned_data`: 按天分区 → 多分区文件正确
- [ ] `test_column_stats_accuracy`: 列统计与实际数据一致
- [ ] `test_append_large_batch`: 100K DataPoint 批量写入

- **验收**: P3 全部功能集成测试通过

---

## P4: Scan + 谓词下推 (三级过滤) [P0]

### P4.1 IcebergScanBuilder

**文件**: `crates/tsdb-iceberg/src/scan.rs`

- [ ] 定义 `IcebergScanBuilder` 结构体 (table, snapshot_id, predicate, projection, case_sensitive)
- [ ] 实现 `predicate(expr)` / `projection(field_ids)` / `case_insensitive()` / `build()` 方法
- [ ] `build()` 返回 `IcebergScan`
- [ ] 单元测试: `test_scan_builder_default`, `test_scan_builder_with_predicate`

- **验收**: ScanBuilder 配置正确

### P4.2 IcebergScan 文件规划

**文件**: `crates/tsdb-iceberg/src/scan.rs`

- [ ] 定义 `IcebergScan` 结构体 (data_files, predicate, projection)
- [ ] 实现 `plan()` — 返回匹配的数据文件列表
- [ ] 实现文件规划核心逻辑:
  1. 加载 current_snapshot → manifest_list
  2. 遍历 manifest_list → 加载 manifest entries
  3. 过滤 DELETED 状态的 entry
  4. 应用谓词下推过滤
- [ ] 单元测试: `test_scan_plan_all_files`, `test_scan_plan_with_no_predicate`

- **验收**: 文件规划正确，返回匹配的 DataFile 列表

### P4.3 Level 1: Manifest 级别过滤

**文件**: `crates/tsdb-iceberg/src/scan.rs`

- [ ] 实现 `manifest_matches_predicate(manifest_meta, predicate)` — 使用分区统计过滤
- [ ] 利用 ManifestMeta 的 partitions_summary (contains_null, lower_bound, upper_bound)
- [ ] 跳过不包含匹配分区的整个 Manifest
- [ ] 单元测试: `test_manifest_filter_day_partition`, `test_manifest_filter_no_match`

- **验收**: Manifest 级别过滤正确跳过不匹配的 Manifest

### P4.4 Level 2: DataFile 级别过滤

**文件**: `crates/tsdb-iceberg/src/scan.rs`

- [ ] 实现 `file_matches_predicate(data_file, predicate)` — 使用列统计过滤
- [ ] 利用 DataFile 的 lower_bounds / upper_bounds
- [ ] 支持: 等值条件 (col = val), 范围条件 (col > val, col < val), IN 条件
- [ ] 跳过列值范围不匹配的数据文件
- [ ] 单元测试: `test_file_filter_timestamp_range`, `test_file_filter_tag_equals`, `test_file_filter_no_match`

- **验收**: DataFile 级别过滤正确跳过不匹配的文件

### P4.5 Level 3: Parquet RowGroup 级别过滤

**文件**: `crates/tsdb-iceberg/src/scan.rs`

- [ ] 实现 `row_group_matches_predicate(parquet_metadata, row_group_idx, predicate)` — 使用 RowGroup 统计
- [ ] 读取 Parquet 文件的元数据 (parquet::file::metadata::ParquetMetaData)
- [ ] 利用 RowGroup 的 column_chunk 统计 (min/max)
- [ ] 跳过不匹配的 RowGroup
- [ ] 单元测试: `test_row_group_filter`, `test_row_group_filter_all_skipped`

- **验收**: RowGroup 级别过滤正确跳过不匹配的 RowGroup

### P4.6 IcebergScan 执行

**文件**: `crates/tsdb-iceberg/src/scan.rs`

- [ ] 实现 `IcebergScan::execute()` — 执行扫描返回 RecordBatch
- [ ] 流程: plan() → 遍历 data_files → 读取 Parquet → RowGroup 过滤 → 合并 RecordBatch
- [ ] 支持列投影: 只读取 projection 指定的列
- [ ] 实现 `IcebergScan::to_record_batches()` — 同步版本
- [ ] 单元测试: `test_scan_execute_full`, `test_scan_execute_with_projection`, `test_scan_execute_with_predicate`

- **验收**: Scan 执行正确，三级过滤生效

### P4.7 P4 集成测试

**文件**: `crates/tsdb-iceberg/src/` 或 `tests/`

- [ ] `test_scan_full_lifecycle`: append 数据 → scan → 验证结果
- [ ] `test_scan_predicate_pushdown_timestamp`: 时间范围谓词 → 跳过不匹配文件
- [ ] `test_scan_predicate_pushdown_tag`: 标签等值谓词 → 跳过不匹配文件
- [ ] `test_scan_projection`: 只查询部分列 → 只读对应 Parquet 列
- [ ] `test_scan_three_level_filter`: 三级过滤全部生效 → 验证跳过率
- [ ] `test_scan_empty_result`: 谓词无匹配 → 返回空 RecordBatch

- **验收**: P4 全部功能集成测试通过

---

## P5: IcebergTableProvider (DataFusion) [P0]

### P5.1 IcebergTableProvider 结构

**文件**: `crates/tsdb-iceberg/src/table_provider.rs` (或 tsdb-datafusion 中)

- [ ] 定义 `IcebergTableProvider` 结构体 (table: Arc<Mutex<IcebergTable>>, schema: SchemaRef)
- [ ] 实现 `IcebergTableProvider::new(table)` — 从 IcebergTable 创建 Provider
- [ ] 实现 Iceberg Schema → DataFusion Schema 转换
- [ ] 单元测试: `test_provider_creation`, `test_provider_schema`

- **验收**: Provider 创建正确，Schema 转换正确

### P5.2 TableProvider trait 实现

**文件**: `crates/tsdb-iceberg/src/table_provider.rs`

- [ ] 实现 `TableProvider::as_any()`
- [ ] 实现 `TableProvider::schema()`
- [ ] 实现 `TableProvider::table_type()` → Base
- [ ] 实现 `TableProvider::scan()`:
  - 将 DataFusion filters 转为 Iceberg 谓词
  - 调用 IcebergScanBuilder → IcebergScan
  - 创建 IcebergScanExec (ExecutionPlan)
- [ ] 单元测试: `test_table_provider_scan`

- **验收**: TableProvider trait 实现完整

### P5.3 IcebergScanExec (ExecutionPlan)

**文件**: `crates/tsdb-iceberg/src/scan_exec.rs`

- [ ] 定义 `IcebergScanExec` 结构体 (data_files, predicate, projection, schema, limit)
- [ ] 实现 `ExecutionPlan` trait:
  - `schema()` → 投影后的 Schema
  - `children()` → 空
  - `with_new_children()` → 自身
  - `execute()` → 创建 IcebergScanStream
- [ ] 单元测试: `test_scan_exec_creation`, `test_scan_exec_schema`

- **验收**: ExecutionPlan 实现正确

### P5.4 IcebergScanStream (FileStream)

**文件**: `crates/tsdb-iceberg/src/scan_exec.rs`

- [ ] 定义 `IcebergScanStream` — 实现 `SendableRecordBatchStream`
- [ ] 实现流式读取: 逐文件读取 Parquet → 逐 RowGroup 过滤 → yield RecordBatch
- [ ] 支持列投影: 只解码 projection 指定的列
- [ ] 支持 limit: 读够行数后停止
- [ ] 单元测试: `test_scan_stream_single_file`, `test_scan_stream_multiple_files`, `test_scan_stream_with_limit`

- **验收**: 流式读取正确，支持投影和 limit

### P5.5 DataFusion filters → Iceberg 谓词转换

**文件**: `crates/tsdb-iceberg/src/table_provider.rs`

- [ ] 实现 `filters_to_predicate(filters: &[Expr]) -> Option<Predicate>`
- [ ] 支持: Eq, Gt, GtEq, Lt, LtEq, InList, And, Or, Not
- [ ] 定义 `Predicate` 枚举 (AlwaysTrue, And, Or, Not, Eq, Gt, Lt, InList, IsNull, IsNotNull)
- [ ] 单元测试: `test_filter_to_predicate_eq`, `test_filter_to_predicate_range`, `test_filter_to_predicate_and_or`

- **验收**: DataFusion 过滤条件正确转为 Iceberg 谓词

### P5.6 P5 集成测试

**文件**: `crates/tsdb-iceberg/src/` 或 `tests/`

- [ ] `test_datafusion_sql_select`: `SELECT * FROM table` 全表查询
- [ ] `test_datafusion_sql_where_timestamp`: `WHERE timestamp > ...` 时间过滤
- [ ] `test_datafusion_sql_where_tag`: `WHERE tag_host = 'server1'` 标签过滤
- [ ] `test_datafusion_sql_aggregation`: `AVG/sum/count` 聚合查询
- [ ] `test_datafusion_sql_group_by`: `GROUP BY` 分组查询
- [ ] `test_datafusion_sql_limit`: `LIMIT` 限制行数
- [ ] `test_datafusion_predicate_pushdown`: 验证谓词下推生效 (减少读取文件数)

- **验收**: DataFusion SQL 查询 Iceberg 表全部正确

---

## P6: Time Travel 查询 [P1]

### P6.1 scan_at_snapshot 实现

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `IcebergTable::scan_at_snapshot(snapshot_id)` — 指定快照查询
- [ ] 实现 `IcebergTable::scan_at_timestamp(timestamp_ms)` — 指定时间点查询
- [ ] 实现 `IcebergTable::snapshots()` — 返回快照历史
- [ ] 实现 `IcebergTable::snapshot_by_id(id)` / `snapshot_by_timestamp(ts)` 查找方法
- [ ] 单元测试: `test_scan_at_snapshot`, `test_scan_at_timestamp`

- **验收**: Time Travel 查询正确

### P6.2 快照历史查询

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `IcebergTable::history()` — 返回 SnapshotLogEntry 列表
- [ ] 实现 `IcebergTable::snapshot_diff(from_id, to_id)` — 两个快照之间的差异
- [ ] 差异包含: added_files, deleted_files, added_records, deleted_records
- [ ] 单元测试: `test_history`, `test_snapshot_diff`

- **验收**: 快照历史和差异查询正确

### P6.3 Rollback 实现

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `IcebergTable::rollback_to_snapshot(snapshot_id)` — 回滚到指定快照
- [ ] 实现 `IcebergTable::rollback_to_timestamp(timestamp_ms)` — 回滚到指定时间点
- [ ] Rollback = 创建新 Snapshot 指向旧快照的数据文件
- [ ] 原子提交更新 current_snapshot_id
- [ ] 单元测试: `test_rollback_to_snapshot`, `test_rollback_then_query`

- **验收**: Rollback 正确，回滚后查询数据正确

### P6.4 Branch/Tag 查询

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `IcebergTable::scan_ref(ref_name)` — 按引用名查询
- [ ] 实现 `IcebergTable::create_branch(name, snapshot_id)` — 创建分支
- [ ] 实现 `IcebergTable::create_tag(name, snapshot_id)` — 创建标签
- [ ] 单元测试: `test_scan_branch`, `test_scan_tag`

- **验收**: Branch/Tag 查询正确

### P6.5 P6 集成测试

**文件**: `crates/tsdb-iceberg/src/` 或 `tests/`

- [ ] `test_time_travel_full`: append → append → 查询快照1 → 查询快照2 → 验证不同
- [ ] `test_time_travel_by_timestamp`: 按时间点查询 → 数据正确
- [ ] `test_rollback_and_query`: append → append → rollback → 查询 → 验证回滚
- [ ] `test_branch_tag_query`: 创建 branch/tag → 按引用查询 → 正确
- [ ] `test_snapshot_diff`: 两个快照差异 → added/deleted 文件正确

- **验收**: P6 全部功能集成测试通过

---

## P7: Compaction (Iceberg 风格) [P1]

### P7.1 小文件选择策略

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `IcebergTable::find_compaction_candidates(target_size)` — 选择小文件
- [ ] 策略: file_size < target_file_size (默认 128MB) 的文件
- [ ] 按分区分组候选文件
- [ ] 单元测试: `test_find_compaction_candidates`

- **验收**: 正确选择需要合并的小文件

### P7.2 文件合并

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `compact_partition(table, partition, data_files) -> DataFile`:
  - 读取所有小文件 → 合并 RecordBatch
  - 写入新的大 Parquet 文件
  - 收集列统计 → 构建 DataFile
- [ ] 单元测试: `test_compact_partition`

- **验收**: 合并后文件数据完整，统计正确

### P7.3 Compaction 提交

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `IcebergTable::compact()`:
  1. 查找小文件候选
  2. 按分区合并
  3. 创建新 Manifest:
     - 旧文件标记为 DELETED
     - 新文件标记为 ADDED
  4. 创建新 Snapshot (operation=replace)
  5. 原子提交
- [ ] 单元测试: `test_compact_commit`, `test_compact_snapshot_operation`

- **验收**: Compaction 提交正确，快照操作为 replace

### P7.4 Compaction 后验证

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 验证 compact 后查询结果不变
- [ ] 验证 compact 后文件数减少
- [ ] 验证 compact 后总数据量不变
- [ ] 单元测试: `test_compact_query_consistency`, `test_compact_file_count_reduction`

- **验收**: Compaction 后数据一致性

### P7.5 P7 集成测试

**文件**: `crates/tsdb-iceberg/src/` 或 `tests/`

- [ ] `test_compact_full_lifecycle`: 多次小批量 append → compact → 查询一致
- [ ] `test_compact_multiple_partitions`: 多分区分别 compact
- [ ] `test_compact_with_concurrent_reads`: compact 中查询不阻塞
- [ ] `test_compact_snapshot_history`: compact 后快照历史完整

- **验收**: P7 全部功能集成测试通过

---

## P8: Snapshot 过期清理 [P1]

### P8.1 过期快照选择

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `IcebergTable::expire_snapshots(older_than_ms, min_snapshots)`:
  - 选择 timestamp_ms < older_than_ms 的快照
  - 保留至少 min_snapshots 个快照
  - 不删除被 branch/tag 引用的快照
- [ ] 单元测试: `test_expire_snapshots_by_age`, `test_expire_snapshots_min_retention`

- **验收**: 正确选择过期快照

### P8.2 孤立文件清理

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `IcebergTable::remove_orphan_files(older_than_ms)`:
  - 扫描数据目录中的所有文件
  - 与当前有效快照引用的文件对比
  - 删除不被任何快照引用的文件
- [ ] 单元测试: `test_remove_orphan_files`

- **验收**: 孤立文件正确清理

### P8.3 过期提交验证

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 验证过期后当前快照仍可查询
- [ ] 验证过期后快照历史被正确截断
- [ ] 验证过期后 TableMetadata 更新正确
- [ ] 单元测试: `test_expire_then_query`, `test_expire_metadata_update`

- **验收**: 过期后系统状态正确

### P8.4 P8 集成测试

**文件**: `crates/tsdb-iceberg/src/` 或 `tests/`

- [ ] `test_expire_snapshots_full`: 多次 append → expire → 验证
- [ ] `test_expire_with_branch_protection`: branch 引用的快照不被过期
- [ ] `test_orphan_file_cleanup`: compact → expire → 孤立文件清理
- [ ] `test_expire_then_time_travel`: 过期后旧快照不可查询

- **验收**: P8 全部功能集成测试通过

---

## P9: Schema 演化 [P2]

### P9.1 SchemaChange 定义

**文件**: `crates/tsdb-iceberg/src/schema.rs`

- [ ] 定义 `SchemaChange` 枚举:
  - AddField { parent_id, name, field_type, required, write_default }
  - DeleteField { field_id }
  - RenameField { field_id, new_name }
  - PromoteType { field_id, new_type } (int→long, float→double)
  - MoveField { field_id, new_position }
- [ ] 单元测试: `test_schema_change_variants`

- **验收**: SchemaChange 枚举完整

### P9.2 Schema 演化应用

**文件**: `crates/tsdb-iceberg/src/schema.rs`

- [ ] 实现 `Schema::apply_change(change) -> Schema` — 应用单个变更
- [ ] 实现 `Schema::apply_changes(changes) -> Schema` — 应用多个变更
- [ ] 新 Schema 分配新的 schema_id
- [ ] last_column_id 递增 (AddField 时)
- [ ] 单元测试: `test_apply_add_field`, `test_apply_delete_field`, `test_apply_rename_field`, `test_apply_promote_type`

- **验收**: Schema 变更应用正确

### P9.3 update_schema 实现

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `IcebergTable::update_schema(changes)`:
  1. 应用 SchemaChange → 新 Schema
  2. 更新 TableMetadata: current_schema_id, schemas, last_column_id
  3. 原子提交
- [ ] 单元测试: `test_update_schema_commit`, `test_update_schema_history`

- **验收**: Schema 更新提交正确

### P9.4 Schema 演化后查询兼容

**文件**: `crates/tsdb-iceberg/src/scan.rs`

- [ ] Scan 时根据每个 DataFile 关联的 schema_id 选择正确的 Schema
- [ ] 新增列: 旧文件中该列返回 null
- [ ] 删除列: 旧文件中该列被忽略
- [ ] 重命名列: 旧文件中用旧名映射到新名
- [ ] 单元测试: `test_scan_after_add_field`, `test_scan_after_delete_field`, `test_scan_after_rename_field`

- **验收**: Schema 演化后查询兼容

### P9.5 P9 集成测试

**文件**: `crates/tsdb-iceberg/src/` 或 `tests/`

- [ ] `test_schema_evolution_add_field`: 添加字段 → 旧数据新字段为 null
- [ ] `test_schema_evolution_delete_field`: 删除字段 → 旧数据该列忽略
- [ ] `test_schema_evolution_rename_field`: 重命名字段 → 查询正确
- [ ] `test_schema_evolution_promote_type`: int→long → 查询正确
- [ ] `test_schema_evolution_multiple_changes`: 多次变更 → 历史完整

- **验收**: P9 全部功能集成测试通过

---

## P10: 分区演化 [P2]

### P10.1 PartitionSpec 变更

**文件**: `crates/tsdb-iceberg/src/partition.rs`

- [ ] 实现 `PartitionSpec::evolve(changes) -> PartitionSpec` — 分区规范演化
- [ ] 定义 `PartitionSpecChange` 枚举 (AddField, RemoveField, RenameField)
- [ ] 新 PartitionSpec 分配新的 spec_id
- [ ] 单元测试: `test_partition_spec_evolve`

- **验收**: 分区规范演化正确

### P10.2 update_partition_spec 实现

**文件**: `crates/tsdb-iceberg/src/table.rs`

- [ ] 实现 `IcebergTable::update_partition_spec(new_spec)`:
  1. 验证新 PartitionSpec 兼容性
  2. 更新 TableMetadata: default_spec_id, partition_specs
  3. 原子提交
- [ ] 单元测试: `test_update_partition_spec`

- **验收**: 分区规范更新提交正确

### P10.3 分区演化后查询兼容

**文件**: `crates/tsdb-iceberg/src/scan.rs`

- [ ] Scan 时根据每个 DataFile 关联的 partition_spec_id 选择正确的 PartitionSpec
- [ ] 旧分区数据仍可按旧分区规范查询
- [ ] 新分区数据按新分区规范查询
- [ ] 单元测试: `test_scan_after_partition_evolution`

- **验收**: 分区演化后查询兼容

### P10.4 P10 集成测试

**文件**: `crates/tsdb-iceberg/src/` 或 `tests/`

- [ ] `test_partition_evolution_full`: 旧分区写入 → 演化 → 新分区写入 → 查询全部正确
- [ ] `test_partition_evolution_compact`: 演化后 compact → 数据一致
- [ ] `test_partition_evolution_time_travel`: 演化后 Time Travel → 旧分区数据正确
- [ ] `test_partition_evolution_schema_compat`: 分区演化 + Schema 演化同时进行

- **验收**: P10 全部功能集成测试通过

---

## 执行顺序

```
P1 (Catalog + TableMetadata + Schema)
├── P1.1 crate 骨架
├── P1.2 Schema 数据结构
├── P1.3 PartitionSpec 数据结构
├── P1.4 TableMetadata 数据结构
├── P1.5 IcebergCatalog 实现
├── P1.6 Catalog 辅助方法
├── P1.7 DataPoint ↔ Schema 映射
└── P1.8 P1 集成测试
     │
     ▼
P2 (Snapshot + Manifest + DataFile)
├── P2.1 Snapshot 数据结构
├── P2.2 DataFile 数据结构
├── P2.3 ManifestEntry 数据结构
├── P2.4 Manifest 读写
├── P2.5 Snapshot 管理
├── P2.6 Refs 管理
├── P2.7 原子提交协议
└── P2.8 P2 集成测试
     │
     ▼
P3 (Append 写入 + 列统计)
├── P3.1 IcebergTable 基础
├── P3.2 列统计收集
├── P3.3 Parquet 文件写入
├── P3.4 Append 写入
├── P3.5 WriteBuffer 批量优化
├── P3.6 DataPoint 读取验证
└── P3.7 P3 集成测试
     │
     ├──▶ P4 (Scan + 谓词下推)
     │    ├── P4.1 IcebergScanBuilder
     │    ├── P4.2 IcebergScan 文件规划
     │    ├── P4.3 Level 1: Manifest 过滤
     │    ├── P4.4 Level 2: DataFile 过滤
     │    ├── P4.5 Level 3: RowGroup 过滤
     │    ├── P4.6 IcebergScan 执行
     │    └── P4.7 P4 集成测试
     │         │
     │         ▼
     │    P5 (IcebergTableProvider)
     │    ├── P5.1 Provider 结构
     │    ├── P5.2 TableProvider trait
     │    ├── P5.3 IcebergScanExec
     │    ├── P5.4 IcebergScanStream
     │    ├── P5.5 filters → 谓词转换
     │    └── P5.6 P5 集成测试
     │         │
     │         ├──▶ P6 (Time Travel)
     │         │    └── P6.1~P6.5
     │         │         │
     │         │         └──▶ P8 (Snapshot 过期)
     │         │              └── P8.1~P8.4
     │         │
     │         └──▶ P9 (Schema 演化)
     │              └── P9.1~P9.5
     │                   │
     │                   └──▶ P10 (分区演化)
     │                        └── P10.1~P10.4
     │
     └──▶ P7 (Compaction)
          └── P7.1~P7.5
```

---

## 与现有系统集成

### 集成点

| 集成点 | 说明 | 依赖 |
|--------|------|------|
| tsdb-arrow | DataPoint ↔ Arrow RecordBatch 转换 | P1 |
| tsdb-rocksdb | RocksDB 引擎 (CF 管理、WriteBatch) | P1 |
| tsdb-parquet | Parquet 文件读写、列统计 | P3 |
| tsdb-datafusion | IcebergTableProvider 注册 | P5 |
| tsdb-flight | Flight SQL 查询 Iceberg 表 | P5 |
| tsdb-cli | CLI 支持 Iceberg 表操作 | P5 |

### CLI 扩展 (P5 完成后)

- [ ] `tsdb-cli iceberg create-table --name cpu --schema ...`
- [ ] `tsdb-cli iceberg append --name cpu --file data.json`
- [ ] `tsdb-cli iceberg scan --name cpu --sql "SELECT * FROM cpu"`
- [ ] `tsdb-cli iceberg snapshots --name cpu`
- [ ] `tsdb-cli iceberg compact --name cpu`
- [ ] `tsdb-cli iceberg rollback --name cpu --snapshot-id 123`
