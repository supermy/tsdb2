# TSDB2 Iceberg 模拟功能 — 验收检查清单

> 基于 spec/spec.md 规格和 tasks.md 任务，逐项验收

---

## 验收总览

| Phase | 名称 | 检查项 | 通过门槛 | 当前状态 |
|-------|------|--------|---------|---------|
| P1 | Catalog + TableMetadata + Schema | 22 | ≥20 (91%) | ⏳ 待验证 |
| P2 | Snapshot + Manifest + DataFile | 20 | ≥18 (90%) | ⏳ 待验证 |
| P3 | Append 写入 + 列统计收集 | 18 | ≥16 (89%) | ⏳ 待验证 |
| P4 | Scan + 谓词下推 (三级过滤) | 18 | ≥16 (89%) | ⏳ 待验证 |
| P5 | IcebergTableProvider (DataFusion) | 16 | ≥14 (88%) | ⏳ 待验证 |
| P6 | Time Travel 查询 | 12 | ≥11 (92%) | ⏳ 待验证 |
| P7 | Compaction (Iceberg 风格) | 12 | ≥11 (92%) | ⏳ 待验证 |
| P8 | Snapshot 过期清理 | 10 | ≥9 (90%) | ⏳ 待验证 |
| P9 | Schema 演化 | 13 | ≥12 (92%) | ⏳ 待验证 |
| P10 | 分区演化 | 10 | ≥9 (90%) | ⏳ 待验证 |
| **合计** | | **151** | **≥136 (90%)** | **⏳** |

---

## P1: Catalog + TableMetadata + Schema 验收

### P1.1 tsdb-iceberg crate 骨架

- [ ] `Cargo.toml` 创建完成，依赖声明正确
- [ ] `src/lib.rs` 入口文件存在，重导出公共 API
- [ ] `src/error.rs` 包含 IcebergError 枚举 (Catalog, TableNotFound, CommitConflict, Schema, Manifest, Io, Json, Rocksdb)
- [ ] workspace `Cargo.toml` 包含 tsdb-iceberg 条目
- [ ] `cargo check -p tsdb-iceberg` 无错误

### P1.2 Schema 数据结构

- [ ] `Schema` 结构体包含 schema_id, fields
- [ ] `Field` 结构体包含 id, name, required, field_type, doc, initial_default, write_default
- [ ] `IcebergType` 枚举覆盖: Boolean, Int, Long, Float, Double, Decimal, Date, Time, Timestamp, Timestamptz, String, Uuid, Binary, Struct, List, Map
- [ ] `Schema::arrow_schema()` 正确转换为 Arrow Schema
- [ ] `Schema::field_by_id()` / `field_by_name()` 查找正确
- [ ] Serialize/Deserialize 实现正确
- [ ] `test_schema_serde_roundtrip` 通过
- [ ] `test_schema_to_arrow` 通过
- [ ] `test_field_lookup` 通过

### P1.3 PartitionSpec 数据结构

- [ ] `PartitionSpec` 结构体包含 spec_id, fields
- [ ] `PartitionField` 结构体包含 source_id, field_id, name, transform
- [ ] `Transform` 枚举覆盖: Identity, Bucket, Truncate, Year, Month, Day, Hour, Void
- [ ] `Transform::apply()` 对值执行正确转换
- [ ] `PartitionSpec::partition_path()` 生成正确路径
- [ ] `test_transform_day` 通过
- [ ] `test_transform_identity` 通过
- [ ] `test_partition_path` 通过

### P1.4 TableMetadata 数据结构

- [ ] `TableMetadata` 包含所有字段 (format_version, table_uuid, location, last_sequence_number, last_updated_ms, last_column_id, current_schema_id, schemas, default_spec_id, partition_specs, current_snapshot_id, snapshots, snapshot_log, properties)
- [ ] `SnapshotLogEntry` 结构体包含 snapshot_id, timestamp_ms
- [ ] Serialize/Deserialize 实现正确
- [ ] `TableMetadata::new()` 创建空表元数据，current_snapshot_id = -1
- [ ] `test_table_metadata_new` 通过
- [ ] `test_table_metadata_serde_roundtrip` 通过

### P1.5 IcebergCatalog 实现

- [ ] `IcebergCatalog` 结构体包含 db, base_dir
- [ ] RocksDB CF 正确创建: `_catalog`, `_table_meta`, `_snapshot`, `_manifest_list`, `_manifest`, `_refs`
- [ ] `IcebergCatalog::open(path)` 打开/创建成功
- [ ] `IcebergCatalog::create_table(name, schema, partition_spec)` 创建表成功
- [ ] `IcebergCatalog::load_table(name)` 加载表成功
- [ ] `IcebergCatalog::drop_table(name)` 删除表成功
- [ ] `IcebergCatalog::list_tables()` 列出所有表
- [ ] `IcebergCatalog::rename_table(old, new)` 重命名表
- [ ] `test_catalog_create_load_table` 通过
- [ ] `test_catalog_list_tables` 通过
- [ ] `test_catalog_drop_table` 通过
- [ ] `test_catalog_rename_table` 通过

### P1.6 Catalog 辅助方法

- [ ] `load_metadata(table_name)` 正确读取 TableMetadata
- [ ] `save_metadata(table_name, metadata)` 正确写入 TableMetadata
- [ ] `next_sequence_number()` 单调递增
- [ ] Key 编码函数正确: `meta_key`, `schema_key`, `part_spec_key`
- [ ] `test_key_encoding` 通过
- [ ] `test_sequence_number_monotonic` 通过

### P1.7 DataPoint ↔ Schema 映射

- [ ] `Schema::from_datapoint_schema()` 从 DataPoint 推导 Schema
- [ ] DataPoint → Arrow RecordBatch 转换正确 (复用 tsdb-arrow)
- [ ] 列 ID 分配规则: timestamp=1, tags 从 2, fields 从 1000
- [ ] `test_schema_from_datapoint` 通过
- [ ] `test_column_id_assignment` 通过

### P1.8 P1 集成测试

- [ ] `test_catalog_full_lifecycle` 通过
- [ ] `test_catalog_persistence` 通过
- [ ] `test_schema_datapoint_roundtrip` 通过
- [ ] `test_partition_spec_day_transform` 通过

---

## P2: Snapshot + Manifest + DataFile 验收

### P2.1 Snapshot 数据结构

- [ ] `Snapshot` 包含所有字段 (snapshot_id, parent_snapshot_id, sequence_number, timestamp_ms, manifest_list, summary, schema_id)
- [ ] `SnapshotSummary` 包含所有字段 (operation, added_data_files, deleted_data_files, added_records, deleted_records, total_data_files, total_records, extra)
- [ ] `SnapshotOperation` 枚举: Append, Replace, Overwrite, Delete
- [ ] Serialize/Deserialize 实现正确
- [ ] `Snapshot::new_append()` / `Snapshot::new_replace()` 构造正确
- [ ] `test_snapshot_serde_roundtrip` 通过
- [ ] `test_snapshot_new_append` 通过
- [ ] `test_snapshot_new_replace` 通过

### P2.2 DataFile 数据结构

- [ ] `DataFile` 包含所有字段 (content, file_path, file_format, partition, record_count, file_size_in_bytes, column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds, split_offsets, sort_order_id)
- [ ] `DataContentType` 枚举: Data, PositionDeletes, EqualityDeletes
- [ ] `PartitionData` 类型定义正确
- [ ] 列统计访问方法正确
- [ ] Serialize/Deserialize 实现正确
- [ ] `test_data_file_serde_roundtrip` 通过
- [ ] `test_data_file_bounds_access` 通过

### P2.3 ManifestEntry 数据结构

- [ ] `ManifestEntry` 包含所有字段 (status, snapshot_id, sequence_number, file_sequence_number, data_file)
- [ ] `EntryStatus` 枚举: Existing=0, Added=1, Deleted=2
- [ ] `ManifestMeta` 包含所有字段 (manifest_id, schema_id, partition_spec_id, added_files_count, existing_files_count, deleted_files_count, partitions_summary)
- [ ] Serialize/Deserialize 实现正确
- [ ] `test_manifest_entry_serde_roundtrip` 通过
- [ ] `test_entry_status_values` 通过

### P2.4 Manifest 读写

- [ ] `ManifestWriter` 写入 `_manifest` CF 正确
- [ ] `ManifestReader` 读取 `_manifest` CF 正确
- [ ] `ManifestListWriter` 写入 `_manifest_list` CF 正确
- [ ] `ManifestListReader` 读取 `_manifest_list` CF 正确
- [ ] Key 编码一致: `{table}\x00{manifest_id}\x00{file_idx}`, `{table}\x00{snap_id}\x00{seq}`
- [ ] `test_manifest_write_read_roundtrip` 通过
- [ ] `test_manifest_list_write_read_roundtrip` 通过

### P2.5 Snapshot 管理

- [ ] `SnapshotManager` 管理快照创建/提交/查询
- [ ] `create_snapshot()` 创建新快照正确
- [ ] `commit_snapshot()` 提交快照到 RocksDB 正确
- [ ] `load_snapshot()` 加载指定快照正确
- [ ] `load_current_snapshot()` 加载当前快照正确
- [ ] `test_create_snapshot` 通过
- [ ] `test_commit_snapshot` 通过
- [ ] `test_load_snapshot` 通过

### P2.6 Refs 管理

- [ ] `Ref` 结构体包含 snapshot_id, type, min_snapshots_to_keep, max_snapshot_age_ms, max_ref_age_ms
- [ ] `RefsManager` 管理快照引用
- [ ] `create_ref()` 创建引用正确
- [ ] `load_ref()` 加载引用正确
- [ ] Key 编码: `{table}\x00{ref_name}` → `_refs` CF
- [ ] `test_create_ref` 通过
- [ ] `test_load_ref` 通过
- [ ] `test_main_ref_auto_created` 通过

### P2.7 原子提交协议

- [ ] `CommitConflict` 错误类型定义
- [ ] `atomic_commit()` 乐观并发提交实现
- [ ] WriteBatch 包含: Snapshot + ManifestList + Manifest entries + TableMetadata
- [ ] 冲突检测: `current.last_updated_ms != old.last_updated_ms`
- [ ] 重试逻辑: 最多 3 次
- [ ] `test_atomic_commit_success` 通过
- [ ] `test_atomic_commit_conflict_retry` 通过
- [ ] `test_atomic_commit_batch_integrity` 通过

### P2.8 P2 集成测试

- [ ] `test_snapshot_lifecycle` 通过
- [ ] `test_manifest_write_read` 通过
- [ ] `test_atomic_commit_full` 通过
- [ ] `test_concurrent_commit_conflict` 通过
- [ ] `test_refs_management` 通过

---

## P3: Append 写入 + 列统计收集 验收

### P3.1 IcebergTable 基础

- [ ] `IcebergTable` 结构体包含 catalog, name, metadata
- [ ] `name()` / `schema()` / `partition_spec()` / `current_snapshot()` / `snapshots()` / `history()` 访问方法正确
- [ ] `test_table_accessors` 通过

### P3.2 列统计收集

- [ ] `collect_column_stats()` 正确收集列统计
- [ ] `ColumnStats` 包含 column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds
- [ ] `value_to_iceberg_bytes()` 编码正确
- [ ] 支持 Int/Long/Float/Double/Timestamp/String/Boolean 类型
- [ ] `test_collect_stats_numeric` 通过
- [ ] `test_collect_stats_string` 通过
- [ ] `test_collect_stats_nulls` 通过
- [ ] `test_collect_stats_empty_batch` 通过

### P3.3 Parquet 文件写入

- [ ] `write_parquet_file()` 写入正确
- [ ] DataPoint → Arrow RecordBatch → Parquet 转换链正确
- [ ] 列统计收集 → DataFile 构建正确
- [ ] 文件路径: `{location}/data/{partition_path}/part-{uuid}.parquet`
- [ ] `build_partition_data()` 正确生成分区数据
- [ ] `test_write_parquet_file` 通过
- [ ] `test_build_partition_data` 通过

### P3.4 Append 写入

- [ ] `IcebergTable::append()` 实现完整
- [ ] 按 partition_spec 分组 DataPoint 正确
- [ ] ManifestEntry (status=Added) 创建正确
- [ ] 新 Snapshot (operation=append) 创建正确
- [ ] 原子提交 (WriteBatch) 正确
- [ ] 空表首次 append 处理正确 (current_snapshot_id = -1)
- [ ] TableMetadata 更新正确
- [ ] `test_append_to_empty_table` 通过
- [ ] `test_append_multiple_times` 通过
- [ ] `test_append_with_partition` 通过

### P3.5 WriteBuffer 批量优化

- [ ] `IcebergTable::append_batch()` 实现完整
- [ ] 按 target_file_size 切分数据正确
- [ ] 单个 WriteBatch 包含所有新条目
- [ ] `test_append_batch_large_dataset` 通过
- [ ] `test_append_batch_file_size_limit` 通过

### P3.6 DataPoint 读取验证

- [ ] `IcebergTable::read_data_file()` 正确读取 Parquet 文件
- [ ] 写入 → 读取 → 比对一致
- [ ] `test_read_data_file` 通过
- [ ] `test_append_then_read_roundtrip` 通过

### P3.7 P3 集成测试

- [ ] `test_append_full_lifecycle` 通过
- [ ] `test_append_multiple_snapshots` 通过
- [ ] `test_append_partitioned_data` 通过
- [ ] `test_column_stats_accuracy` 通过
- [ ] `test_append_large_batch` 通过

---

## P4: Scan + 谓词下推 (三级过滤) 验收

### P4.1 IcebergScanBuilder

- [ ] `IcebergScanBuilder` 包含 table, snapshot_id, predicate, projection, case_sensitive
- [ ] `predicate()` / `projection()` / `case_insensitive()` / `build()` 方法正确
- [ ] `build()` 返回 `IcebergScan`
- [ ] `test_scan_builder_default` 通过
- [ ] `test_scan_builder_with_predicate` 通过

### P4.2 IcebergScan 文件规划

- [ ] `IcebergScan` 包含 data_files, predicate, projection
- [ ] `plan()` 返回匹配的 DataFile 列表
- [ ] 文件规划核心逻辑正确: snapshot → manifest_list → entries → 过滤
- [ ] `test_scan_plan_all_files` 通过
- [ ] `test_scan_plan_with_no_predicate` 通过

### P4.3 Level 1: Manifest 级别过滤

- [ ] `manifest_matches_predicate()` 使用分区统计过滤
- [ ] 利用 partitions_summary (contains_null, lower_bound, upper_bound)
- [ ] 跳过不匹配的整个 Manifest
- [ ] `test_manifest_filter_day_partition` 通过
- [ ] `test_manifest_filter_no_match` 通过

### P4.4 Level 2: DataFile 级别过滤

- [ ] `file_matches_predicate()` 使用列统计过滤
- [ ] 利用 lower_bounds / upper_bounds
- [ ] 支持等值、范围、IN 条件
- [ ] 跳过列值范围不匹配的文件
- [ ] `test_file_filter_timestamp_range` 通过
- [ ] `test_file_filter_tag_equals` 通过
- [ ] `test_file_filter_no_match` 通过

### P4.5 Level 3: RowGroup 级别过滤

- [ ] `row_group_matches_predicate()` 使用 RowGroup 统计
- [ ] 读取 Parquet 元数据正确
- [ ] 利用 RowGroup column_chunk 统计 (min/max)
- [ ] 跳过不匹配的 RowGroup
- [ ] `test_row_group_filter` 通过
- [ ] `test_row_group_filter_all_skipped` 通过

### P4.6 IcebergScan 执行

- [ ] `IcebergScan::execute()` 返回 RecordBatch
- [ ] 流程: plan → 遍历 data_files → 读取 Parquet → RowGroup 过滤 → 合并
- [ ] 列投影: 只读取 projection 指定的列
- [ ] `to_record_batches()` 同步版本正确
- [ ] `test_scan_execute_full` 通过
- [ ] `test_scan_execute_with_projection` 通过
- [ ] `test_scan_execute_with_predicate` 通过

### P4.7 P4 集成测试

- [ ] `test_scan_full_lifecycle` 通过
- [ ] `test_scan_predicate_pushdown_timestamp` 通过
- [ ] `test_scan_predicate_pushdown_tag` 通过
- [ ] `test_scan_projection` 通过
- [ ] `test_scan_three_level_filter` 通过
- [ ] `test_scan_empty_result` 通过

---

## P5: IcebergTableProvider (DataFusion) 验收

### P5.1 IcebergTableProvider 结构

- [ ] `IcebergTableProvider` 包含 table, schema
- [ ] `IcebergTableProvider::new()` 从 IcebergTable 创建成功
- [ ] Iceberg Schema → DataFusion Schema 转换正确
- [ ] `test_provider_creation` 通过
- [ ] `test_provider_schema` 通过

### P5.2 TableProvider trait 实现

- [ ] `as_any()` 实现正确
- [ ] `schema()` 返回正确 Schema
- [ ] `table_type()` 返回 Base
- [ ] `scan()` 将 filters 转为 Iceberg 谓词 → IcebergScan → IcebergScanExec
- [ ] `test_table_provider_scan` 通过

### P5.3 IcebergScanExec

- [ ] `IcebergScanExec` 包含 data_files, predicate, projection, schema, limit
- [ ] `ExecutionPlan` trait 实现完整: schema, children, with_new_children, execute
- [ ] `test_scan_exec_creation` 通过
- [ ] `test_scan_exec_schema` 通过

### P5.4 IcebergScanStream

- [ ] `IcebergScanStream` 实现 `SendableRecordBatchStream`
- [ ] 流式读取: 逐文件 → 逐 RowGroup 过滤 → yield RecordBatch
- [ ] 列投影支持
- [ ] limit 支持
- [ ] `test_scan_stream_single_file` 通过
- [ ] `test_scan_stream_multiple_files` 通过
- [ ] `test_scan_stream_with_limit` 通过

### P5.5 DataFusion filters → Iceberg 谓词

- [ ] `filters_to_predicate()` 转换正确
- [ ] 支持: Eq, Gt, GtEq, Lt, LtEq, InList, And, Or, Not
- [ ] `Predicate` 枚举定义完整
- [ ] `test_filter_to_predicate_eq` 通过
- [ ] `test_filter_to_predicate_range` 通过
- [ ] `test_filter_to_predicate_and_or` 通过

### P5.6 P5 集成测试

- [ ] `test_datafusion_sql_select` 通过
- [ ] `test_datafusion_sql_where_timestamp` 通过
- [ ] `test_datafusion_sql_where_tag` 通过
- [ ] `test_datafusion_sql_aggregation` 通过
- [ ] `test_datafusion_sql_group_by` 通过
- [ ] `test_datafusion_sql_limit` 通过
- [ ] `test_datafusion_predicate_pushdown` 通过

---

## P6: Time Travel 查询验收

### P6.1 scan_at_snapshot

- [ ] `scan_at_snapshot(snapshot_id)` 指定快照查询正确
- [ ] `scan_at_timestamp(timestamp_ms)` 指定时间点查询正确
- [ ] `snapshot_by_id()` / `snapshot_by_timestamp()` 查找正确
- [ ] `test_scan_at_snapshot` 通过
- [ ] `test_scan_at_timestamp` 通过

### P6.2 快照历史查询

- [ ] `history()` 返回 SnapshotLogEntry 列表
- [ ] `snapshot_diff()` 返回两个快照之间的差异
- [ ] 差异包含: added_files, deleted_files, added_records, deleted_records
- [ ] `test_history` 通过
- [ ] `test_snapshot_diff` 通过

### P6.3 Rollback

- [ ] `rollback_to_snapshot()` 回滚正确
- [ ] `rollback_to_timestamp()` 回滚正确
- [ ] Rollback 创建新 Snapshot 指向旧数据文件
- [ ] 原子提交更新 current_snapshot_id
- [ ] `test_rollback_to_snapshot` 通过
- [ ] `test_rollback_then_query` 通过

### P6.4 Branch/Tag 查询

- [ ] `scan_ref(ref_name)` 按引用名查询正确
- [ ] `create_branch()` 创建分支正确
- [ ] `create_tag()` 创建标签正确
- [ ] `test_scan_branch` 通过
- [ ] `test_scan_tag` 通过

### P6.5 P6 集成测试

- [ ] `test_time_travel_full` 通过
- [ ] `test_time_travel_by_timestamp` 通过
- [ ] `test_rollback_and_query` 通过
- [ ] `test_branch_tag_query` 通过
- [ ] `test_snapshot_diff` 通过

---

## P7: Compaction (Iceberg 风格) 验收

### P7.1 小文件选择策略

- [ ] `find_compaction_candidates()` 选择小文件正确
- [ ] 策略: file_size < target_file_size
- [ ] 按分区分组候选文件
- [ ] `test_find_compaction_candidates` 通过

### P7.2 文件合并

- [ ] `compact_partition()` 合并正确
- [ ] 读取小文件 → 合并 RecordBatch → 写入大文件
- [ ] 列统计收集 → DataFile 构建正确
- [ ] `test_compact_partition` 通过

### P7.3 Compaction 提交

- [ ] `IcebergTable::compact()` 实现完整
- [ ] 旧文件标记 DELETED，新文件标记 ADDED
- [ ] 新 Snapshot operation=replace
- [ ] 原子提交正确
- [ ] `test_compact_commit` 通过
- [ ] `test_compact_snapshot_operation` 通过

### P7.4 Compaction 后验证

- [ ] compact 后查询结果不变
- [ ] compact 后文件数减少
- [ ] compact 后总数据量不变
- [ ] `test_compact_query_consistency` 通过
- [ ] `test_compact_file_count_reduction` 通过

### P7.5 P7 集成测试

- [ ] `test_compact_full_lifecycle` 通过
- [ ] `test_compact_multiple_partitions` 通过
- [ ] `test_compact_with_concurrent_reads` 通过
- [ ] `test_compact_snapshot_history` 通过

---

## P8: Snapshot 过期清理验收

### P8.1 过期快照选择

- [ ] `expire_snapshots()` 选择过期快照正确
- [ ] 保留至少 min_snapshots 个快照
- [ ] 不删除被 branch/tag 引用的快照
- [ ] `test_expire_snapshots_by_age` 通过
- [ ] `test_expire_snapshots_min_retention` 通过

### P8.2 孤立文件清理

- [ ] `remove_orphan_files()` 清理孤立文件正确
- [ ] 扫描数据目录 → 对比有效快照引用 → 删除
- [ ] `test_remove_orphan_files` 通过

### P8.3 过期提交验证

- [ ] 过期后当前快照仍可查询
- [ ] 过期后快照历史正确截断
- [ ] 过期后 TableMetadata 更新正确
- [ ] `test_expire_then_query` 通过
- [ ] `test_expire_metadata_update` 通过

### P8.4 P8 集成测试

- [ ] `test_expire_snapshots_full` 通过
- [ ] `test_expire_with_branch_protection` 通过
- [ ] `test_orphan_file_cleanup` 通过
- [ ] `test_expire_then_time_travel` 通过

---

## P9: Schema 演化验收

### P9.1 SchemaChange 定义

- [ ] `SchemaChange` 枚举包含: AddField, DeleteField, RenameField, PromoteType, MoveField
- [ ] 每个变体字段完整
- [ ] `test_schema_change_variants` 通过

### P9.2 Schema 演化应用

- [ ] `Schema::apply_change()` 应用单个变更正确
- [ ] `Schema::apply_changes()` 应用多个变更正确
- [ ] 新 Schema 分配新 schema_id
- [ ] last_column_id 递增 (AddField)
- [ ] `test_apply_add_field` 通过
- [ ] `test_apply_delete_field` 通过
- [ ] `test_apply_rename_field` 通过
- [ ] `test_apply_promote_type` 通过

### P9.3 update_schema 实现

- [ ] `update_schema()` 应用变更 + 更新 Metadata + 原子提交
- [ ] `test_update_schema_commit` 通过
- [ ] `test_update_schema_history` 通过

### P9.4 Schema 演化后查询兼容

- [ ] Scan 根据 schema_id 选择正确 Schema
- [ ] 新增列: 旧文件返回 null
- [ ] 删除列: 旧文件忽略该列
- [ ] 重命名列: 旧名映射到新名
- [ ] `test_scan_after_add_field` 通过
- [ ] `test_scan_after_delete_field` 通过
- [ ] `test_scan_after_rename_field` 通过

### P9.5 P9 集成测试

- [ ] `test_schema_evolution_add_field` 通过
- [ ] `test_schema_evolution_delete_field` 通过
- [ ] `test_schema_evolution_rename_field` 通过
- [ ] `test_schema_evolution_promote_type` 通过
- [ ] `test_schema_evolution_multiple_changes` 通过

---

## P10: 分区演化验收

### P10.1 PartitionSpec 变更

- [ ] `PartitionSpec::evolve()` 演化正确
- [ ] `PartitionSpecChange` 枚举: AddField, RemoveField, RenameField
- [ ] 新 PartitionSpec 分配新 spec_id
- [ ] `test_partition_spec_evolve` 通过

### P10.2 update_partition_spec 实现

- [ ] `update_partition_spec()` 验证 + 更新 Metadata + 原子提交
- [ ] `test_update_partition_spec` 通过

### P10.3 分区演化后查询兼容

- [ ] Scan 根据 partition_spec_id 选择正确 PartitionSpec
- [ ] 旧分区数据可查询
- [ ] 新分区数据可查询
- [ ] `test_scan_after_partition_evolution` 通过

### P10.4 P10 集成测试

- [ ] `test_partition_evolution_full` 通过
- [ ] `test_partition_evolution_compact` 通过
- [ ] `test_partition_evolution_time_travel` 通过
- [ ] `test_partition_evolution_schema_compat` 通过

---

## 跨 Phase 验收

### 与现有系统集成

- [ ] tsdb-arrow: DataPoint ↔ Arrow RecordBatch 转换兼容
- [ ] tsdb-rocksdb: RocksDB CF 管理和 WriteBatch 兼容
- [ ] tsdb-parquet: Parquet 文件读写兼容
- [ ] tsdb-datafusion: IcebergTableProvider 注册和查询兼容
- [ ] tsdb-flight: Flight SQL 查询 Iceberg 表兼容

### 数据一致性

- [ ] 同一批数据通过 Iceberg 写入和直接 Parquet 写入，查询结果一致
- [ ] 多次 append 后查询结果与直接写入全部数据一致
- [ ] compact 后查询结果与 compact 前一致
- [ ] rollback 后查询结果与目标快照一致

### 性能基线

- [ ] Append 吞吐: ≥10K DataPoint/s (单线程)
- [ ] Scan 全表: 100K 行 < 1s
- [ ] 谓词下推: 跳过率 ≥50% (典型查询)
- [ ] Compaction: 10 个小文件合并 < 5s

---

## 最终发布检查清单

### 发布前必须全部 ✅

- [ ] `cargo test -p tsdb-iceberg` 全部通过 (0 failed)
- [ ] `cargo clippy -p tsdb-iceberg -- -D warnings` 零警告
- [ ] `cargo fmt -p tsdb-iceberg -- --check` 格式正确
- [ ] P1-P5 (P0) 全部验收项通过
- [ ] P6-P8 (P1) 验收项 ≥90% 通过
- [ ] P9-P10 (P2) 验收项 ≥85% 通过
- [ ] 数据一致性测试全部通过
- [ ] 性能基线达标
- [ ] 与现有系统集成测试通过
- [ ] 无 unsafe 代码 (或所有 unsafe 有安全注释)
