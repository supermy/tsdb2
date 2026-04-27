# TSDB2 Parquet 存储优化 — 验收清单

## Phase 1: Schema 与写入优化

- [ ] **1.1** compact schema 包含 tags_hash 列，列顺序为 timestamp → tags_hash → tag_* → field_*
- [ ] **1.2** `datapoints_to_record_batch()` 正确计算并填充 tags_hash 值
- [ ] **1.3** tags_hash 值与 RocksDB `compute_tags_hash()` 结果一致
- [ ] **1.4** 写入前 RecordBatch 按 (tags_hash ASC, timestamp ASC) 排序
- [ ] **1.5** Parquet 文件元数据包含 `sorting_columns` 信息
- [ ] **1.6** Parquet 文件包含 Column Index（column_index 字段非空）
- [ ] **1.7** Parquet 文件包含 Offset Index（offset_index 字段非空）
- [ ] **1.8** Parquet 文件包含页级统计信息（PageStatistics）
- [ ] **1.9** 新目录结构 `{tier}/{measurement}/{YYYYMMDD}/` 正确生成
- [ ] **1.10** 旧目录结构文件仍可正确读取（向后兼容）
- [ ] **1.11** RocksDB 导出的 Parquet 文件包含排序和索引
- [ ] **1.12** `cargo test -p tsdb-arrow` 全部通过
- [ ] **1.13** `cargo test -p tsdb-parquet` 全部通过
- [ ] **1.14** `cargo test -p tsdb-rocksdb` 全部通过

## Phase 2: 文件统计信息与剪枝

- [ ] **2.1** `FileStats` 结构体包含 timestamp_min/max, tags_hash_min/max, tag_values 统计
- [ ] **2.2** `extract_file_stats()` 正确从 Parquet 元数据提取统计信息
- [ ] **2.3** sidecar `.stats.json` 文件正确生成和读取
- [ ] **2.4** `_manifest.json` 清单文件正确生成和更新
- [ ] **2.5** `prune_files()` 正确基于时间范围剪枝
- [ ] **2.6** `prune_files()` 正确基于标签值范围剪枝
- [ ] **2.7** `prune_row_groups()` 正确基于行组统计信息剪枝
- [ ] **2.8** 写入 Parquet 后自动生成 sidecar 和清单文件
- [ ] **2.9** `cargo test -p tsdb-parquet` 全部通过

## Phase 3: 读取优化

- [ ] **3.1** `read_range_arrow()` 使用文件统计信息剪枝，跳过不相关文件
- [ ] **3.2** `read_parquet_file()` 使用行组统计信息跳过不相关行组
- [ ] **3.3** `read_parquet_file()` 使用 RowFilter 进行谓词下推
- [ ] **3.4** `read_parquet_file()` 支持列投影，只读取需要的列
- [ ] **3.5** `extract_filters()` 正确解析 `tag_{key} = 'value'` 过滤
- [ ] **3.6** `extract_filters()` 正确解析 `timestamp BETWEEN` 范围
- [ ] **3.7** `extract_filters()` 正确解析 AND 组合条件
- [ ] **3.8** `TsdbTableProvider::scan()` 使用文件剪枝
- [ ] **3.9** `TsdbTableProvider::scan()` 使用谓词下推
- [ ] **3.10** 旧格式文件（无索引）回退到全量读取，不报错
- [ ] **3.11** `cargo test -p tsdb-datafusion` 全部通过
- [ ] **3.12** `cargo test -p tsdb-admin` 全部通过

## Phase 4: DataFusion 原生集成（可选）

- [ ] **4.1** `SessionContext` 注册 `LocalFileSystem` ObjectStore
- [ ] **4.2** `TsdbTableProvider` 使用 `ListingTable` 替代 `MemTable`
- [ ] **4.3** SQL 查询结果与优化前一致
- [ ] **4.4** SQL 查询性能显著提升（时间范围查询 < 100ms）
- [ ] **4.5** `SqlApi` 正确处理 RocksDB + Parquet 数据去重

## Phase 5: Compaction 与迁移

- [ ] **5.1** Compaction 输出文件保持 (tags_hash, timestamp) 排序
- [ ] **5.2** Compaction 输出文件包含 Column Index + Offset Index
- [ ] **5.3** Compaction 后 sidecar 和清单文件正确更新
- [ ] **5.4** `migrate_parquet_file()` 正确将旧格式转换为新格式
- [ ] **5.5** 迁移后查询结果与迁移前一致
- [ ] **5.6** `make parquet-upgrade` target 正常工作
- [ ] **5.7** `make parquet-stats` target 正常工作
- [ ] **5.8** CI 流水线全部通过

## 性能验收标准

- [ ] **P1** 时间范围查询（1天/30天数据）：文件读取量减少 > 90%
- [ ] **P2** 标签过滤查询：行组/页级剪枝跳过 > 80% 数据
- [ ] **P3** 列投影查询（2列/20列）：I/O 减少约 90%
- [ ] **P4** 单次查询内存峰值降低 > 50%

## 整体验收

- [ ] **E1** `cargo clippy --workspace --all-targets -- -D warnings` 无警告
- [ ] **E2** `cargo fmt --all -- --check` 格式检查通过
- [ ] **E3** `cargo test --workspace` 全部通过
- [ ] **E4** 前端 SQL 控制台查询正常
- [ ] **E5** 前端 Parquet 查看器正常
- [ ] **E6** 数据生命周期降级正常
- [ ] **E7** `make db-info` 显示正确的统计信息
