# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-04-22

### Added

- **Arrow Flight SQL 服务端** (`tsdb-flight` crate)
  - `do_get`: SQL 查询通过 Flight 协议流式返回 FlightData
  - `do_put`: FlightData 流式写入到 RocksDB
  - `do_action("list_measurements")`: 列出所有 measurement
  - `get_flight_info` / `get_schema` / `poll_flight_info`: 查询元数据
  - `with_rocksdb()`: 自动注册已有 measurement 表

- **列投影下推**: `read_range_projection()` 只解码指定字段，减少内存和 I/O
- **谓词下推**: DataFusion TableProvider 时间范围过滤下推到存储层
- **Limit 下推**: 扫描层直接截断数据，避免读取多余行
- **INI 配置策略**: 5 种预设配置 (balanced/write-heavy/read-heavy/memory-limited/iot)
- **DataArchiver**: RocksDB 过期 CF → JSONL → Parquet 冷数据归档
- **业务场景测试**: IoT 监控 (8)、金融行情 (8)、运维 APM (9) 共 25 个端到端测试
- **Schema Builder**: `TsdbSchemaBuilder` 链式构建扩展/紧凑模式 Schema
- **time_bucket UDF**: DataFusion 自定义标量函数，按时间窗口分桶聚合
- **CPU 特性检测**: 运行时检测 AVX2/SSE4.2/NEON，自动优化 RocksDB 参数

### Changed

- **WAL 文件句柄常开**: `TsdbWAL` 使用 `BufWriter<File>` 常驻句柄，避免每次 append 重新打开文件
- **Compaction 原子替换**: 使用临时文件 + `rename()` 原子替换，防止崩溃后数据重复
- **CLI query 懒加载**: 查询范围从 365 天缩减为 7 天，按 measurement 分组加载
- **Flight 自动注册表**: `with_rocksdb()` 创建时自动扫描 CF 并注册 measurement 表
- **分层压缩策略**: L0-L2 无压缩 (热点) + L3+ Zstd (冷数据)

### Fixed

- CSV 导出/导入 Bug: 手动构建 CSV 表头和行数据，tags 展开为 `tag_xxx` 列
- FieldValue 反序列化: 自定义 `Deserialize` 同时支持裸值和标签对象两种 JSON 格式
- Arrow Flight 54 API 兼容: 使用 `batches_to_flight_data()` 替代已移除的 API

### Performance

- WAL append: BufWriter 常驻句柄，性能提升约 10x
- MultiGET: RocksDB `multi_get_cf` 批量点查，比逐个 get 快 3.3x
- WriteBatch Tags 去重: 同 series 只写一次 meta，写入 QPS 提升 1.75x
- 列投影读取: 只解码查询所需字段，QPS ~250K pts/s

### Test Coverage

- 总测试数: 360 (v0.1.0: 151)
- 单元测试: 228
- 属性测试: 11
- 集成测试: 65
- 业务测试: 27
- 压力测试: 22
- CLI 测试: 7

## [0.1.0] - 2026-04-18

### Added

- RocksDB 存储引擎: CF 按日期分区、三级 TTL 清理、Merge 字段合并
- Parquet 存储引擎: 日期分区、hot/cold 编码、WAL 预写日志
- Arrow 数据模型: DataPoint ↔ RecordBatch 双向转换
- DataFusion SQL 查询引擎: SELECT / WHERE / GROUP BY / 聚合
- CLI 工具: status / query / bench / compact / import / archive / export / doctor
- 一致性快照: 基于 RocksDB Snapshot 的 MVCC 读取
- 多平台支持: Linux / macOS / Windows
- CI/CD: GitHub Actions (check/test/clippy/fmt/audit/coverage/bench/release)
