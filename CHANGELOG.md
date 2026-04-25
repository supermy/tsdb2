# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-04-25

### Added

- **Web 管理界面** (`tsdb-admin` + `tsdb-dashboard`)
  - NNG Admin Server: 基于 nanomsg-next-generation 的管理服务
  - axum HTTP Gateway: REST API + WebSocket 实时推送
  - React + Vite + Ant Design 前端，10 个功能页面
  - 暗色主题，响应式布局

- **Dashboard 页面**
  - 仪表盘: 服务概览、实时指标、健康状态
  - 服务管理: 创建/启停/重启服务、日志查看
  - 配置管理: 配置文件 CRUD、对比、应用
  - 功能测试: SQL 测试、写入/读取基准测试
  - 状态监控: CPU/内存/磁盘/网络实时图表、告警
  - 数据查询: 时间范围查询
  - RocksDB 管理: CF 统计、KV 扫描、序列 Schema
  - Parquet 查看: 文件列表、元数据、数据预览
  - SQL 控制台: SQL 编辑器、结果表格、执行历史、示例 SQL
  - 数据生命周期: 热温冷分布、归档操作

- **系统指标采集** (`MetricsCollector`)
  - CPU 使用率、负载、温度
  - 内存使用率、总量、已用
  - 磁盘使用率、总量、已用
  - 网络收发字节数
  - GPU 温度
  - 自动写入 RocksDB 作为时序数据 (sys_cpu, sys_memory, sys_disk, sys_network, sys_temp)
  - WebSocket 实时推送

- **Parquet 数据查看 API** (`ParquetApi`)
  - 列出 archive/ 和 parquet_data/ 目录下的 Parquet 文件
  - 读取文件元数据（行数、列数、压缩方式、列类型）
  - 数据预览（前 N 行，JSON 格式）

- **SQL 执行 API** (`SqlApi`)
  - 直接从 RocksDB 读取数据，通过 DataFusion 执行 SQL
  - 支持 SELECT / WHERE / GROUP BY / ORDER BY / LIMIT / 聚合函数
  - 支持 SHOW TABLES（启用 information_schema）
  - Timestamp 列自动格式化为可读日期时间
  - 结果以 JSON 格式返回，最多 1000 行

- **数据生命周期管理 API** (`LifecycleApi`)
  - 基于 CF 日期分区自动分类热/温/冷数据
  - 热数据 (≤3天): RocksDB 内存 + SST, SNAPPY 压缩
  - 温数据 (4-14天): RocksDB SST only, 降级压缩
  - 冷数据 (>14天): 归档为 Parquet, ZSTD 压缩
  - 手动归档操作: 将指定天数前的数据导出为 Parquet

- **RocksDB 管理 API**
  - 统计概览: CF 数量、SST 大小、键数
  - CF 详情: 每层文件数、memtable 大小、block cache
  - KV 扫描: 指定 CF 和前缀扫描键值对
  - KV 获取: 指定 CF 和键获取值
  - 序列 Schema: 显示 measurement → tags → fields 映射
  - 手动压缩

- **运维脚本**
  - `scripts/build.sh`: 完整构建（后端 + 前端）
  - `scripts/start.sh`: 生产模式启动
  - `scripts/dev.sh`: 开发模式（前后端热更新）
  - `scripts/check.sh`: 代码质量检查

### Changed

- HTTP Dashboard 端口从 3000 改为 8902
- 默认配置从 `default` 改为 `balanced`
- 构建流程增加前端产物同步到 `target/release/dashboard/`

### Fixed

- NNG Req0 并发竞态: 使用 Mutex 序列化 + spawn_blocking
- 路径遍历漏洞: 添加名称验证
- CORS 跨域: 添加 CorsLayer
- Gateway 启动时序: 拆分 AdminServer bind/run
- RocksDB LOCK 文件冲突: 进程清理 + LOCK 删除
- Parquet 元数据 API 兼容: `file_meta` → `parquet_meta`
- SQL API 直接查询 RocksDB: 从依赖 Parquet 文件改为直接读取
- Timestamp Arrow 数组 downcast: 按 TimeUnit 分别处理
- 前端路由缺失: 注册 Parquet/SQL/Lifecycle 三个页面

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
