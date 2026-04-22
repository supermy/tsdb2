# TSDB2 实施任务清单

> 基于 spec.md 规格，按优先级和依赖关系排列

---

## 任务总览

| Phase          | 名称           | 任务数       | 优先级 | 依赖 |
| -------------- | -------------- | ------------ | ------ | ---- |
| T1             | 单元测试增强   | 6            | P0     | 无   |
| T2             | 压力测试增强   | 4            | P1     | T1   |
| T3             | 集成测试增强   | 3            | P0     | T1   |
| T4             | 真实数据生成器 | 3            | P1     | 无   |
| T5             | CLI 子命令实现 | 6            | P0     | T1   |
| T6             | CLI 测试覆盖   | 3            | P1     | T5   |
| T7             | 多平台兼容性   | 3            | P2     | T1   |
| T8             | CI/CD 增强     | 4            | P1     | T1   |
| T9             | Flight SQL 集成| 4            | P0     | T1   |
| **合计** |                | **36** |        |      |

---

## T1: 单元测试增强 [P0]

### T1.1 merge.rs 独立单元测试

**文件**: `crates/tsdb-rocksdb/src/merge.rs`

当前状态: 0 个独立测试，merge 仅在 db.rs 的集成测试中覆盖

- [ ] `test_full_merge_single_operand`: full_merge(空 existing, [op1]) == op1
- [ ] `test_full_merge_multiple_operands`: full_merge(existing, [op1, op2]) 字段 union 正确
- [ ] `test_full_merge_field_override`: 同名字段后者覆盖前者
- [ ] `test_full_merge_5_plus_operands`: 5+ operands 合并结果与逐次合并一致
- [ ] `test_full_merge_empty_operands`: 空 operands 不 panic
- [ ] `test_partial_merge_returns_none`: partial_merge 始终返回 None (安全降级)
- [ ] `test_full_merge_empty_fields_union`: 空字段集合并

- **验收**: 覆盖率 ≥85%，clippy 零警告

### T1.2 snapshot.rs 独立单元测试

**文件**: `crates/tsdb-rocksdb/src/snapshot.rs`

当前状态: 0 个独立测试，仅在 db.rs 的 test_db_snapshot_consistency 中覆盖

- [ ] `test_snapshot_get_after_write_invisible`: snapshot 创建后新写入不可见
- [ ] `test_snapshot_read_range_consistency`: snapshot 读取与 DB 直接读取在创建时刻一致
- [ ] `test_snapshot_multiple_independent`: 多个 snapshot 互不影响
- [ ] `test_snapshot_cross_cf_read`: 跨 CF Snapshot 读取

- **验收**: 覆盖率 ≥85%

### T1.3 error.rs 全路径覆盖

**文件**: `crates/tsdb-rocksdb/src/error.rs`

当前状态: 0 个测试

- [ ] `test_invalid_key_construction`: InvalidKey 变体构造和 Display
- [ ] `test_invalid_value_construction`: InvalidValue 变体构造和 Display
- [ ] `test_cf_not_found_construction`: CfNotFound 变体
- [ ] `test_arrow_conversion_error_chain`: From`<TsdbArrowError>` 正确映射
- [ ] `test_error_source_chain`: 错误链可通过 source() 追溯

- **验收**: 覆盖率 ≥90%

### T1.4 key.rs 属性测试 (proptest)

**文件**: `crates/tsdb-rocksdb/src/key.rs`

当前状态: 8 个单元测试，缺少属性测试

- [ ] 添加 `proptest` 开发依赖到 Cargo.toml
- [ ] `proptest_key_encode_decode_roundtrip`: decode(encode(k)) == k for all k
- [ ] `proptest_key_ordering_invariant`: encode(a) < encode(b) iff a < b
- [ ] `proptest_prefix_encode_is_prefix`: prefix_encode(h) 是 encode(h, t) 的前缀
- [ ] `proptest_key_size_constant`: 所有合法 key 编码后长度 = 16

- **验收**: proptest 默认 256 cases 全部通过

### T1.5 value.rs 属性测试 (proptest)

**文件**: `crates/tsdb-rocksdb/src/value.rs`

当前状态: 8 个单元测试，缺少属性测试

- [ ] `proptest_value_encode_decode_roundtrip`: decode(encode(f)) == f for all f
- [ ] `proptest_value_all_field_types`: Float64/Int64/Utf8/Boolean 全类型组合
- [ ] `proptest_value_special_floats`: NaN/Inf/-Inf/0.0/-0.0 编解码保留语义

- **验收**: proptest 默认 256 cases 全部通过

### T1.6 comparator.rs 不变量测试

**文件**: `crates/tsdb-rocksdb/src/comparator.rs`

当前状态: 5 个单元测试

- [ ] `test_comparator_transitivity`: a<b && b<c → a<c (10000 组随机数据)
- [ ] `test_comparator_antisymmetry`: a<b → !(b<a) for all a≠b
- [ ] `test_comparator_reflexivity`: !(a<a) for all a
- [ ] `test_comparator_different_length_keys`: 不同长度 key 比较安全

- **验收**: 覆盖率 ≥95%

---

## T2: 压力测试增强 [P1]

### T2.1 写入吞吐增强

**文件**: `crates/tsdb-stress-rocksdb/src/write_stress.rs`

当前状态: 3 个测试 (100k single, 100k batch, multi_measurement)

- [ ] `stress_rocksdb_write_1M_single_thread`: 1M points, 阈值 >40K pts/s
- [ ] `stress_rocksdb_write_batch_sizes`: 对比 batch=1/100/1000/10000 吞吐差异
- [ ] 添加 StressTestReport 结构化输出 (JSON)

- **验收**: 1M 版本在 CI 中 <120s

### T2.2 并发读写增强

**文件**: `crates/tsdb-stress-rocksdb/src/concurrent_stress.rs`

当前状态: 3 个测试 (4_threads, merge_same_key, mixed_read_write)

- [ ] `stress_concurrent_write_8_threads`: 8 线程各写 125K, 无数据丢失
- [ ] `stress_concurrent_snapshot_consistency`: 写入中创建 snapshot, 快照一致性 100%

- **验收**: 无 panic, 无数据丢失

### T2.3 读取性能增强

**文件**: `crates/tsdb-stress-rocksdb/src/read_stress.rs`

当前状态: 3 个测试 (range_scan_100k, prefix_scan_100, cross_day_7days)

- [ ] `stress_rocksdb_batched_multi_get`: 1000 keys 批量查询, P99 < 20ms
- [ ] `stress_rocksdb_cross_day_scan_30days`: 30 天跨日期扫描无退化

- **验收**: 性能不退化超过 20%

### T2.4 长时间稳定性测试 (新增)

**新建文件**: `crates/tsdb-stress-rocksdb/src/stability_stress.rs`

- [ ] `stress_long_running_5min`: 5 分钟持续写入, 无内存泄漏
- [ ] `stress_long_running_cf_creation`: 持续创建新 CF (模拟新日期), 无退化
- [ ] 在 lib.rs 中添加 `pub mod stability_stress;`

- **验收**: 内存稳定, 无泄漏趋势

---

## T3: 集成测试增强 [P0]

### T3.1 DataFusion + RocksDB SQL 集成

**文件**: `crates/tsdb-integration-tests/src/rocksdb_e2e.rs`

当前状态: 10 个测试，缺少 SQL 查询 RocksDB 数据的测试

- [ ] `test_e2e_rocksdb_sql_select`: 通过 DataFusion SQL 查询 RocksDB 数据
- [ ] `test_e2e_rocksdb_sql_aggregation`: AVG/SUM/COUNT/MIN/MAX 聚合正确
- [ ] `test_e2e_rocksdb_sql_where_tag`: WHERE tag 条件过滤正确

- **实现路径**: TsdbRocksDb.read_range() → ArrowAdapter.read_range_arrow() → DataFusionQueryEngine.register_from_datapoints() → execute SQL
- **验收**: SQL 查询结果与手动计算一致

### T3.2 引擎对比测试

**文件**: `crates/tsdb-integration-tests/src/rocksdb_e2e.rs`

- [ ] `test_e2e_parquet_vs_rocksdb_consistency`: 同一批数据分别写入两者，查询结果一致
- [ ] `test_e2e_rocksdb_performance_baseline`: 记录 RocksDB E2E 性能基线

- **验收**: Parquet vs RocksDB 查询结果完全一致

### T3.3 Arrow 适配空结果测试

**文件**: `crates/tsdb-integration-tests/src/rocksdb_e2e.rs`

- [ ] `test_e2e_rocksdb_empty_result_arrow`: 空结果 → 空 RecordBatch (schema 正确, num_rows=0)

- **验收**: 空 RecordBatch 的 schema 与非空结果一致

---

## T4: 真实数据生成器 [P1]

### T4.1 DevOps 数据生成器

**文件**: `crates/tsdb-test-utils/src/data_gen.rs`

- [ ] `make_devops_datapoints_enhanced`: 支持 hosts/ranges/measurements 可配置
- [ ] 支持周期性模式 (白天高、夜间低)
- [ ] 支持异常注入 (spike/dropout/gap)

- **参数**: device_count, hours, measurements, anomaly_rate
- **验收**: 生成的 Tags/Fields/DataPoint 结构合法

### T4.2 IoT 数据生成器

**文件**: `crates/tsdb-test-utils/src/data_gen.rs`

- [ ] `make_iot_datapoints`: 高频传感器数据
- [ ] 支持设备分组 (区域/类型)
- [ ] 支持离线模拟 (随机 gap)

- **参数**: devices, duration_days, offline_rate
- **验收**: 大规模 (100K devices) 不 OOM

### T4.3 金融 K线生成器

**文件**: `crates/tsdb-test-utils/src/data_gen.rs`

- [ ] `make_financial_ohlcv`: OHLCV K线数据
- [ ] 支持几何布朗运动价格模型
- [ ] 时间戳对齐到 bar interval

- **参数**: tickers, years, bar_interval, volatility
- **验收**: high >= low, open/close 在 [low, high]

---

## T5: CLI 子命令实现 [P0]

### T5.1 status 子命令

**文件**: `crates/tsdb-cli/src/main.rs`

当前状态: 占位 println

- [ ] 实现 `execute_status(data_dir, storage_engine)`:
  - 打开 TsdbRocksDb
  - 调用 `list_ts_cfs()` + `stats()` + `cf_stats()`
  - 格式化输出到终端
- [ ] 添加 `--storage-engine` 参数支持 (rocksdb / parquet)

- **验收**: `tsdb-cli status --data-dir ./data` 输出格式化状态信息

### T5.2 query 子命令

**文件**: `crates/tsdb-cli/src/main.rs`

当前状态: 占位 println

- [ ] 实现 `execute_query(data_dir, sql, format)`:
  - 打开 TsdbRocksDb
  - 读取所有数据 → ArrowAdapter → RecordBatch
  - 注册到 DataFusionQueryEngine → execute SQL
  - 输出格式: json (默认) / table / csv
- [ ] 添加 `--sql` 参数
- [ ] 添加 `--format` 参数 (json/table/csv)

- **验收**: `tsdb-cli query --sql "SELECT * FROM cpu"` 返回正确结果

### T5.3 compact 子命令

**文件**: `crates/tsdb-cli/src/main.rs`

当前状态: 占位 println

- [ ] 实现 `execute_compact(data_dir, cf_name)`:
  - 打开 TsdbRocksDb
  - 如果指定 `--cf`: compact_cf(cf_name)
  - 否则: 遍历 list_ts_cfs() 逐个 compact
- [ ] 添加 `--cf` 可选参数

- **验收**: `tsdb-cli compact --data-dir ./data` 成功执行

### T5.4 bench 子命令增强

**文件**: `crates/tsdb-cli/src/main.rs`

当前状态: 有基础 `run_bench()` 实现

- [ ] 增强 `run_bench()`:
  - write 模式: 生成数据 → write_batch → 报告 ops/sec
  - read 模式: 写入数据 → range_scan → 报告 latency
  - 输出 JSON 报告
- [ ] 添加 `--points` 参数 (默认 100000)
- [ ] 添加 `--workers` 参数 (默认 1)

- **验收**: `tsdb-cli bench write --points 100000` 输出性能报告

### T5.5 import 子命令

**文件**: `crates/tsdb-cli/src/main.rs`

当前状态: 占位 println

- [ ] 实现 JSON 格式导入:
  - 解析 JSON 文件 → Vec`<DataPoint>`
  - write_batch() 批量写入
- [ ] 实现 Line Protocol 格式导入 (可选, P2)
- [ ] 实现 CSV 格式导入 (可选, P2)
- [ ] 添加 `--file` 参数
- [ ] 添加 `--format` 参数 (json/lp/csv)
- [ ] 添加 `--batch-size` 参数 (默认 10000)

- **验收**: `tsdb-cli import --file test.json --format json` 成功导入

### T5.6 archive 子命令

**文件**: `crates/tsdb-cli/src/main.rs`

当前状态: 占位 println

- [ ] 实现 `archive create`: 遍历过期 CF → 导出 SST 文件
- [ ] 实现 `archive restore`: 从 SST 文件恢复
- [ ] 实现 `archive list`: 列出归档目录中的文件
- [ ] 添加 `--older-than` 参数 (默认 30d)
- [ ] 添加 `--output-dir` 参数

- **验收**: archive create → archive list → archive restore 全链路

---

## T6: CLI 测试覆盖 [P1]

### T6.1 CLI 集成测试

**新建文件**: `crates/tsdb-cli/tests/cli_tests.rs`

- [ ] `test_cli_status_output`: status 子命令输出包含 "Column Families"
- [ ] `test_cli_query_output`: query 子命令输出 JSON 格式
- [ ] `test_cli_compact_success`: compact 子命令成功执行
- [ ] `test_cli_bench_write_output`: bench write 输出性能报告
- [ ] `test_cli_import_json`: import JSON 文件成功

- **验收**: 所有 CLI 测试通过

### T6.2 CLI 参数解析测试

**文件**: `crates/tsdb-cli/tests/cli_tests.rs`

- [ ] `test_cli_help_output`: `--help` 显示所有子命令
- [ ] `test_cli_version_output`: `--version` 显示版本号
- [ ] `test_cli_missing_data_dir`: 缺少 --data-dir 报错
- [ ] `test_cli_unknown_subcommand`: 未知子命令报错

- **验收**: 错误消息友好

### T6.3 CLI 配置加载测试

**文件**: `crates/tsdb-cli/tests/cli_tests.rs`

- [ ] `test_cli_config_file_loading`: tsdb.toml 加载成功
- [ ] `test_cli_default_config`: 无配置文件时使用默认值

- **验收**: 配置加载正确

---

## T7: 多平台兼容性 [P2]

### T7.1 CPU 特性检测模块

**新建文件**: `crates/tsdb-rocksdb/src/cpu_features.rs`

- [ ] `detect_cpu_features()`: 运行时检测 AVX2/NEON/SSE4.2
- [ ] `optimized_config()`: 根据 CPU 特性返回最优 RocksDbConfig
- [ ] ARM: 较小 block_size (4KB), 较大 write_buffer
- [ ] x86_64: 启用 SIMD 友好选项
- [ ] 在 lib.rs 中添加 `pub mod cpu_features;`

- **验收**: 在 x86_64 和 aarch64 上均能正常工作

### T7.2 RISC-V cross-compile CI

**文件**: `.github/workflows/ci.yml`

- [ ] 添加 riscv64gc-unknown-linux-gnu nightly check job
- [ ] 识别并修复 RISC-V 不兼容代码 (如有)

- **验收**: nightly 编译通过 (允许部分失败)

### T7.3 Docker 多架构构建

**新建文件**: `Dockerfile` + `docker-bake.hcl`

- [ ] 多阶段构建 (build + runtime)
- [ ] 支持: linux/amd64, linux/arm64, linux/arm/v7

- **验收**: `docker buildx bake` 成功构建 3 个架构

---

## T8: CI/CD 增强 [P1]

### T8.1 覆盖率阈值门禁

**文件**: `.github/workflows/ci.yml`

当前状态: coverage job 存在但无 `--fail-under` 阈值

- [ ] 添加 `--fail-under 80` 到 tarpaulin 命令
- [ ] 排除 tsdb-stress-rocksdb
- [ ] 上传 Codecov

- **验收**: 覆盖率 <80% 时 CI 失败

### T8.2 性能回归检测

**新建文件**: `.github/workflows/bench.yml`

- [ ] criterion benchmark baseline 存储
- [ ] PR vs main 性能对比
- [ ] 回归阈值: >10% warning, >25% fail

- **验收**: 主要基准测试有回归保护

### T8.3 压力测试 Gate

**新建文件**: `.github/workflows/stress.yml`

- [ ] 仅 main 分支 push 时运行
- [ ] 运行 tsdb-stress-rocksdb 测试
- [ ] 超时限制: 每个 stress test ≤10min
- [ ] 结果存档为 artifact

- **验收**: main 分支 push 自动运行压力测试

### T8.4 Dependabot + 安全审计

**新建文件**: `.github/dependabot.yml`

- [ ] weekly 依赖更新 PR
- [ ] 保持已有 audit-check job

- **验收**: 安全漏洞自动检测

---

## T9: Flight SQL 集成 [P0]

### T9.1 Flight SQL 服务端完善

**文件**: `crates/tsdb-flight/src/server.rs`

当前状态: ✅ 已实现 do_get/do_put/do_action/list_actions/get_flight_info/get_schema/poll_flight_info

- [x] `TsdbFlightServer::new()`: 创建 Flight 服务端
- [x] `do_get`: SQL 查询 → Arrow Flight Data 流式返回
- [x] `do_put`: Arrow Flight Data 流式写入 → RocksDB
- [x] `do_action("list_measurements")`: 列出所有 measurement
- [x] `list_actions`: 返回支持的 action 列表
- [x] `get_flight_info`: 返回查询的 FlightInfo (schema + endpoint)
- [x] `get_schema`: 返回查询的 SchemaResult
- [x] `poll_flight_info`: 长查询轮询

- **验收**: 所有 Flight RPC 方法可调用, Arrow IPC 编解码正确

### T9.2 TsdbTableProvider 优化

**文件**: `crates/tsdb-datafusion/src/table_provider.rs`

当前状态: ✅ 已实现谓词下推+列投影+Limit下推+Parquet原生扫描

- [x] 列投影下推: 只读取查询所需的列
- [x] 谓词下推: timestamp 范围条件下推到 Parquet 扫描层
- [x] Limit 下推: 在扫描层直接截断数据
- [x] Parquet 原生扫描: 直接读取 Parquet 为 RecordBatch, 避免 DataPoint 中间转换
- [x] `extract_timestamp_range()`: 从 DataFusion Expr 中提取时间范围

- **验收**: 查询性能相比无优化版本提升 3x+

### T9.3 Flight E2E 集成测试

**文件**: `crates/tsdb-integration-tests/src/flight_e2e.rs`

当前状态: ✅ 18 个测试全部通过

- [x] `test_flight_server_creation`: 服务端创建
- [x] `test_flight_server_with_rocksdb`: RocksDB 集成
- [x] `test_flight_list_actions`: list_actions RPC
- [x] `test_flight_do_get_sql_query`: SQL 查询 via do_get
- [x] `test_flight_get_flight_info`: get_flight_info RPC
- [x] `test_flight_get_schema`: get_schema RPC
- [x] `test_flight_do_action_list_measurements`: list_measurements action
- [x] `test_datafusion_sql_aggregation`: DataFusion 聚合查询
- [x] `test_datafusion_sql_filter`: DataFusion 过滤查询
- [x] `test_datafusion_sql_group_by`: DataFusion 分组查询
- [x] `test_datafusion_sql_limit`: DataFusion LIMIT 查询
- [x] `test_arrow_roundtrip`: Arrow 编解码往返
- [x] `test_arrow_projection`: Arrow 列投影
- [x] `test_parquet_write_read_roundtrip`: Parquet 读写往返
- [x] `test_parquet_range_read`: Parquet 范围查询
- [x] `test_rocksdb_write_and_query`: RocksDB 写入+查询
- [x] `test_rocksdb_multi_get`: RocksDB 批量查询
- [x] `test_full_pipeline_rocksdb_to_datafusion`: 全链路: RocksDB→Parquet→DataFusion

- **验收**: 18 个测试全部通过

### T9.4 Flight gRPC 端到端测试

**文件**: `crates/tsdb-integration-tests/src/flight_grpc_e2e.rs`

当前状态: 待实现

- [ ] 启动 tonic gRPC 服务器
- [ ] 使用 Arrow Flight 客户端连接
- [ ] 测试 do_get SQL 查询
- [ ] 测试 do_put 数据写入
- [ ] 测试 get_flight_info 元数据查询
- [ ] 测试 do_action list_measurements

- **验收**: gRPC 端到端测试通过

---

## 执行顺序

```
T1 (单元测试增强)
├── T1.1 merge.rs 测试
├── T1.2 snapshot.rs 测试
├── T1.3 error.rs 测试
├── T1.4 key.rs proptest
├── T1.5 value.rs proptest
└── T1.6 comparator.rs 不变量
     │
     ├──▶ T2 (压力测试增强)
     │    ├── T2.1 写入吞吐
     │    ├── T2.2 并发读写
     │    ├── T2.3 读取性能
     │    └── T2.4 长时间稳定性
     │
     ├──▶ T3 (集成测试增强)
     │    ├── T3.1 SQL 集成
     │    ├── T3.2 引擎对比
     │    └── T3.3 空结果测试
     │
     ├──▶ T9 (Flight SQL 集成) ✅ 大部分完成
     │    ├── T9.1 Flight SQL 服务端 ✅
     │    ├── T9.2 TsdbTableProvider 优化 ✅
     │    ├── T9.3 Flight E2E 测试 ✅
     │    └── T9.4 Flight gRPC E2E (待实现)
     │
     ├──▶ T5 (CLI 子命令实现)
     │    ├── T5.1 status
     │    ├── T5.2 query
     │    ├── T5.3 compact
     │    ├── T5.4 bench 增强
     │    ├── T5.5 import
     │    └── T5.6 archive
     │         │
     │         └──▶ T6 (CLI 测试覆盖)
     │              ├── T6.1 集成测试
     │              ├── T6.2 参数解析测试
     │              └── T6.3 配置加载测试
     │
     └──▶ T8 (CI/CD 增强)
          ├── T8.1 覆盖率门禁
          ├── T8.2 性能回归
          ├── T8.3 压力测试 Gate
          └── T8.4 Dependabot

T4 (真实数据生成器) — 独立，可与 T1 并行
T7 (多平台兼容性) — 低优先级，T1 完成后启动
```

---

## 辅助函数提取 (跨任务)

stress-rocksdb 各模块存在大量重复辅助函数，应提取到 tsdb-test-utils:

**文件**: `crates/tsdb-test-utils/src/data_gen.rs`

- [ ] 提取 `make_tags()`, `make_fields()`, `ts_today()`, `ts_days_ago()` 到 tsdb-test-utils
- [ ] 各 stress 模块改为 `use tsdb_test_utils::*`
- [ ] 删除各模块中的重复辅助函数

- **验收**: stress 测试仍全部通过
