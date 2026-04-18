# TSDB2 验收检查清单

> 基于 spec.md 规格和 tasks.md 任务，逐项验收

---

## 验收总览

| 类别 | 检查项 | 通过门槛 | 当前状态 |
|------|--------|---------|---------|
| T1 单元测试增强 | 30 | ≥28 (93%) | ⏳ 待验证 |
| T2 压力测试增强 | 10 | ≥9 (90%) | ⏳ 待验证 |
| T3 集成测试增强 | 6 | ≥6 (100%) | ⏳ 待验证 |
| T4 真实数据生成器 | 9 | ≥8 (89%) | ⏳ 待验证 |
| T5 CLI 子命令实现 | 12 | ≥11 (92%) | ⏳ 待验证 |
| T6 CLI 测试覆盖 | 11 | ≥10 (91%) | ⏳ 待验证 |
| T7 多平台兼容性 | 8 | ≥7 (87%) | ⏳ 待验证 |
| T8 CI/CD 增强 | 10 | ≥9 (90%) | ⏳ 待验证 |
| **合计** | **96** | **≥88 (91%)** | **⏳** |

---

## T1: 单元测试增强验收

### T1.1 merge.rs 独立测试

- [ ] `test_full_merge_single_operand`: full_merge(空, [op1]) == op1
- [ ] `test_full_merge_multiple_operands`: 字段 union 正确
- [ ] `test_full_merge_field_override`: 同名字段后者覆盖
- [ ] `test_full_merge_5_plus_operands`: 5+ operands 与逐次合并一致
- [ ] `test_full_merge_empty_operands`: 空 operands 不 panic
- [ ] `test_partial_merge_returns_none`: partial_merge 返回 None
- [ ] `test_full_merge_empty_fields_union`: 空字段集合并
- [ ] 覆盖率 ≥85%
- [ ] clippy 零警告

### T1.2 snapshot.rs 独立测试

- [ ] `test_snapshot_get_after_write_invisible`: 新写入不可见
- [ ] `test_snapshot_read_range_consistency`: 读取一致性
- [ ] `test_snapshot_multiple_independent`: 多快照独立
- [ ] `test_snapshot_cross_cf_read`: 跨 CF 读取
- [ ] 覆盖率 ≥85%

### T1.3 error.rs 全路径覆盖

- [ ] `test_invalid_key_construction`: InvalidKey 构造和 Display
- [ ] `test_invalid_value_construction`: InvalidValue 构造和 Display
- [ ] `test_cf_not_found_construction`: CfNotFound 变体
- [ ] `test_arrow_conversion_error_chain`: From<TsdbArrowError> 映射
- [ ] `test_error_source_chain`: source() 追溯
- [ ] 覆盖率 ≥90%

### T1.4 key.rs 属性测试

- [ ] proptest 依赖已添加到 Cargo.toml
- [ ] `proptest_key_encode_decode_roundtrip`: 256 cases 通过
- [ ] `proptest_key_ordering_invariant`: 256 cases 通过
- [ ] `proptest_prefix_encode_is_prefix`: 256 cases 通过
- [ ] `proptest_key_size_constant`: 256 cases 通过

### T1.5 value.rs 属性测试

- [ ] `proptest_value_encode_decode_roundtrip`: 256 cases 通过
- [ ] `proptest_value_all_field_types`: 256 cases 通过
- [ ] `proptest_value_special_floats`: NaN/Inf/-Inf 语义保留

### T1.6 comparator.rs 不变量测试

- [ ] `test_comparator_transitivity`: 10000 组随机数据
- [ ] `test_comparator_antisymmetry`: a<b → !(b<a)
- [ ] `test_comparator_reflexivity`: !(a<a)
- [ ] `test_comparator_different_length_keys`: 安全比较
- [ ] 覆盖率 ≥95%

---

## T2: 压力测试增强验收

### T2.1 写入吞吐增强

- [ ] `stress_rocksdb_write_1M_single_thread`: 1M points, >40K pts/s
- [ ] `stress_rocksdb_write_batch_sizes`: batch=1/100/1000/10000 对比
- [ ] StressTestReport JSON 输出
- [ ] 1M 版本 CI 中 <120s

### T2.2 并发读写增强

- [ ] `stress_concurrent_write_8_threads`: 8 线程无数据丢失
- [ ] `stress_concurrent_snapshot_consistency`: 快照一致性 100%
- [ ] 无 panic, 无 deadlock

### T2.3 读取性能增强

- [ ] `stress_rocksdb_batched_multi_get`: P99 < 20ms
- [ ] `stress_rocksdb_cross_day_scan_30days`: 无退化
- [ ] 性能不退化超过 20%

### T2.4 长时间稳定性测试

- [ ] `stress_long_running_5min`: 内存稳定
- [ ] `stress_long_running_cf_creation`: 延迟稳定
- [ ] lib.rs 包含 `pub mod stability_stress;`

---

## T3: 集成测试增强验收

### T3.1 DataFusion + RocksDB SQL 集成

- [ ] `test_e2e_rocksdb_sql_select`: SELECT * 查询正确
- [ ] `test_e2e_rocksdb_sql_aggregation`: AVG/SUM/COUNT/MIN/MAX 正确
- [ ] `test_e2e_rocksdb_sql_where_tag`: WHERE tag 过滤正确

### T3.2 引擎对比测试

- [ ] `test_e2e_parquet_vs_rocksdb_consistency`: 查询结果一致
- [ ] `test_e2e_rocksdb_performance_baseline`: 基线已记录

### T3.3 Arrow 适配空结果测试

- [ ] `test_e2e_rocksdb_empty_result_arrow`: 空 RecordBatch schema 正确

---

## T4: 真实数据生成器验收

### T4.1 DevOps Generator

- [ ] `make_devops_datapoints_enhanced`: hosts/ranges/measurements 可配置
- [ ] 周期性模式: 白天值 > 夜间值
- [ ] 异常注入: spike/dropout/gap 可控
- [ ] Tags/Fields/DataPoint 结构合法

### T4.2 IoT Generator

- [ ] `make_iot_datapoints`: 高频数据生成
- [ ] 设备分组: region/type 标签正确
- [ ] 离线模拟: gap 分布可控
- [ ] 大规模 (100K devices) 不 OOM

### T4.3 Financial Generator

- [ ] `make_financial_ohlcv`: OHLCV 数据
- [ ] high >= low, open/close 在 [low, high]
- [ ] 时间戳对齐到 bar interval
- [ ] 不同 volatility 产生不同分布

---

## T5: CLI 子命令实现验收

### T5.1 status 子命令

- [ ] `tsdb-cli status --data-dir ./data` 输出格式化状态
- [ ] 包含: 版本、CF 列表、内存信息
- [ ] `--storage-engine` 参数支持

### T5.2 query 子命令

- [ ] `tsdb-cli query --sql "SELECT * FROM cpu"` 返回 JSON
- [ ] `--format table` 表格输出
- [ ] `--format csv` CSV 输出
- [ ] SQL 聚合查询正确

### T5.3 compact 子命令

- [ ] `tsdb-cli compact --data-dir ./data` 全部 CF compact
- [ ] `--cf ts_cpu_20260417` 指定 CF compact
- [ ] compact 后数据可读

### T5.4 bench 子命令增强

- [ ] `tsdb-cli bench write --points 100000` 输出报告
- [ ] report 包含: ops/sec, p50/p99 latency
- [ ] `--workers` 参数支持
- [ ] bench read 模式

### T5.5 import 子命令

- [ ] `tsdb-cli import --file test.json --format json` 导入成功
- [ ] `--batch-size` 参数支持
- [ ] 导入后数据可查询

### T5.6 archive 子命令

- [ ] `archive create --older-than 30d` 生成归档文件
- [ ] `archive restore` 恢复成功
- [ ] `archive list` 列出归档
- [ ] 归档→删除→恢复→查询 数据完整

---

## T6: CLI 测试覆盖验收

### T6.1 CLI 集成测试

- [ ] `test_cli_status_output`: 输出包含 "Column Families"
- [ ] `test_cli_query_output`: 输出 JSON 格式
- [ ] `test_cli_compact_success`: 成功执行
- [ ] `test_cli_bench_write_output`: 输出性能报告
- [ ] `test_cli_import_json`: 导入成功

### T6.2 CLI 参数解析测试

- [ ] `test_cli_help_output`: --help 显示子命令
- [ ] `test_cli_version_output`: --version 显示版本
- [ ] `test_cli_missing_data_dir`: 缺少参数报错
- [ ] `test_cli_unknown_subcommand`: 未知命令报错

### T6.3 CLI 配置加载测试

- [ ] `test_cli_config_file_loading`: tsdb.toml 加载
- [ ] `test_cli_default_config`: 默认值正确

---

## T7: 多平台兼容性验收

### T7.1 CPU 特性检测模块

- [ ] `detect_cpu_features()` 在 x86_64 上返回 has_avx2/has_sse42
- [ ] `detect_cpu_features()` 在 aarch64 上返回 has_neon
- [ ] `optimized_config()` 返回平台最优配置
- [ ] lib.rs 包含 `pub mod cpu_features;`

### T7.2 RISC-V cross-compile

- [ ] CI: `cargo check --target riscv64gc-unknown-linux-gnu` 通过
- [ ] 无 x86-specific intrinsic/inline asm

### T7.3 Docker 多架构

- [ ] `docker buildx bake` linux/amd64 成功
- [ ] `docker buildx bake` linux/arm64 成功
- [ ] 容器内 `tsdb-cli --version` 正常

---

## T8: CI/CD 验收

### T8.1 覆盖率阈值门禁

- [ ] tarpaulin 命令包含 `--fail-under 80`
- [ ] 覆盖率 ≥80% 时 job pass
- [ ] 覆盖率 <80% 时 job fail
- [ ] Codecov 上传成功

### T8.2 性能回归检测

- [ ] PR 触发 bench job
- [ ] 回归 ≤10%: pass
- [ ] 回归 10~25%: warn
- [ ] 回归 >25%: fail

### T8.3 压力测试 Gate

- [ ] main 分支 push 触发 stress job
- [ ] 每个 test timeout ≤10min
- [ ] 结果存档为 artifact

### T8.4 Dependabot

- [ ] `.github/dependabot.yml` 存在
- [ ] weekly 依赖更新配置
- [ ] audit-check 通过

---

## 辅助函数提取验收

- [ ] `make_tags()`, `make_fields()`, `ts_today()`, `ts_days_ago()` 在 tsdb-test-utils 中
- [ ] stress 模块使用 `use tsdb_test_utils::*`
- [ ] 无重复辅助函数
- [ ] 全部 stress 测试通过

---

## 最终发布检查清单

### 发布前必须全部 ✅

- [ ] `cargo test --workspace` 全部通过 (0 failed)
- [ ] `cargo clippy --all-targets -- -D warnings` 零警告
- [ ] `cargo fmt --all -- --check` 格式正确
- [ ] 覆盖率 ≥80%
- [ ] Linux/macOS/Windows 三平台 CI 绿色
- [ ] AArch64 cross-compile 通过
- [ ] ARMv7 cross-compile 通过
- [ ] MSRV (1.85.0) check 通过
- [ ] 安全审计零漏洞
- [ ] CLI `--help` / `--version` 正常
- [ ] 压力测试基线已记录
- [ ] 所有 spec 中的缺失测试项已补充
