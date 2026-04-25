# TSDB2

高性能时序数据库，双引擎架构（RocksDB + Parquet/Iceberg），支持 Flight SQL、Web 管理界面和数据生命周期管理。

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│  浏览器 / Electron                                          │
│  React + Ant Design Dashboard (10 个功能页面)               │
└─────────────────────┬───────────────────────────────────────┘
                      │ HTTP REST + WebSocket
┌─────────────────────▼───────────────────────────────────────┐
│  axum HTTP Gateway (:8902)                                   │
│  /api/services  /api/configs  /api/test  /api/metrics       │
│  /api/rocksdb   /api/parquet  /api/sql   /api/lifecycle     │
│  /api/collector /api/ws (WebSocket → NNG pub/sub 实时推送)  │
└─────────────────────┬───────────────────────────────────────┘
                      │ NNG req/rep
┌─────────────────────▼───────────────────────────────────────┐
│  NNG Admin Server (:8080 rep / :8081 pub)                   │
│  ServiceManager │ ConfigManager │ MetricsCollector          │
│  ParquetApi     │ SqlApi        │ LifecycleApi              │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│  Flight gRPC Server (:50051) + Storage Engine               │
│  RocksDB (热数据) │ Parquet/Iceberg (冷数据)                │
└─────────────────────────────────────────────────────────────┘
```

## 功能特性

- **双引擎存储**: RocksDB 热数据 + Parquet/Iceberg 冷数据归档
- **Flight SQL**: Arrow Flight gRPC 协议，支持 SQL 查询和流式写入
- **Web SQL 控制台**: 浏览器内直接执行 SQL，支持聚合、分组、SHOW TABLES
- **数据生命周期**: 热数据(≤3天) → 温数据(4-14天) → 冷数据(>14天) → Parquet 归档
- **Parquet 数据查看**: 文件列表、元数据、数据预览
- **实时监控**: CPU/内存/磁盘/网络/温度采集，WebSocket 实时推送
- **RocksDB 管理**: CF 统计、KV 扫描、Schema 查看、手动压缩
- **服务管理**: 创建/启停/重启服务，日志查看
- **配置管理**: 多配置文件，对比、应用
- **基准测试**: 写入/读取性能测试

## 快速开始

### 构建

```bash
# 完整构建（后端 + 前端）
./scripts/build.sh

# 或手动构建
cargo build --release -p tsdb-cli
cd tsdb-dashboard && npm install && npm run build
cp -r tsdb-dashboard/dist/* target/release/dashboard/
```

### 启动

```bash
# 生产模式
./scripts/start.sh

# 开发模式（前后端热更新）
./scripts/dev.sh

# 手动启动
tsdb-cli serve --data-dir ./data --storage-engine rocksdb \
    --config balanced --host 0.0.0.0 \
    --flight-port 50051 --admin-port 8080 --http-port 8902
```

启动后访问 http://localhost:8902 打开 Dashboard。

### 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `TSDB_DATA_DIR` | `./data` | 数据存储目录 |
| `TSDB_HOST` | `0.0.0.0` | 监听地址 |
| `TSDB_FLIGHT_PORT` | `50051` | Flight gRPC 端口 |
| `TSDB_ADMIN_PORT` | `8080` | NNG Admin 端口 |
| `TSDB_HTTP_PORT` | `8902` | HTTP Dashboard 端口 |
| `TSDB_ENGINE` | `rocksdb` | 存储引擎 |
| `TSDB_CONFIG` | `balanced` | 配置名称 |

## CLI 命令

```bash
# 写入数据
tsdb-cli put --measurement cpu --tags "host=server1,region=us" \
    --fields "value=72.5" --timestamp 1700000000000

# 查询数据
tsdb-cli query --measurement cpu --start 1700000000 --end 1700003600

# 启动服务
tsdb-cli serve [OPTIONS]

# SQL 查询
tsdb-cli sql "SELECT * FROM cpu WHERE timestamp > now() - interval '1 hour'"

# 数据归档
tsdb-cli archive --older-than 30

# 健康检查
tsdb-cli doctor
```

## HTTP API

### 服务管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/services` | 列出所有服务 |
| POST | `/api/services` | 创建服务 |
| GET | `/api/services/:name` | 获取服务状态 |
| DELETE | `/api/services/:name` | 删除服务 |
| POST | `/api/services/:name/start` | 启动服务 |
| POST | `/api/services/:name/stop` | 停止服务 |
| POST | `/api/services/:name/restart` | 重启服务 |
| GET | `/api/services/:name/logs?lines=100` | 获取服务日志 |

### 配置管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/configs` | 列出所有配置 |
| POST | `/api/configs` | 保存配置 |
| GET | `/api/configs/:name` | 获取配置详情 |
| DELETE | `/api/configs/:name` | 删除配置 |
| POST | `/api/configs/apply` | 应用配置到服务 |
| POST | `/api/configs/compare` | 对比两个配置 |

### 功能测试

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/test/sql` | 执行 SQL 查询 |
| POST | `/api/test/write-bench` | 写入基准测试 |
| POST | `/api/test/read-bench` | 读取基准测试 |

### 监控指标

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/metrics/health?service=default` | 健康检查 |
| GET | `/api/metrics/stats?service=default` | 当前指标 |
| GET | `/api/metrics/timeseries?range_secs=300` | 时序数据 |
| GET | `/api/metrics/alerts?service=default` | 告警列表 |
| GET | `/api/collector/status` | 采集器状态 |
| POST | `/api/collector/configure` | 配置采集器 |
| POST | `/api/collector/start` | 启动采集器 |
| POST | `/api/collector/stop` | 停止采集器 |
| WS | `/api/ws` | 实时指标推送 |

### RocksDB 管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/rocksdb/stats` | RocksDB 统计概览 |
| GET | `/api/rocksdb/cf-list` | 列出所有 CF |
| GET | `/api/rocksdb/cf-detail/:name` | CF 详细信息 |
| POST | `/api/rocksdb/compact` | 手动压缩 |
| GET | `/api/rocksdb/kv-scan?cf=xxx&prefix=&limit=20` | KV 扫描 |
| GET | `/api/rocksdb/kv-get?cf=xxx&key=xxx` | KV 获取 |
| GET | `/api/rocksdb/series-schema` | 序列 Schema |

### Parquet 数据查看

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/parquet/list` | 列出所有 Parquet 文件 |
| GET | `/api/parquet/file-detail?path=xxx` | 文件元数据 |
| GET | `/api/parquet/preview?path=xxx&limit=50` | 数据预览 |

### SQL 执行

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/sql/execute` | 执行 SQL 语句 |
| GET | `/api/sql/tables` | 列出可用表 |

### 数据生命周期

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/lifecycle/status` | 数据分层状态 |
| POST | `/api/lifecycle/archive` | 执行数据归档 |

## 数据生命周期

TSDB2 实现了自动化的数据分层管理：

```
🔥 热数据 (≤3天)          🌤️ 温数据 (4-14天)         ❄️ 冷数据 (>14天)
RocksDB 内存 + SST        RocksDB SST only           归档 → Parquet
SNAPPY 压缩               自动降级压缩                ZSTD 压缩
快速读写                   读取为主                    仅读取
     │                         │                          │
     └───────── 自动过渡 ───────┴─────── 手动/自动归档 ────┘
```

- **热数据**: 最近 3 天的数据，存储在 RocksDB 内存和 SST 文件中，SNAPPY 压缩
- **温数据**: 4-14 天的数据，仅保留 SST 文件，自动降级压缩
- **冷数据**: 超过 14 天的数据，归档为 Parquet 文件，ZSTD 高压缩比
- **归档操作**: 通过 API 或 CLI 将冷数据从 RocksDB 导出为 Parquet 文件

## Dashboard 页面

| 页面 | 路径 | 功能 |
|------|------|------|
| 仪表盘 | `/dashboard` | 服务概览、实时指标 |
| 服务管理 | `/services` | 创建/启停/重启服务 |
| 配置管理 | `/configs` | 配置文件 CRUD、对比 |
| 功能测试 | `/testing` | SQL 测试、读写基准 |
| 状态监控 | `/monitoring` | CPU/内存/磁盘/网络实时图表 |
| 数据查询 | `/data-query` | 时间范围查询 |
| RocksDB | `/rocksdb` | CF 统计、KV 浏览、Schema |
| Parquet | `/parquet` | 文件列表、元数据、数据预览 |
| SQL 控制台 | `/sql` | SQL 编辑器、结果表格、历史 |
| 数据生命周期 | `/lifecycle` | 热温冷分布、归档操作 |

## 配置文件

配置文件位于 `configs/` 目录，INI 格式：

| 配置 | 说明 |
|------|------|
| `balanced.ini` | 均衡配置（默认） |
| `write-heavy.ini` | 写入密集场景 |
| `read-heavy.ini` | 读取密集场景 |
| `high-compression.ini` | 高压缩比 |
| `low-cost.ini` | 低成本/低内存 |
| `default.ini` | 基础默认配置 |
| `high_performance.ini` | 高性能配置 |
| `low_memory.ini` | 低内存配置 |

## 项目结构

```
tsdb2/
├── crates/
│   ├── tsdb-arrow/         # Arrow schema + StorageEngine trait + DataPoint 转换
│   ├── tsdb-rocksdb/       # RocksDB 存储引擎 + 数据归档
│   ├── tsdb-storage-arrow/ # Arrow 内存存储引擎
│   ├── tsdb-iceberg/       # Iceberg 表格式集成
│   ├── tsdb-datafusion/    # DataFusion SQL 查询引擎
│   ├── tsdb-flight/        # Arrow Flight SQL 服务
│   ├── tsdb-admin/         # NNG Admin + axum HTTP Gateway
│   ├── tsdb-cli/           # 命令行工具
│   ├── tsdb-bench/         # 性能基准测试
│   ├── tsdb-test-utils/    # 测试工具
│   └── tsdb-integration-tests/  # 集成/业务测试
├── tsdb-dashboard/         # React + Vite + Ant Design 前端
├── configs/                # 配置文件
├── scripts/                # 运维脚本
└── docs/                   # 文档
```

## 测试

```bash
./scripts/check.sh

# 或手动
cargo clippy --workspace -- -D warnings
cargo test --workspace --exclude tsdb-stress
cd tsdb-dashboard && npm run build
```

## License

MIT
