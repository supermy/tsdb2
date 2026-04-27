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
│  axum HTTP Gateway (:3000)                                   │
│  /api/services  /api/configs  /api/test  /api/metrics       │
│  /api/rocksdb   /api/parquet  /api/sql   /api/lifecycle     │
│  /api/collector /api/iceberg  /api/ws (WebSocket 实时推送)  │
└─────────────────────┬───────────────────────────────────────┘
                      │ NNG req/rep
┌─────────────────────▼───────────────────────────────────────┐
│  NNG Admin Server (:8080 rep / :8081 pub)                   │
│  ServiceManager │ ConfigManager │ MetricsCollector          │
│  ParquetApi     │ SqlApi        │ LifecycleApi │ IcebergApi │
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
- **Web SQL 控制台**: 浏览器内直接执行 SQL，自动合并 RocksDB + Parquet 数据
- **数据生命周期**: 热数据(RocksDB) → 温数据(Parquet SNAPPY) → 冷数据(Parquet ZSTD)
- **Iceberg 表管理**: 创建/扫描/快照/回滚/压缩/过期清理
- **Parquet 数据查看**: 文件列表、元数据、数据预览
- **实时监控**: CPU/内存/磁盘/网络/温度采集，WebSocket 实时推送
- **RocksDB 管理**: CF 统计、KV 扫描、Schema 查看、手动压缩
- **服务管理**: 创建/启停/重启服务，日志查看
- **配置管理**: 多配置文件，对比、应用
- **基准测试**: 写入/读取性能测试

## 快速开始

### 前置要求

- Rust 1.85.0+
- Node.js 20+ (前端构建)
- RocksDB 开发库 (Linux: `librocksdb-dev`)

### 使用 Makefile（推荐）

```bash
# 查看所有命令
make help

# 完整构建
make build

# 开发模式（前后端热更新）
make dev

# 生产启动
make start

# 运行测试
make test

# 代码质量检查
make check

# Docker 构建
make docker-build
```

### 手动构建

```bash
# 后端
cargo build --release -p tsdb-cli

# 前端
cd tsdb-dashboard && npm install && npm run build
cp -r tsdb-dashboard/dist target/release/dashboard/
```

### 启动

```bash
# 生产模式
make start

# 开发模式
make dev

# 手动启动
tsdb-cli serve --data-dir ./data --parquet-dir ./data_parquet \
    --storage-engine rocksdb --config default \
    --host 0.0.0.0 --flight-port 50051 \
    --admin-port 8080 --http-port 3000
```

启动后访问 http://localhost:3000 打开 Dashboard。

### 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `TSDB_DATA_DIR` | `./data` | RocksDB 数据目录 |
| `TSDB_PARQUET_DIR` | `./data_parquet` | Parquet 数据目录 |
| `TSDB_HOST` | `0.0.0.0` | 监听地址 |
| `TSDB_FLIGHT_PORT` | `50051` | Flight gRPC 端口 |
| `TSDB_ADMIN_PORT` | `8080` | NNG Admin 端口 |
| `TSDB_HTTP_PORT` | `3000` | HTTP Dashboard 端口 |
| `TSDB_ENGINE` | `rocksdb` | 存储引擎 |
| `TSDB_CONFIG` | `default` | 配置名称 |

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
| POST | `/api/test/generate-business-data` | 生成多业务测试数据 |

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
| POST | `/api/sql/execute` | 执行 SQL 语句（自动合并 RocksDB + Parquet） |
| GET | `/api/sql/tables` | 列出可用表 |

### 数据生命周期

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/lifecycle/status` | 数据分层状态 |
| POST | `/api/lifecycle/archive` | 执行数据归档 |
| POST | `/api/lifecycle/demote-to-warm` | 手动降级为温数据 |
| POST | `/api/lifecycle/demote-to-cold` | 手动降级为冷数据 |

### Iceberg 表管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/iceberg/tables` | 列出所有表 |
| POST | `/api/iceberg/tables` | 创建表 |
| GET | `/api/iceberg/tables/:name` | 表详情 |
| DELETE | `/api/iceberg/tables/:name` | 删除表 |
| POST | `/api/iceberg/tables/:name/append` | 追加数据 |
| GET | `/api/iceberg/tables/:name/scan` | 扫描数据 |
| GET | `/api/iceberg/tables/:name/snapshots` | 快照列表 |
| POST | `/api/iceberg/tables/:name/rollback` | 回滚快照 |
| POST | `/api/iceberg/tables/:name/compact` | 压缩表 |
| POST | `/api/iceberg/tables/:name/expire` | 过期清理 |
| POST | `/api/iceberg/tables/:name/schema` | 更新 Schema |

## 数据生命周期

TSDB2 实现了基于存储引擎的数据分层策略：

```
┌──────────────────────────────────────────────────┐
│ 热数据 (RocksDB)                                  │
│   所有在 RocksDB 中的 CF                          │
│   demote_eligible:                                │
│     none  → age≤3天, 不可降级                     │
│     warm  → age 4-14天, 可降级为温数据             │
│     cold  → age>14天, 可降级为冷数据               │
└─────────────┬───────────────────┬────────────────┘
              │ demote_to_warm    │ demote_to_cold
              │ (SNAPPY 压缩)     │ (ZSTD 压缩)
              ▼                   ▼
┌──────────────────┐  ┌──────────────────┐
│ 温数据 (Parquet)  │  │ 冷数据 (Parquet)  │
│ SNAPPY 压缩      │  │ ZSTD 压缩        │
│ 可继续降级为冷数据 │  │ 长期存储         │
└──────────────────┘  └──────────────────┘
```

**核心原则**：RocksDB 中的所有 CF 都是热数据，不论其年龄。只有已降级到 Parquet 的数据才被分类为温/冷数据。

详细文档见 [docs/data-lifecycle.md](docs/data-lifecycle.md)。

## Dashboard 页面

| 页面 | 路径 | 功能 |
|------|------|------|
| 仪表盘 | `/dashboard` | 服务概览、实时指标、多业务数据生成 |
| 服务管理 | `/services` | 创建/启停/重启服务 |
| 配置管理 | `/configs` | 配置文件 CRUD、对比 |
| 功能测试 | `/testing` | SQL 测试、读写基准 |
| 状态监控 | `/monitoring` | CPU/内存/磁盘/网络实时图表 |
| 数据查询 | `/data-query` | 时间范围查询 |
| RocksDB | `/rocksdb` | CF 统计、KV 浏览、Schema |
| Parquet | `/parquet` | 文件列表、元数据、数据预览 |
| SQL 控制台 | `/sql` | SQL 编辑器、结果表格、历史 |
| 数据生命周期 | `/lifecycle` | 热温冷分布、手动降级操作 |
| Iceberg | `/iceberg` | 表管理、快照、Schema |

## 项目结构

```
tsdb2/
├── crates/
│   ├── tsdb-arrow/         # Arrow schema + StorageEngine trait + DataPoint 转换
│   ├── tsdb-rocksdb/       # RocksDB 存储引擎 + 数据归档
│   ├── tsdb-storage-arrow/ # Arrow 内存存储引擎 (WAL + Parquet)
│   ├── tsdb-iceberg/       # Iceberg 表格式集成
│   ├── tsdb-datafusion/    # DataFusion SQL 查询引擎
│   ├── tsdb-flight/        # Arrow Flight SQL 服务
│   ├── tsdb-admin/         # NNG Admin + axum HTTP Gateway
│   ├── tsdb-cli/           # 命令行工具
│   ├── tsdb-bench/         # 性能基准测试
│   ├── tsdb-test-utils/    # 测试工具
│   ├── tsdb-integration-tests/  # 集成/业务测试
│   ├── tsdb-stress/        # 压力测试
│   └── tsdb-stress-rocksdb/ # RocksDB 压力测试
├── tsdb-dashboard/         # React + Vite + Ant Design 前端
├── configs/                # 配置文件 (INI 格式)
├── scripts/                # 运维脚本
├── docs/                   # 文档
├── Makefile                # 构建与开发命令
└── Dockerfile              # Docker 镜像构建
```

## Makefile 命令

| 命令 | 说明 |
|------|------|
| `make build` | 完整构建 (release) |
| `make dev` | 开发模式 (热更新) |
| `make start` | 生产启动 |
| `make stop` | 停止服务 |
| `make test` | 运行测试 + lint |
| `make check` | 完整质量检查 |
| `make lint` | Clippy + 格式检查 |
| `make docker-build` | 构建 Docker 镜像 |
| `make bench` | 运行性能基准 |
| `make help` | 查看所有命令 |

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

## 测试

```bash
# 使用 Makefile
make test

# 完整质量检查
make check

# 手动
cargo clippy --workspace -- -D warnings
cargo test --workspace --exclude tsdb-stress-rocksdb
cd tsdb-dashboard && npm run build
```

## 文档

| 文档 | 说明 |
|------|------|
| [architecture.md](docs/architecture.md) | 技术架构（类图、时序图、数据模型） |
| [data-lifecycle.md](docs/data-lifecycle.md) | 数据生命周期管理 |
| [data-structures.md](docs/data-structures.md) | 数据结构设计 |
| [key-technologies.md](docs/key-technologies.md) | 关键技术选型 |
| [user-guide.md](docs/user-guide.md) | 使用手册 |
| [test-report.md](docs/test-report.md) | 测试报告 |

## License

MIT
