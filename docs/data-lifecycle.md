# TSDB2 数据生命周期管理

## 1. 概述

TSDB2 实现了基于存储引擎的数据分层策略，双引擎（RocksDB + Arrow）均支持完整的生命周期管理。数据按实际存储位置分为四个层级：

| 层级 | 存储引擎 | 压缩方式 | 查询延迟 | 说明 |
|------|----------|----------|----------|------|
| 🔥 热数据 (Hot) | RocksDB / Arrow | LSM / SNAPPY | < 1ms / ~5ms | 所有活跃数据 |
| 🌤️ 温数据 (Warm) | Parquet | SNAPPY | ~10ms | 已降级到 warm/ 的数据 |
| ❄️ 冷数据 (Cold) | Parquet | ZSTD | ~50ms | 已降级到 cold/ 的数据 |
| 📦 归档 (Archive) | Parquet | ZSTD (最高) | ~100ms | 已归档到 archive/ 的数据 |

### 1.1 RocksDB 引擎生命周期

**核心原则：RocksDB 中的所有 CF 都是热数据，不论其年龄。只有已降级到 Parquet 的数据才被分类为温/冷数据。**

`demote_eligible` 字段标识热数据 CF 是否可降级：
- `none`：年龄 ≤3 天，不可降级
- `warm`：年龄 4-14 天，可降级为温数据
- `cold`：年龄 >14 天，可降级为冷数据

```
写入数据
    │
    ▼
┌──────────┐
│  RocksDB │ ◄── 热数据 (所有 CF)
│  (LSM)   │     高速读写
└────┬─────┘
     │ demote_to_warm (SNAPPY压缩)
     │ 仅 age > 3天的 CF 可降级
     ▼
┌──────────┐
│  Parquet │ ◄── 温数据
│ (SNAPPY) │     列式存储, 快速扫描
└────┬─────┘
     │ demote_to_cold (ZSTD压缩)
     ▼
┌──────────┐
│  Parquet │ ◄── 冷数据 (>14天)
│  (ZSTD)  │     高压缩比, 归档存储
└────┬─────┘
     │ archive
     ▼
┌──────────┐
│  Parquet │ ◄── 归档数据
│  (ZSTD)  │     长期存储
└──────────┘
```

### 1.2 Arrow 引擎生命周期

**核心原则：Arrow 引擎中 `data_YYYYMMDD/measurement/` 目录下的数据是热数据。降级通过目录移动实现。**

```
写入路径:
  Client → validate() [路径安全检查]
         → WAL [段文件轮转, 64MB/段]
         → AsyncWriteBuffer [后台定时flush]
         → TsdbParquetWriter [schema evolution延迟flush]
         → data_YYYYMMDD/measurement/*.parquet [临时文件+fsync+原子rename]

降级路径:
  data_YYYYMMDD/measurement/  ──demote_to_warm──→  warm/data_YYYYMMDD/measurement/
  warm/data_YYYYMMDD/measurement/  ──demote_to_cold──→  cold/data_YYYYMMDD/measurement/
  cold/data_YYYYMMDD/measurement/  ──archive──→  archive/data_YYYYMMDD/measurement/

清理路径:
  cleanup_expired() → flush() → 删除过期分区 → 清理stale writer
```

**Arrow 引擎降级规则：**
- `none`：年龄 ≤3 天，不可降级
- `warm`：年龄 4-14 天，可降级为温数据
- `cold`：年龄 >14 天，可降级为冷数据

**Arrow 引擎 cf_name 格式：** `{measurement}_{YYYYMMDD}`（例如 `cpu_20260426`）

## 2. IoT 数据生命周期示例

### 2.1 数据生成

以 IoT 物联网场景为例，系统支持生成模拟数据用于测试：

```bash
# 启动服务 - RocksDB 引擎
tsdb-cli serve --data-dir /tmp/tsdb2 --parquet-dir /tmp/parquet --storage-engine rocksdb

# 启动服务 - Arrow 引擎
tsdb-cli serve --data-dir /tmp/tsdb2 --parquet-dir /tmp/parquet --storage-engine arrow

# 生成IoT数据 (保留温/冷CF在RocksDB，便于手动降级)
curl -X POST http://localhost:3000/api/test/generate-business-data \
  -H 'Content-Type: application/json' \
  -d '{"scenario":"iot","points_per_series":30,"skip_auto_demote":true}'
```

**参数说明：**

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `scenario` | 业务场景: `iot` / `devops` / `ecommerce` / `generic` | `iot` |
| `points_per_series` | 每个时间序列的数据点数 | 60 |
| `skip_auto_demote` | 是否跳过自动降级（true=保留在RocksDB） | `true` |

**IoT 场景生成的指标：**

| 指标名 | 标签 (Tags) | 字段 (Fields) |
|--------|-------------|---------------|
| `iot_temperature` | device_id, location, floor | temperature (f64) |
| `iot_humidity` | device_id, location, floor | humidity (f64) |
| `iot_pressure` | device_id, location | pressure (f64) |

**时间分布：**

```
|<--- 热数据 --->|<------ 温数据 ------>|<------- 冷数据 -------->|
0天            3天                  14天                      24天
  ↓              ↓                    ↓
RocksDB/Arrow  Parquet(SNAPPY)     Parquet(ZSTD)
```

### 2.2 查看生命周期状态

```bash
# 查看数据分层状态
curl http://localhost:3000/api/lifecycle/status | python3 -m json.tool
```

**RocksDB 引擎返回示例：**

```json
{
  "success": true,
  "data": {
    "hot_cfs": [
      {
        "cf_name": "ts_iot_temperature_20260426",
        "measurement": "iot_temperature",
        "date": "20260426",
        "age_days": 0,
        "sst_size": 2048,
        "num_keys": 540,
        "tier": "hot",
        "storage": "rocksdb",
        "demote_eligible": "none"
      }
    ],
    "warm_cfs": [],
    "cold_cfs": [],
    "total_hot_bytes": 2048,
    "total_warm_bytes": 0,
    "total_cold_bytes": 0
  }
}
```

**Arrow 引擎返回示例：**

```json
{
  "success": true,
  "data": {
    "hot_cfs": [
      {
        "cf_name": "iot_temperature_20260426",
        "measurement": "iot_temperature",
        "date": "20260426",
        "age_days": 0,
        "sst_size": 4096,
        "num_keys": 540,
        "tier": "hot",
        "storage": "parquet",
        "demote_eligible": "none"
      }
    ],
    "warm_cfs": [
      {
        "cf_name": "iot_temperature_20260422",
        "measurement": "iot_temperature",
        "date": "20260422",
        "age_days": 4,
        "sst_size": 3072,
        "num_keys": 540,
        "tier": "warm",
        "storage": "parquet",
        "demote_eligible": "cold"
      }
    ],
    "cold_cfs": [],
    "archive_files": [],
    "parquet_partitions": [],
    "total_hot_bytes": 4096,
    "total_warm_bytes": 3072,
    "total_cold_bytes": 0,
    "total_archive_bytes": 0
  }
}
```

**关键字段说明：**

- `tier`: 数据层级分类（hot/warm/cold），基于**实际存储位置**
  - `hot` = 在 RocksDB 中 或 Arrow 引擎的 `data_YYYYMMDD/` 目录中
  - `warm` = 在 Parquet `warm/` 目录中
  - `cold` = 在 Parquet `cold/` 目录中
- `storage`: 实际存储引擎（rocksdb/parquet）
- `demote_eligible`: 可降级目标（none/warm/cold），基于 `age_days` 自动计算
- `age_days`: 数据年龄（天），从 CF 名中的日期计算

### 2.3 手动降级

#### 热数据 → 温数据

**RocksDB 引擎：**

```bash
curl -X POST http://localhost:3000/api/lifecycle/demote-to-warm \
  -H 'Content-Type: application/json' \
  -d '{"cf_names":["ts_iot_temperature_20260422"]}'
```

降级流程：创建 warm 目录 → 写入 `.demote_marker` → 从 RocksDB 导出 CF 到 Parquet (SNAPPY) → 删除 CF → 删除标记

**Arrow 引擎：**

```bash
curl -X POST http://localhost:3000/api/lifecycle/demote-to-warm \
  -H 'Content-Type: application/json' \
  -d '{"cf_names":["iot_temperature_20260422"]}'
```

降级流程：创建 warm 目录 → 写入 `.demote_marker` → 将 Parquet 文件从 `data_YYYYMMDD/measurement/` 移动到 `warm/data_YYYYMMDD/measurement/` → 删除标记 → 清理空目录

#### 温数据 → 冷数据

```bash
curl -X POST http://localhost:3000/api/lifecycle/demote-to-cold \
  -H 'Content-Type: application/json' \
  -d '{"cf_names":["iot_temperature_20260422"]}'
```

**RocksDB 引擎降级流程：**
- 场景A: CF 仍在 RocksDB → 导出为 Parquet (ZSTD) 到 cold 目录 → 删除 CF
- 场景B: CF 已在 warm 目录 → 将文件从 warm/ 移动到 cold/

**Arrow 引擎降级流程：**
- 如果数据在 hot 目录 → 先移动到 warm → 再移动到 cold
- 如果数据在 warm 目录 → 将文件从 warm/ 移动到 cold/

### 2.4 归档

```bash
# 归档 7 天以上的数据
curl -X POST http://localhost:3000/api/lifecycle/archive \
  -H 'Content-Type: application/json' \
  -d '{"older_than_days":7}'
```

**RocksDB 引擎：** 将超过指定天数的 RocksDB CF 导出为 Parquet (ZSTD) 并删除。

**Arrow 引擎：** 将超过指定天数的 hot 和 cold 分区移动到 `archive/` 目录。

### 2.5 SQL 查询

SQL 控制台自动合并 RocksDB 和 Parquet 数据：

```sql
-- 查询 IoT 温度数据（自动合并热/温/冷数据）
SELECT timestamp, device_id, temperature
FROM iot_temperature
WHERE timestamp > '2026-04-01'
ORDER BY timestamp DESC
LIMIT 100;

-- 按设备聚合
SELECT device_id, avg(temperature), count(*)
FROM iot_temperature
GROUP BY device_id;
```

## 3. 目录结构

### 3.1 RocksDB 引擎

```
/tmp/tsdb2/                          # RocksDB 数据目录
├── _series_meta/                    # 序列元数据 CF
├── ts_iot_temperature_20260426/     # 热数据 CF (当天)
├── ts_iot_temperature_20260425/     # 热数据 CF (1天前)
└── ts_iot_temperature_20260424/     # 热数据 CF (2天前)

/tmp/parquet/                        # Parquet 数据目录
├── warm/                            # 温数据 (SNAPPY)
│   └── ts_iot_temperature_20260422/
│       └── part-00000000.parquet
├── cold/                            # 冷数据 (ZSTD)
│   └── ts_iot_temperature_20260411/
│       └── part-00000000.parquet
└── archive/                         # 归档数据 (ZSTD)
    └── ts_iot_temperature_20260401/
        └── part-00000000.parquet
```

### 3.2 Arrow 引擎

```
/tmp/parquet/                        # Parquet 数据目录
├── data_20260426/                   # 热数据分区 (当天)
│   ├── iot_temperature/             # Measurement 子目录
│   │   ├── part-00000001.parquet
│   │   └── L1-iot_temperature-20260426-abc.parquet
│   └── iot_humidity/
│       └── part-00000002.parquet
├── data_20260425/                   # 热数据分区 (1天前)
│   └── iot_temperature/
│       └── part-00000003.parquet
├── wal/                             # WAL 段文件
│   ├── wal-000001.log
│   └── wal-000002.log
├── warm/                            # 温数据 (SNAPPY)
│   └── data_20260422/
│       └── iot_temperature/
│           └── part-00000004.parquet
├── cold/                            # 冷数据 (ZSTD)
│   └── data_20260411/
│       └── iot_temperature/
│           └── L2-iot_temperature-20260411-xyz.parquet
└── archive/                         # 归档数据 (ZSTD)
    └── data_20260401/
        └── iot_temperature/
            └── part-00000005.parquet
```

## 4. 崩溃恢复

### 4.1 降级操作崩溃恢复

降级操作使用标记文件（`.demote_marker`）确保崩溃安全：

```
降级开始 → 写入 .demote_marker → 移动/导出数据 → 删除源数据 → 删除 .demote_marker
                                    │                 │
                                    │    崩溃点1       │ 崩溃点2
                                    ▼                 ▼
                              下次启动发现标记    下次启动发现标记
                              + 目标已存在       + 源仍存在
                              → 安全，继续删除源  → 再次删除源
```

### 4.2 Arrow 引擎 WAL 崩溃恢复

Arrow 引擎使用 WAL（预写日志）确保写入持久性：

```
写入数据 → WAL.append() [write→flush→sync_all] → AsyncWriteBuffer → Parquet

崩溃恢复流程:
1. 扫描 WAL 目录中所有 wal-*.log 段文件
2. 逐条解码并校验 CRC32
3. 反序列化为 DataPoint
4. 重写到存储引擎 (write_batch)
5. flush 到 Parquet
6. truncate WAL
```

**WAL 段文件轮转：**
- 默认 64MB/段
- 超过阈值自动创建新段文件 `wal-NNNNNN.log`
- flush 成功后 truncate 清理当前段 + 删除旧段

## 5. API 参考

### 生命周期 API

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/lifecycle/status` | GET | 获取数据分层状态 (双引擎) |
| `/api/lifecycle/demote-to-warm` | POST | 将指定 CF 降级为温数据 (双引擎) |
| `/api/lifecycle/demote-to-cold` | POST | 将指定 CF 降级为冷数据 (双引擎) |
| `/api/lifecycle/archive` | POST | 归档指定天数以上的数据 (双引擎) |

### 请求体格式

**demote-to-warm / demote-to-cold:**
```json
{
  "cf_names": ["ts_iot_temperature_20260422"]
}
```

**archive:**
```json
{
  "older_than_days": 7
}
```

### 验证规则

- `cf_names` 不能为空，最多 100 个
- `cf_names` 中的名称必须匹配 `^[a-zA-Z0-9_-]+$`
- `older_than_days` 必须 ≥ 7

## 6. 前端操作指南

### 6.1 生成测试数据

1. 打开 Dashboard 页面
2. 选择业务场景（IoT / DevOps / 电商）
3. 设置每序列点数
4. 开关「保留温/冷CF在RocksDB」：
   - **开启（默认）**：数据生成后保留在 RocksDB，可手动降级
   - **关闭**：数据生成后自动降级到 Parquet
5. 点击「生成数据」

### 6.2 手动降级

1. 打开「数据生命周期」页面
2. 在热数据表中勾选要降级的 CF
3. 点击「降级为温数据」
4. 在温数据表中勾选要降级的 CF
5. 点击「降级为冷数据」

### 6.3 查看数据

- **存储列**：显示实际存储引擎（RocksDB / Parquet SNAPPY / Parquet ZSTD）
- **年龄列**：显示数据年龄，用于判断层级
- **路径列**：显示数据文件路径
