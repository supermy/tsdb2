# TSDB2 数据生命周期管理

## 1. 概述

TSDB2 实现了基于存储引擎的数据分层策略。数据按实际存储位置分为三个层级：

| 层级 | 存储引擎 | 压缩方式 | 查询延迟 | 说明 |
|------|----------|----------|----------|------|
| 🔥 热数据 (Hot) | RocksDB | LSM-Tree 内部压缩 | < 1ms | 所有在 RocksDB 中的 CF |
| 🌤️ 温数据 (Warm) | Parquet | SNAPPY | ~10ms | 已降级到 Parquet/warm/ 的数据 |
| ❄️ 冷数据 (Cold) | Parquet | ZSTD | ~50ms | 已降级到 Parquet/cold/ 的数据 |

**核心原则：RocksDB 中的所有 CF 都是热数据，不论其年龄。只有已降级到 Parquet 的数据才被分类为温/冷数据。**

`demote_eligible` 字段标识热数据 CF 是否可降级：
- `none`：年龄 ≤3 天，不可降级
- `warm`：年龄 4-14 天，可降级为温数据
- `cold`：年龄 >14 天，可降级为冷数据

### 架构图

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
└──────────┘
```

## 2. IoT 数据生命周期示例

### 2.1 数据生成

以 IoT 物联网场景为例，系统支持生成模拟数据用于测试：

```bash
# 启动服务
tsdb-cli serve --data-dir /tmp/tsdb2 --parquet-dir /tmp/parquet

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
RocksDB      RocksDB/Parquet      Parquet(ZSTD)
(SNAPPY)
```

### 2.2 查看生命周期状态

```bash
# 查看数据分层状态
curl http://localhost:3000/api/lifecycle/status | python3 -m json.tool
```

**返回示例：**

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
      },
      {
        "cf_name": "ts_iot_temperature_20260422",
        "measurement": "iot_temperature",
        "date": "20260422",
        "age_days": 4,
        "sst_size": 2048,
        "num_keys": 540,
        "tier": "hot",
        "storage": "rocksdb",
        "demote_eligible": "warm"
      }
    ],
    "warm_cfs": [
      {
        "cf_name": "ts_iot_temperature_20260421",
        "measurement": "iot_temperature",
        "date": "20260421",
        "age_days": 5,
        "sst_size": 2048,
        "num_keys": 540,
        "tier": "warm",
        "storage": "parquet",
        "demote_eligible": "cold"
      }
    ],
    "cold_cfs": [
      {
        "cf_name": "ts_iot_temperature_20260411",
        "measurement": "iot_temperature",
        "date": "20260411",
        "age_days": 15,
        "sst_size": 2048,
        "num_keys": 540,
        "tier": "cold",
        "storage": "parquet",
        "demote_eligible": "none"
      }
    ],
    "total_hot_bytes": 26624,
    "total_warm_bytes": 45056,
    "total_cold_bytes": 36864
  }
}
```

**关键字段说明：**

- `tier`: 数据层级分类（hot/warm/cold），基于**实际存储位置**
  - `hot` = 在 RocksDB 中
  - `warm` = 在 Parquet/warm/ 中
  - `cold` = 在 Parquet/cold/ 中
- `storage`: 实际存储引擎（rocksdb/parquet）
- `demote_eligible`: 可降级目标（none/warm/cold），基于 `age_days` 自动计算
- `age_days`: 数据年龄（天），从 CF 名中的日期计算

### 2.3 手动降级

#### 热数据 → 温数据 (RocksDB → Parquet SNAPPY)

```bash
curl -X POST http://localhost:3000/api/lifecycle/demote-to-warm \
  -H 'Content-Type: application/json' \
  -d '{"cf_names":["ts_iot_temperature_20260422","ts_iot_temperature_20260421"]}'
```

**降级流程：**

```
1. 创建 Parquet 输出目录: /tmp/parquet/warm/ts_iot_temperature_20260422/
2. 写入降级标记文件: .demote_marker
3. 从 RocksDB 导出 CF 数据到 Parquet (SNAPPY 压缩)
4. 从 RocksDB 删除 CF
5. 删除降级标记文件
6. 返回降级结果
```

#### 温数据 → 冷数据 (Parquet SNAPPY → Parquet ZSTD)

```bash
curl -X POST http://localhost:3000/api/lifecycle/demote-to-cold \
  -H 'Content-Type: application/json' \
  -d '{"cf_names":["ts_iot_temperature_20260422"]}'
```

**降级流程：**

```
场景A: CF 仍在 RocksDB 中
  → 导出为 Parquet (ZSTD) 到 cold 目录
  → 从 RocksDB 删除 CF

场景B: CF 已在 Parquet warm 目录中
  → 将 Parquet 文件从 warm/ 移动到 cold/
  → 重新压缩为 ZSTD（如需要）
```

### 2.4 归档

```bash
# 归档 7 天以上的 RocksDB CF
curl -X POST http://localhost:3000/api/lifecycle/archive \
  -H 'Content-Type: application/json' \
  -d '{"older_than_days":7}'
```

归档操作将超过指定天数的 RocksDB CF 导出为 Parquet (ZSTD) 并删除。

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

```
/tmp/tsdb2/                          # RocksDB 数据目录
├── _series_meta/                    # 序列元数据 CF
├── ts_iot_temperature_20260426/     # 热数据 CF (当天)
├── ts_iot_temperature_20260425/     # 热数据 CF (1天前)
└── ts_iot_temperature_20260424/     # 热数据 CF (2天前)

/tmp/parquet/                        # Parquet 数据目录
├── warm/                            # 温数据 (SNAPPY)
│   ├── ts_iot_temperature_20260422/
│   │   └── part-00000000.parquet
│   └── ts_iot_temperature_20260421/
│       └── part-00000000.parquet
└── cold/                            # 冷数据 (ZSTD)
    ├── ts_iot_temperature_20260411/
    │   └── part-00000000.parquet
    └── ts_iot_temperature_20260410/
        └── part-00000000.parquet
```

## 4. 崩溃恢复

降级操作使用标记文件（`.demote_marker`）确保崩溃安全：

```
降级开始 → 写入 .demote_marker → 导出 Parquet → 删除 CF → 删除 .demote_marker
                                    │                 │
                                    │    崩溃点1       │ 崩溃点2
                                    ▼                 ▼
                              下次启动发现标记    下次启动发现标记
                              + Parquet 已存在   + CF 仍存在
                              → 安全，继续删除CF  → 再次删除CF
```

## 5. API 参考

### 生命周期 API

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/lifecycle/status` | GET | 获取数据分层状态 |
| `/api/lifecycle/demote-to-warm` | POST | 将指定 CF 降级为温数据 |
| `/api/lifecycle/demote-to-cold` | POST | 将指定 CF 降级为冷数据 |
| `/api/lifecycle/archive` | POST | 归档指定天数以上的数据 |

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
