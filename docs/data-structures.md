# tsdb2 数据结构详述

## 1. 核心数据模型 (tsdb-arrow)

### 1.1 DataPoint — 时序数据点

```rust
pub struct DataPoint {
    pub measurement: String,    // 度量名 (如 "cpu", "memory")
    pub tags: Tags,             // 标签集合 (标识 series)
    pub fields: Fields,         // 字段集合 (实际数据值)
    pub timestamp: i64,         // 微秒级时间戳
}
```

**设计决策**:
- `measurement` 为 String 而非 enum，支持用户自定义度量名
- `tags` 使用 BTreeMap 保证序列化顺序一致性，确保同一组 tags 无论插入顺序如何，序列化结果相同
- `fields` 使用 BTreeMap，字段名有序，便于跨数据点对比
- `timestamp` 使用 i64 微秒精度，覆盖范围 ±292271 年，满足时序场景需求

**内存布局**:
```
DataPoint (堆分配)
├── measurement: String (24 bytes: ptr + len + cap)
├── tags: BTreeMap<String, String> (红黑树节点，每个节点 ~56 bytes)
├── fields: BTreeMap<String, FieldValue> (红黑树节点，每个节点 ~64 bytes)
└── timestamp: i64 (8 bytes)
```

### 1.2 Tags — 标签集合

```rust
pub type Tags = BTreeMap<String, String>;
```

**特性**:
- 有序: 按 key 字典序排列，序列化结果确定
- 去重: 同一 key 后写入覆盖先写入
- 哈希一致性: `compute_tags_hash()` 对排序后的 key-value 对计算 FxHash，顺序无关

**编码格式** (二进制):
```
[num_tags: u16][key_len: u16][key_bytes][val_len: u16][val_bytes]...
```

### 1.3 Fields — 字段集合

```rust
pub type Fields = BTreeMap<String, FieldValue>;
```

**编码格式** (二进制):
```
[num_fields: u16][field_name_len: u16][name_bytes][type_tag: u8][value_bytes]...
```

类型标签:
| type_tag | 类型 | value_bytes |
|----------|------|-------------|
| 0 | Float | 8 bytes (f64 LE) |
| 1 | Integer | 8 bytes (i64 LE) |
| 2 | String | [len: u32][bytes] |
| 3 | Boolean | 1 byte (0/1) |

### 1.4 FieldValue — 字段值枚举

```rust
pub enum FieldValue {
    Float(f64),       // 64-bit 浮点数
    Integer(i64),     // 64-bit 整数
    String(String),   // UTF-8 字符串
    Boolean(bool),    // 布尔值
}
```

**序列化兼容性**:
- 序列化 (serde): 使用枚举格式 `{"Float": 1.0}`, `{"Integer": 42}`
- 反序列化: 自定义 Visitor，同时支持枚举格式和普通值格式 (`1.0` → Float, `42` → Integer)
- 类型推断优先级: Float > Integer > Boolean > String

**Arrow 类型映射**:
| FieldValue | Arrow DataType |
|------------|---------------|
| Float(f64) | Float64 |
| Integer(i64) | Int64 |
| String(String) | Utf8 |
| Boolean(bool) | Boolean |

---

## 2. RocksDB 存储结构 (tsdb-rocksdb)

### 2.1 TsdbKey — 时序数据键

```rust
pub struct TsdbKey {
    pub tags_hash: u64,     // 标签哈希 (8 bytes, 大端序)
    pub timestamp: i64,     // 微秒时间戳 (8 bytes, 大端序)
}
```

**编码格式** (16 bytes, big-endian):
```
┌──────────────────────┬──────────────────────┐
│   tags_hash (u64)    │   timestamp (i64)    │
│     8 bytes BE       │     8 bytes BE       │
└──────────────────────┴──────────────────────┘
```

**排序语义**: 先按 tags_hash 升序，再按 timestamp 升序。同一 series 的数据在 SST 文件中连续存储。

**前缀编码** (8 bytes, 用于前缀扫描):
```
┌──────────────────────┐
│   tags_hash (u64)    │
│     8 bytes BE       │
└──────────────────────┘
```

**常量**:
- `TAGS_HASH_SIZE = 8` — 标签哈希字节数
- `TIMESTAMP_SIZE = 8` — 时间戳字节数
- `KEY_SIZE = 16` — 完整键长度

### 2.2 Tags Hash 计算

```rust
pub fn compute_tags_hash(tags: &Tags) -> u64;
```

**算法**: FxHash (Rust 标准库使用的快速非加密哈希)
- 对排序后的 key-value 对依次哈希
- 确定性: 同一组 tags 无论插入顺序，哈希结果相同
- 冲突率: 64-bit 空间，实际场景冲突概率极低

### 2.3 Column Family 架构

```
_series_meta              — 全局唯一, 存储 tags_hash → tags 映射
ts_{measurement}_{date}   — 按度量名+日期分区
```

**_series_meta CF**:
```
Key: tags_hash (8 bytes, BE)
Val: [num_tags: u16][key_len: u16][key][val_len: u16][val]...
```

**ts_{measurement}_{date} CF**:
```
Key: TsdbKey (16 bytes: tags_hash + timestamp)
Val: [num_fields: u16][field_name_len: u16][name][type: u8][value]...
```

### 2.4 Value 编码 (字段值二进制格式)

```
┌──────────────┬──────────────────────────────────────────┐
│ num_fields   │ field_entry[0] │ field_entry[1] │ ...    │
│ u16 (2B)     │ 变长           │ 变长           │        │
└──────────────┴──────────────────────────────────────────┘

field_entry:
┌──────────────┬──────────┬──────────┬──────────┐
│ name_len     │ name     │ type_tag │ value    │
│ u16 (2B)     │ [bytes]  │ u8 (1B)  │ 变长     │
└──────────────┴──────────┴──────────┴──────────┘
```

---

## 3. RocksDB 引擎 API 数据流

### 3.1 写入路径

```
DataPoint
  │
  ├─ compute_tags_hash(tags) → tags_hash: u64
  ├─ TsdbKey::new(tags_hash, timestamp).encode() → key: [u8; 16]
  ├─ encode_fields(fields) → value: Vec<u8>
  │
  ├─ WriteBatch.put_cf(data_cf, key, value)
  └─ WriteBatch.put_cf(meta_cf, tags_hash_be, encode_tags(tags))
       │
       └─ db.write(batch)  ← 原子提交
```

### 3.2 读取路径

```
get(measurement, tags, timestamp)
  │
  ├─ compute_tags_hash(tags) → tags_hash
  ├─ TsdbKey::new(tags_hash, timestamp).encode() → key
  ├─ micros_to_date(timestamp) → date → cf_name
  ├─ db.get_pinned_cf(data_cf, key) → Option<PinnableSlice>
  │
  └─ decode_fields(value) → Fields → DataPoint
```

### 3.3 MultiGET 读取路径

```
multi_get(measurement, keys: &[(Tags, i64)])
  │
  ├─ 按 CF 分组 keys (同一天的数据在同一 CF)
  ├─ 对每组:
  │   ├─ 编码所有 keys → Vec<Vec<u8>>
  │   ├─ db.multi_get_cf(keys) → Vec<Result<Option<Vec<u8>>>>
  │   └─ 解码 values + 解析 tags (从 _series_meta)
  │
  └─ 返回 Vec<Option<DataPoint>> (与输入 keys 等长)
```

### 3.4 范围查询路径

```
read_range(measurement, start, end)
  │
  ├─ micros_to_date(start) → start_date
  ├─ micros_to_date(end) → end_date
  │
  ├─ 遍历 [start_date, end_date] 每一天:
  │   ├─ cf_name = ts_{measurement}_{date}
  │   ├─ db.iterator_cf(data_cf, IteratorMode::Start)
  │   └─ 过滤 timestamp ∈ [start, end]
  │
  └─ results.sort_by_key(|dp| dp.timestamp)
```

### 3.5 前缀扫描路径

```
prefix_scan(measurement, tags, start, end)
  │
  ├─ compute_tags_hash(tags) → tags_hash
  ├─ 遍历 [start_date, end_date] 每一天:
  │   ├─ start_key = TsdbKey(tags_hash, start)
  │   ├─ end_key = TsdbKey(tags_hash, end + 1)
  │   ├─ ReadOptions.set_iterate_range(start_key..end_key)
  │   └─ iterator_cf_opt → 只扫描 tags_hash 匹配的行
  │
  └─ 直接返回 (已按 timestamp 排序)
```

---

## 4. Arrow Schema 映射

### 4.1 Extended Schema

```
| timestamp (Int64) | measurement (Utf8) | tags_hash (UInt64) | tag_keys (List<Utf8>) | tag_values (List<Utf8>) | usage (Float64) | ... |
```

适用于 tag key 不固定的场景，所有 tag 存储为列表。

### 4.2 Compact Schema

```
| timestamp (Int64) | tag_host (Utf8) | tag_region (Utf8) | usage (Float64) | idle (Float64) | count (Int64) |
```

适用于 tag key 固定的场景，每个 tag 作为独立列，查询性能更优。

---

## 5. 配置数据结构

### 5.1 RocksDbConfig

```rust
pub struct RocksDbConfig {
    pub cache_size: usize,                  // Block cache 大小 (默认 512MB)
    pub block_size: usize,                  // SST Block 大小 (默认 4KB)
    pub write_buffer_size: usize,           // 全局 Write Buffer 大小 (默认 256MB)
    pub allow_stall: bool,                  // 内存超限时是否 stall (默认 false)
    pub cf_write_buffer_size: usize,        // CF 写缓冲区大小 (默认 64MB)
    pub max_write_buffer_number: i32,       // CF 最大 Write Buffer 数 (默认 4)
    pub cf_max_bytes_for_level_base: u64,   // L1 层大小基准 (默认 256MB)
    pub default_retention_secs: u64,        // 默认保留天数 (默认 7天)
    pub default_ttl_secs: u64,             // 默认 TTL 秒数 (默认 30天)
    pub compression_l0_l2: DBCompressionType, // L0-L2 压缩 (默认 None)
    pub compression_l3_plus: DBCompressionType, // L3+ 压缩 (默认 Zstd)
    pub bloom_filter_bits_per_key: i32,     // Bloom 位数/Key (默认 10)
    pub bloom_filter_block_based: bool,     // Block-Based Bloom (默认 false)
    pub memtable_prefix_bloom_ratio: f64,   // MemTable 前缀 Bloom 比例 (默认 0.1)
    pub level0_file_num_compaction_trigger: i32, // L0 Compaction 触发 (默认 4)
    pub level0_slowdown_writes_trigger: i32,     // L0 慢写触发 (默认 20)
    pub level0_stop_writes_trigger: i32,         // L0 停写触发 (默认 36)
    pub optimize_meta_cf: bool,             // Meta CF 优化 (默认 true)
}
```

### 5.2 ArrowStorageConfig

```rust
pub struct ArrowStorageConfig {
    pub hot_days: u64,              // 热数据天数 (默认 7)
    pub retention_days: u64,        // 保留天数 (默认 30)
    pub max_buffer_rows: usize,     // 缓冲区最大行数 (默认 100K)
    pub flush_interval_ms: u64,     // 刷盘间隔 (默认 5000ms)
    pub max_rows_per_file: usize,   // 单文件最大行数 (默认 1M)
    pub row_group_size: usize,      // Row Group 大小 (默认 1M)
    pub wal_enabled: bool,          // WAL 开关 (默认 true)
    pub wal_sync_interval_ms: u64,  // WAL sync 间隔 (默认 100ms)
    pub memory_limit: usize,        // 内存限制 (默认 1GB)
}
```
