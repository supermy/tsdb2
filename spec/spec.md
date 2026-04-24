# TSDB2 Iceberg Table Format Simulation Spec

## 1. 目标

在 TSDB2 中使用 RocksDB 模拟 Apache Iceberg 表格式核心概念，实现 Snapshot 隔离、Manifest 元数据管理、Schema 演化、Time Travel 查询等能力，并直接对接 Parquet 数据文件，形成 **RocksDB 元数据 + Parquet 数据** 的 Iceberg 兼容架构。

## 2. 设计原则

| 原则 | 说明 |
|------|------|
| **Iceberg 兼容** | 元数据模型遵循 Iceberg V2 规范，支持未来对接 Iceberg 生态 |
| **RocksDB 元数据** | 所有元数据 (Snapshot/Manifest/Schema) 存储在 RocksDB CF 中，利用其事务和原子性 |
| **Parquet 数据** | 数据文件使用 Parquet 格式，支持列投影和谓词下推 |
| **渐进式实现** | 先实现核心 (Snapshot/Manifest/Scan)，再扩展 (Schema 演化/Delete 文件) |
| **TDD 驱动** | 每个模块先写测试，再实现功能 |

## 3. Iceberg 核心概念映射

### 3.1 Iceberg 元数据层级

```
Catalog (表入口)
  └── Metadata File (JSON, 表级元数据)
        ├── Schema (表结构定义, 支持 ID 引用和演化)
        ├── Partition Spec (分区规范, 支持 day(timestamp) 等 Transform)
        ├── Sort Order (排序规范)
        └── Snapshots[] (快照列表)
              └── Snapshot (某时刻的表状态)
                    ├── manifest_list (Avro/Parquet, 清单文件列表)
                    │     └── Manifest File (Avro/Parquet, 数据文件列表)
                    │           ├── Data File Entry (status=ADDED/EXISTING/DELETED)
                    │           │     ├── file_path (Parquet 文件路径)
                    │           │     ├── partition (分区值元组)
                    │           │     ├── record_count (行数)
                    │           │     ├── file_size_in_bytes (文件大小)
                    │           │     ├── lower_bounds / upper_bounds (列统计)
                    │           │     └── column_sizes / null_value_counts (列指标)
                    │           └── Delete File Entry (V2, 行级删除)
                    └── summary (operation=append/replace/overwrite/delete)
```

### 3.2 RocksDB CF 映射方案

| Iceberg 概念 | RocksDB 存储 | CF 名称 | Key 编码 | Value 编码 |
|-------------|-------------|---------|---------|-----------|
| Catalog | 全局元数据 | `_catalog` | `table_name` (Utf8) | TableMetadata (JSON) |
| Table Metadata | 表级元数据 | `_table_meta` | `{table}\x00meta` | TableMetadata JSON |
| Schema History | Schema 版本链 | `_table_meta` | `{table}\x00schema\x00{id}` | Schema JSON |
| Partition Spec | 分区规范 | `_table_meta` | `{table}\x00part_spec\x00{id}` | PartitionSpec JSON |
| Snapshot | 快照元数据 | `_snapshot` | `{table}\x00snap\x00{id}` | Snapshot JSON |
| Manifest List | 清单文件列表 | `_manifest_list` | `{table}\x00{snap_id}\x00{seq}` | ManifestEntry JSON |
| Manifest Entry | 数据文件条目 | `_manifest` | `{table}\x00{manifest_id}\x00{file_idx}` | DataFileEntry JSON |
| Branch/Tag | 快照引用 | `_refs` | `{table}\x00{ref_name}` | Ref JSON |

### 3.3 数据流

```
写入路径:
  DataPoint → WriteBuffer → Parquet File (data_{date}/part-{n}.parquet)
                         → DataFileEntry (含列统计) → Manifest → ManifestList → Snapshot → TableMetadata
                         → RocksDB 元数据 CF 原子提交

查询路径:
  SQL → DataFusion → IcebergTableProvider.scan()
      → load current_snapshot → load manifest_list → 过滤 manifest (分区统计)
      → 过滤 data_file (列统计 lower_bounds/upper_bounds) → 只读匹配的 Parquet 文件
      → RecordBatch → DataFusion 执行

Time Travel:
  SQL: SELECT * FROM cpu FOR SYSTEM_VERSION AS OF {snapshot_id}
  → load specified snapshot → 同上查询路径

Compaction:
  小文件合并 → 新 Parquet 文件 → 新 Manifest (旧文件标记 DELETED) → 新 Snapshot (operation=replace)
```

## 4. 核心数据结构

### 4.1 TableMetadata

```rust
struct TableMetadata {
    format_version: u32,              // 2
    table_uuid: String,               // UUID v4
    location: String,                 // 表数据根目录
    last_sequence_number: u64,        // 单调递增
    last_updated_ms: u64,             // 最后更新时间
    last_column_id: i32,              // 最大列 ID
    current_schema_id: i32,           // 当前 Schema ID
    schemas: Vec<Schema>,             // Schema 历史
    default_spec_id: i32,             // 默认分区规范 ID
    partition_specs: Vec<PartitionSpec>, // 分区规范历史
    current_snapshot_id: i64,         // 当前快照 ID (-1 表示空表)
    snapshots: Vec<Snapshot>,         // 快照列表
    snapshot_log: Vec<SnapshotLogEntry>, // 快照变更日志
    properties: BTreeMap<String, String>, // 表属性
}
```

### 4.2 Schema

```rust
struct Schema {
    schema_id: i32,
    fields: Vec<Field>,  // 与 Iceberg Schema 兼容
}

struct Field {
    id: i32,                  // 全局唯一列 ID
    name: String,
    required: bool,
    field_type: IcebergType,
    doc: Option<String>,
    initial_default: Option<serde_json::Value>,
    write_default: Option<serde_json::Value>,
}

enum IcebergType {
    Boolean, Int, Long, Float, Double,
    Decimal { precision: u8, scale: u8 },
    Date, Time, Timestamp, Timestamptz,
    String, Uuid, Binary,
    Struct { fields: Vec<Field> },
    List { element_id: i32, element_required: bool, element: Box<IcebergType> },
    Map { key_id: i32, key: Box<IcebergType>, value_id: i32, value_required: bool, value: Box<IcebergType> },
}
```

### 4.3 PartitionSpec

```rust
struct PartitionSpec {
    spec_id: i32,
    fields: Vec<PartitionField>,
}

struct PartitionField {
    source_id: i32,          // 源列 ID
    field_id: i32,           // 分区字段 ID
    name: String,            // 分区名 (如 "ts_day")
    transform: Transform,    // 转换函数
}

enum Transform {
    Identity,
    Bucket { n: u32 },
    Truncate { w: u32 },
    Year, Month, Day, Hour,
    Void,
}
```

### 4.4 Snapshot

```rust
struct Snapshot {
    snapshot_id: i64,
    parent_snapshot_id: Option<i64>,
    sequence_number: u64,
    timestamp_ms: u64,
    manifest_list: String,       // Manifest list 存储位置标识
    summary: SnapshotSummary,
    schema_id: i32,
}

struct SnapshotSummary {
    operation: String,           // "append" | "replace" | "overwrite" | "delete"
    added_data_files: Option<i32>,
    deleted_data_files: Option<i32>,
    added_records: Option<i64>,
    deleted_records: Option<i64>,
    total_data_files: Option<i32>,
    total_records: Option<i64>,
    extra: BTreeMap<String, String>,
}
```

### 4.5 ManifestEntry / DataFileEntry

```rust
struct ManifestEntry {
    status: EntryStatus,         // Existing=0, Added=1, Deleted=2
    snapshot_id: i64,
    sequence_number: Option<u64>,
    file_sequence_number: Option<u64>,
    data_file: DataFile,
}

enum EntryStatus { Existing, Added, Deleted }

struct DataFile {
    content: DataContentType,    // Data=0, PositionDeletes=1, EqualityDeletes=2
    file_path: String,
    file_format: String,         // "parquet"
    partition: PartitionData,    // 分区值元组
    record_count: i64,
    file_size_in_bytes: i64,
    column_sizes: Option<BTreeMap<i32, i64>>,
    value_counts: Option<BTreeMap<i32, i64>>,
    null_value_counts: Option<BTreeMap<i32, i64>>,
    lower_bounds: Option<BTreeMap<i32, Vec<u8>>>,
    upper_bounds: Option<BTreeMap<i32, Vec<u8>>>,
    split_offsets: Option<Vec<i64>>,
    sort_order_id: Option<i32>,
}
```

## 5. 新增 Crate: tsdb-iceberg

```
crates/tsdb-iceberg/
├── Cargo.toml
└── src/
    ├── lib.rs              — crate 入口 + 重导出
    ├── catalog.rs          — IcebergCatalog (RocksDB-backed)
    ├── table.rs            — IcebergTable (表操作入口)
    ├── snapshot.rs         — Snapshot 管理 (创建/提交/过期)
    ├── manifest.rs         — Manifest 读写 (数据文件条目管理)
    ├── scan.rs             — IcebergScan (文件规划 + 谓词下推)
    ├── schema.rs           — Schema 定义 + 演化
    ├── partition.rs        — PartitionSpec + Transform
    ├── stats.rs            — 列统计收集 (Parquet → lower/upper bounds)
    ├── commit.rs           — 原子提交 (乐观并发 + 重试)
    ├── error.rs            — 错误类型
    └── time_travel.rs      — Time Travel 查询
```

### 5.1 依赖关系

```toml
[dependencies]
tsdb-arrow = { workspace = true }
tsdb-rocksdb = { workspace = true }
tsdb-parquet = { workspace = true }
tsdb-datafusion = { workspace = true }
arrow = { workspace = true }
parquet = { workspace = true }
datafusion = { workspace = true }
rocksdb = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
uuid = { version = "1", features = ["v4"] }
```

## 6. 核心接口设计

### 6.1 IcebergCatalog

```rust
pub struct IcebergCatalog {
    db: rocksdb::DB,
    base_dir: PathBuf,
}

impl IcebergCatalog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self>;
    pub fn create_table(&self, name: &str, schema: Schema, partition_spec: PartitionSpec) -> Result<IcebergTable>;
    pub fn load_table(&self, name: &str) -> Result<IcebergTable>;
    pub fn drop_table(&self, name: &str) -> Result<()>;
    pub fn list_tables(&self) -> Result<Vec<String>>;
    pub fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()>;
}
```

### 6.2 IcebergTable

```rust
pub struct IcebergTable {
    catalog: Arc<IcebergCatalog>,
    name: String,
    metadata: TableMetadata,
}

impl IcebergTable {
    // 写入
    pub fn append(&mut self, datapoints: &[DataPoint]) -> Result<()>;
    pub fn overwrite(&mut self, datapoints: &[DataPoint], predicate: Option<Expr>) -> Result<()>;
    pub fn delete(&mut self, predicate: Expr) -> Result<()>;

    // 查询
    pub fn scan(&self) -> IcebergScanBuilder;
    pub fn scan_at_snapshot(&self, snapshot_id: i64) -> IcebergScanBuilder;

    // 元数据
    pub fn current_snapshot(&self) -> Option<&Snapshot>;
    pub fn snapshots(&self) -> &[Snapshot];
    pub fn schema(&self) -> &Schema;
    pub fn partition_spec(&self) -> &PartitionSpec;
    pub fn history(&self) -> &[SnapshotLogEntry];

    // 维护
    pub fn compact(&mut self) -> Result<()>;
    pub fn expire_snapshots(&mut self, older_than_ms: u64, min_snapshots: u32) -> Result<()>;
    pub fn update_schema(&mut self, changes: SchemaChange) -> Result<()>;
    pub fn update_partition_spec(&mut self, new_spec: PartitionSpec) -> Result<()>;
}
```

### 6.3 IcebergScanBuilder

```rust
pub struct IcebergScanBuilder<'a> {
    table: &'a IcebergTable,
    snapshot_id: Option<i64>,
    predicate: Option<Expr>,
    projection: Option<Vec<i32>>,
    case_sensitive: bool,
}

impl<'a> IcebergScanBuilder<'a> {
    pub fn predicate(mut self, expr: Expr) -> Self;
    pub fn projection(mut self, field_ids: Vec<i32>) -> Self;
    pub fn case_insensitive(mut self) -> Self;
    pub fn build(self) -> Result<IcebergScan>;
}
```

### 6.4 IcebergScan

```rust
pub struct IcebergScan {
    data_files: Vec<DataFile>,
    predicate: Option<Expr>,
    projection: Option<Vec<i32>>,
}

impl IcebergScan {
    pub fn plan(&self) -> &[DataFile];
    pub async fn execute(&self) -> Result<Vec<RecordBatch>>;
    pub fn to_execution_plan(&self) -> Arc<dyn ExecutionPlan>;
}
```

## 7. 原子提交协议

### 7.1 乐观并发提交

```
1. 读取 current TableMetadata (版本 V)
2. 创建新数据文件 (Parquet)
3. 收集列统计 → 构建 DataFileEntry
4. 创建新 Manifest (包含新 DataFileEntry)
5. 创建新 ManifestList (引用新 Manifest + 旧 Manifest)
6. 创建新 Snapshot
7. 创建新 TableMetadata (版本 V+1)
8. 原子提交: CAS(current_version=V, new_version=V+1)
   - 成功: 提交完成
   - 失败: 重试 (从步骤1重新开始)
```

### 7.2 RocksDB 原子性保证

使用 RocksDB WriteBatch 保证元数据写入原子性:

```rust
fn commit(&self, old_meta: &TableMetadata, new_meta: &TableMetadata) -> Result<()> {
    let current = self.load_metadata()?;
    if current.last_updated_ms != old_meta.last_updated_ms {
        return Err(IcebergError::CommitConflict);
    }

    let mut batch = rocksdb::WriteBatch::default();
    // 写入新 Snapshot
    batch.put_cf(snapshot_cf, snap_key, snap_json);
    // 写入新 ManifestList
    batch.put_cf(manifest_list_cf, ml_key, ml_json);
    // 写入新 Manifest entries
    for entry in new_entries {
        batch.put_cf(manifest_cf, entry_key, entry_json);
    }
    // 更新 TableMetadata
    batch.put_cf(meta_cf, meta_key, new_meta_json);

    self.db.write(batch)?;  // 原子提交
    Ok(())
}
```

## 8. 谓词下推设计

### 8.1 三级过滤

```
Level 1: Manifest 级别
  - 使用 partition field_summary (contains_null, lower_bound, upper_bound)
  - 跳过不包含匹配分区的整个 Manifest

Level 2: DataFile 级别
  - 使用 lower_bounds / upper_bounds 列统计
  - 跳过列值范围不匹配的数据文件

Level 3: Parquet RowGroup 级别
  - 使用 Parquet 文件内的 RowGroup 统计
  - 跳过不匹配的 RowGroup
```

### 8.2 过滤流程

```rust
fn plan_files(&self, predicate: &Expr) -> Vec<DataFile> {
    let snapshot = self.current_snapshot();
    let manifest_list = self.load_manifest_list(snapshot);

    let mut result = Vec::new();
    for manifest_meta in manifest_list {
        // Level 1: Manifest 级别过滤
        if !manifest_matches_predicate(&manifest_meta, predicate) {
            continue;
        }

        let entries = self.load_manifest_entries(&manifest_meta);
        for entry in entries {
            if entry.status == EntryStatus::Deleted {
                continue;
            }

            // Level 2: DataFile 级别过滤
            if !file_matches_predicate(&entry.data_file, predicate) {
                continue;
            }

            result.push(entry.data_file.clone());
        }
    }
    result
}
```

## 9. DataFusion 集成

### 9.1 IcebergTableProvider

```rust
pub struct IcebergTableProvider {
    table: Arc<Mutex<IcebergTable>>,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = self.table.lock().unwrap();
        let scan = table.scan();
        // 将 DataFusion filters 转为 Iceberg 谓词
        let predicate = filters_to_predicate(filters);
        let iceberg_scan = scan.predicate(predicate).build()?;

        // 使用 Parquet 文件列表创建 ParquetExec
        let data_files = iceberg_scan.plan();
        let plan = self.create_parquet_exec(data_files, projection, limit, state)?;
        Ok(plan)
    }
}
```

## 10. Time Travel

```sql
-- 查询特定快照
SELECT * FROM cpu FOR SYSTEM_VERSION AS OF 3051729675574597004;

-- 查询特定时间点
SELECT * FROM cpu FOR SYSTEM_TIME AS OF '2026-04-20 00:00:00';

-- 查看快照历史
SELECT * FROM cpu.snapshots;

-- 回滚到特定快照
ROLLBACK TABLE cpu TO SNAPSHOT 3051729675574597004;
```

## 11. Compaction (Iceberg 风格)

```
1. 选择小文件 (file_size < target_file_size)
2. 读取小文件内容 → 合并为大文件
3. 创建新 Manifest:
   - 旧文件标记为 DELETED
   - 新文件标记为 ADDED
4. 创建新 Snapshot (operation="replace")
5. 原子提交
6. 后台清理: 删除只被 DELETED 快照引用的物理文件
```

## 12. Schema 演化

```rust
enum SchemaChange {
    AddField { parent_id: Option<i32>, name: String, field_type: IcebergType, required: bool, write_default: Option<serde_json::Value> },
    DeleteField { field_id: i32 },
    RenameField { field_id: i32, new_name: String },
    PromoteType { field_id: i32, new_type: IcebergType },  // int→long, float→double
    MoveField { field_id: i32, new_position: usize },
}
```

## 13. 实施优先级

| Phase | 内容 | 优先级 | 依赖 |
|-------|------|--------|------|
| P1 | Catalog + TableMetadata + Schema | P0 | 无 |
| P2 | Snapshot + Manifest + DataFile | P0 | P1 |
| P3 | Append 写入 + 列统计收集 | P0 | P2 |
| P4 | Scan + 谓词下推 (三级过滤) | P0 | P3 |
| P5 | IcebergTableProvider (DataFusion) | P0 | P4 |
| P6 | Time Travel 查询 | P1 | P5 |
| P7 | Compaction (Iceberg 风格) | P1 | P3 |
| P8 | Snapshot 过期清理 | P1 | P6 |
| P9 | Schema 演化 | P2 | P5 |
| P10 | 分区演化 | P2 | P9 |
