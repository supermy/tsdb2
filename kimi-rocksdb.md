我将基于 RocksDB 的核心架构（LSM-Tree、Column Family、WAL、Compaction），设计一套对标 Arrow+Parquet+DataFusion 的时序存储引擎。这个设计会保留 RocksDB 的工程优势，同时引入列式存储和向量化查询能力。

---

## 整体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           查询层 (Query Layer)                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │  SQL Parser │→ │ Logical Plan│→ │  Optimizer  │→ │  Physical Executor  │ │
│  │ (sqlparser) │  │   Builder   │  │ (CBO/RBO)   │  │  (Vectorized Exec)  │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        执行引擎 (Execution Engine)                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                    Columnar RecordBatch Stream                          │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │ │
│  │  │  Filter  │→ │ Project  │→ │   Join   │→ │ Aggregate│→ │   Sort   │   │ │
│  │  │ Pushdown │  │ Pushdown │  │  (Hash)  │  │ (Vector) │  │(External)│   │ │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        存储引擎 (Storage Engine)                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                    Column Family: TimeSeries (默认)                    │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │ │
│  │  │  MemTable (Columnar Arrow Format)                               │   │ │
│  │  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌────────────┐ │   │ │
│  │  │  │ Timestamp   │ │  Metric_1   │ │  Metric_2   │ │  Tags      │ │   │ │
│  │  │  │  (Int64)    │ │  (Float64)  │ │  (Float64)  │ │(Dictionary)│ │   │ │
│  │  │  │  Sorted     │ │  NullBitmap │ │  NullBitmap │ │            │ │   │ │
│  │  │  └─────────────┘ └─────────────┘ └─────────────┘ └────────────┘ │   │ │
│  │  └─────────────────────────────────────────────────────────────────┘   │ │
│  │                              ↓ Flush                                  │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │ │
│  │  │  SSTable (Parquet-like Columnar Format)                         │   │ │
│  │  │  ┌─────────────────────────────────────────────────────────┐   │   │ │
│  │  │  │  Row Group 0 (128MB)                                    │   │   │ │
│  │  │  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐ │   │   │ │
│  │  │  │  │ Column Chunk│ │ Column Chunk│ │ Column Chunk        │ │   │   │ │
│  │  │  │  │ (Timestamp) │ │ (Metric_1)  │ │ (Tags Dict+Index)   │ │   │   │ │
│  │  │  │  │ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ ┌─────┐ │ │   │   │ │
│  │  │  │  │ │Data Page│ │ │ │Data Page│ │ │ │Dict Page│ │Index│ │ │   │   │ │
│  │  │  │  │ │(RLE+Delta)│ │ │ │(Gorilla)│ │ │ │(Plain)  │ │(BitP)│ │ │   │   │ │
│  │  │  │  │ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ └─────┘ │ │   │   │ │
│  │  │  │  └─────────────┘ └─────────────┘ └─────────────────────┘ │   │   │ │
│  │  │  │  [Statistics: Min/Max/NullCount per Column Chunk]        │   │   │ │
│  │  │  └─────────────────────────────────────────────────────────┘   │   │ │
│  │  │  Row Group 1...                                                 │   │ │
│  │  └─────────────────────────────────────────────────────────────────┘   │ │
│  │                              ↓ Compaction                             │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │ │
│  │  │  Tiered Storage (L0→L1→L2→L3)                                  │   │ │
│  │  │  L0: Recent SSTables (1h retention, uncompressed)              │   │ │
│  │  │  L1: Hourly compaction (ZSTD level 3)                         │   │ │
│  │  │  L2: Daily compaction (ZSTD level 9 + Dictionary encoding)    │   │ │
│  │  │  L3: Cold archive (ZSTD level 22 + Bit packing)               │   │ │
│  │  └─────────────────────────────────────────────────────────────────┘   │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                    Column Family: Metadata (Index)                       │ │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐  │ │
│  │  │ Series Index│ │  Tag Index  │ │ Time Index  │ │  Bloom Filter   │  │ │
│  │  │(Inverted)   │ │(B+ Tree)    │ │(Interval Tree)│ │ (Per SSTable)   │  │ │
│  │  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────────┘  │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        文件系统层 (File System Layer)                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │  WAL Files │  │ SST Files   │  │ Manifest    │  │  Object Storage     │ │
│  │  (binlog)  │  │ (.tsst)     │  │ (JSON)      │  │  (S3/MinIO Tier)    │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 核心组件设计

### 1. 内存结构：Columnar MemTable

不同于 RocksDB 的 SkipList，采用 **Apache Arrow 列式内存格式**：

```rust
/// 时序数据专用 MemTable，基于 Arrow RecordBatch
pub struct TimeSeriesMemTable {
    /// 时间戳列 (Int64, nanoseconds since epoch)
    timestamp: TimestampArray,
    
    /// 指标列 (Float64, 支持 SIMD)
    metrics: Vec<Float64Array>,
    
    /// 标签列 (Dictionary<UInt16, Utf8>, 高压缩比)
    tags: Vec<DictionaryArray<UInt16Type>>,
    
    /// 每个时间序列的元数据
    series_metadata: HashMap<SeriesId, SeriesMeta>,
    
    /// 内存使用限制 (默认 64MB)
    memory_limit: usize,
    
    /// 当前已用内存
    memory_usage: AtomicUsize,
}

impl TimeSeriesMemTable {
    /// 写入数据 - 按时间序列分组，保持列式布局
    pub fn write(&mut self, batch: TimeSeriesBatch) -> Result<()> {
        // 1. 按 (metric_name, tags) 分组到 series_id
        // 2. 追加到对应列数组
        // 3. 维护排序：每个 series 内部按时间戳排序
        // 4. 检查内存阈值，触发 flush
    }
    
    /// 扫描数据 - 返回 Arrow RecordBatch Stream
    pub fn scan(&self, range: TimeRange, projection: &[String]) -> RecordBatchStream {
        // 使用 Arrow 的 SIMD 优化进行列扫描
    }
}
```

**关键优化**：
- **时间戳排序**：每个时间序列内部保持有序，支持二分查找
- **字典编码**：标签列使用 Arrow Dictionary 类型，重复字符串只存一次
- **零拷贝扫描**：直接返回 Arrow Array 引用，避免序列化开销

### 2. 磁盘格式：TSST (TimeSeries SSTable)

对标 Parquet，但针对时序数据优化：

```
TSST File Layout:
┌─────────────────────────────────────────────────────────────┐
│                    Header (16 bytes)                        │
│  Magic: "TSST" (4 bytes)                                    │
│  Version: 1 (4 bytes)                                       │
│  Row Group Count: N (4 bytes)                               │
│  Footer Offset: u64 (8 bytes)                               │
├─────────────────────────────────────────────────────────────┤
│                    Row Group 0                                │
│  ┌───────────────────────────────────────────────────────┐ │
│  │  Column Chunk 0: Timestamp                              │ │
│  │  ┌─────────────────┐ ┌─────────────────┐ ┌────────────┐ │ │
│  │  │ Data Page 0     │ │ Data Page 1     │ │ ...        │ │ │
│  │  │ [Delta-of-Delta │ │ [Delta-of-Delta │ │            │ │ │
│  │  │  + RLE]         │ │  + RLE]         │ │            │ │ │
│  │  └─────────────────┘ └─────────────────┘ └────────────┘ │ │
│  │  Statistics: {min, max, count, distinct_count}          │ │
│  ├───────────────────────────────────────────────────────┤ │
│  │  Column Chunk 1: Metric Value (Float64)                 │ │
│  │  ┌─────────────────┐ ┌─────────────────┐ ┌────────────┐ │ │
│  │  │ Data Page 0     │ │ Data Page 1     │ │ ...        │ │ │
│  │  │ [Gorilla XOR    │ │ [Gorilla XOR    │ │            │ │ │
│  │  │  Encoding]      │ │  Encoding]      │ │            │ │ │
│  │  └─────────────────┘ └─────────────────┘ └────────────┘ │ │
│  │  Statistics: {min, max, sum, count, null_count}         │ │
│  ├───────────────────────────────────────────────────────┤ │
│  │  Column Chunk 2: Tags (Dictionary)                      │ │
│  │  ┌─────────────────┐ ┌─────────────────┐                 │ │
│  │  │ Dictionary Page │ │ Data Page (Ids) │                 │ │
│  │  │ [All unique     │ │ [Bit-packed     │                 │ │
│  │  │  tag values]    │ │  uint16 ids]    │                 │ │
│  │  └─────────────────┘ └─────────────────┘                 │ │
│  └───────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    Row Group 1...                           │
├─────────────────────────────────────────────────────────────┤
│                    Footer                                   │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ Schema (FlatBuffers)                                   │ │
│  │ Row Group Metadata (offsets, sizes, statistics)        │ │
│  │ Bloom Filter Data (for point lookups)                  │ │
│  │ Time Range Index (min/max per Row Group)               │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**编码策略**：

| 数据类型 | 编码方式 | 说明 |
|---------|---------|------|
| 时间戳 | Delta-of-Delta + RLE | 时序数据通常间隔固定，压缩率极高 |
| Float64 指标 | Gorilla XOR | Facebook 时序数据库同款，适合缓慢变化的浮点数 |
| 标签 | Dictionary + Bit-pack | 低基数字符串的最佳方案 |
| 冷数据 | ZSTD (level 9-22) | 高压缩比，适合归档层 |

### 3. 分层存储与 Compaction 策略

继承 RocksDB 的 LSM-Tree 层级，但针对时序特性调整：

```
Level Structure (Time-Aware Tiered Compaction):

L0 (Hot):  最近1小时数据，未压缩，保留原始精度
           ├── 每10分钟一个 SST 文件
           └── 支持高效的时间范围查询

L1 (Warm):  1小时~1天数据，ZSTD level 3
           ├── 每小时合并为一个 SST
           └── 保留降采样前的原始数据

L2 (Cold):  1天~30天数据，ZSTD level 9 + Dictionary
           ├── 每日合并，启用字典编码
           └── 可选：1分钟→5分钟降采样

L3 (Archive): 30天以上数据，ZSTD level 22 + Aggressive downsampling
           ├── 每周合并，启用位压缩
           └── 自动降采样：5分钟→1小时→1天粒度
```

**Compaction 触发条件**：

```rust
pub struct CompactionPolicy {
    /// 大小触发：L0 超过 4 个文件触发
    pub l0_file_threshold: usize,
    
    /// 时间触发：每小时强制合并 L0→L1
    pub time_based_trigger: Duration,
    
    /// 保留策略：自动删除超期数据
    pub retention_policy: RetentionPolicy,
    
    /// 降采样规则
    pub downsampling_rules: Vec<DownsamplingRule>,
}

pub struct DownsamplingRule {
    pub after: Duration,           // 数据保留多久后应用
    pub target_interval: Duration, // 目标采样间隔
    pub aggregation: Aggregation,  // avg/max/min/sum
}
```

### 4. 索引系统：Column Family 分离

利用 RocksDB 的 Column Family 机制，将索引与数据分离：

```rust
/// 默认 CF：存储原始时序数据
const DATA_CF: &str = "data";

/// 索引 CF 1：Series 倒排索引 (tag -> series_ids)
const SERIES_INDEX_CF: &str = "series_index";

/// 索引 CF 2：时间范围索引 (series_id -> time ranges)
const TIME_INDEX_CF: &str = "time_index";

/// 索引 CF 3：元数据 (series_id -> metric_name, tags, schema)
const METADATA_CF: &str = "metadata";

pub struct IndexManager {
    db: Arc<DB>,
}

impl IndexManager {
    /// 查询：tag_filter -> series_ids
    pub fn query_series(&self, filter: TagFilter) -> Result<Vec<SeriesId>> {
        // 使用 RocksDB 的 MergeOperator 维护倒排列表
    }
    
    /// 查询：series_id + time_range -> SST file list
    pub fn locate_files(&self, series: SeriesId, range: TimeRange) -> Result<Vec<FileMeta>> {
        // 使用 Interval Tree 索引定位文件
    }
}
```

### 5. 查询执行引擎：向量化 + 谓词下推

对标 DataFusion 的执行引擎，但深度集成存储层：

```rust
/// 物理执行计划节点
pub enum PhysicalPlan {
    /// 扫描 TSST 文件，返回 Arrow RecordBatch
    TsstScan {
        files: Vec<FileMeta>,
        projection: Vec<usize>,
        predicate: Option<Expr>,
        time_range: TimeRange,
    },
    
    /// 向量化过滤 (SIMD)
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    
    /// 向量化聚合
    Aggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        aggs: Vec<AggregateExpr>,
    },
    
    /// 时序专用算子：降采样 (Downsampling)
    TimeSeriesResample {
        input: Box<PhysicalPlan>,
        interval: Duration,
        aggregation: Aggregation,
    },
}

/// 执行上下文
pub struct ExecutionContext {
    /// 线程池
    runtime: Arc<Runtime>,
    
    /// 内存限制
    memory_pool: Arc<MemoryPool>,
    
    /// 缓存管理
    cache: Arc<CacheManager>,
}

impl ExecutionContext {
    /// 执行计划，返回 RecordBatch Stream
    pub async fn execute(&self, plan: PhysicalPlan) -> Result<SendableRecordBatchStream> {
        match plan {
            PhysicalPlan::TsstScan { files, projection, predicate, time_range } => {
                // 1. 并行读取多个 SST 文件
                // 2. 使用 Arrow 的 IPC 格式零拷贝传输
                // 3. 应用谓词下推，跳过不符合的 Row Group
                // 4. 使用 SIMD 进行批量解码和过滤
            }
            // ... 其他算子
        }
    }
}
```

**关键优化**：
- **谓词下推**：将 `WHERE time > x AND metric = y` 下推到 SST 扫描层，利用 Footer 中的统计信息跳过整个 Row Group
- **投影下推**：只读取需要的列，利用列式存储优势
- **并行执行**：每个 Row Group 独立解码，利用多核 CPU

### 6. WAL 与恢复机制

继承 RocksDB 的 WAL 设计，但针对列式数据优化：

```rust
pub struct TimeSeriesWAL {
    /// 当前 WAL 文件
    current_file: File,
    
    /// 已确认的最大序列号
    last_sequence: AtomicU64,
    
    /// 批量提交大小
    batch_size: usize,
}

/// WAL 条目格式 (支持列式批量写入)
#[derive(Serialize, Deserialize)]
pub struct WALEntry {
    pub sequence: u64,
    pub timestamp: u64,  // 写入时间
    pub data: ColumnarBatch,  // Arrow IPC 格式
}

impl TimeSeriesWAL {
    /// 恢复：重放 WAL 重建 MemTable
    pub fn recover(&self, memtable: &mut TimeSeriesMemTable) -> Result<()> {
        // 1. 读取所有 WAL 文件
        // 2. 按 sequence 排序
        // 3. 反序列化 Arrow IPC 数据
        // 4. 写入 MemTable
    }
}
```

---

## 与 Arrow/Parquet/DataFusion 的对比

| 特性 | Apache Arrow+Parquet+DataFusion | 本设计 (RocksDB-based TSDB) |
|------|--------------------------------|---------------------------|
| **内存格式** | Arrow RecordBatch | Arrow RecordBatch (完全一致) |
| **磁盘格式** | Parquet (通用列式) | TSST (时序优化) |
| **写入模型** | 批量导入为主 | LSM-Tree，高吞吐实时写入 |
| **更新/删除** | 不友好 (需重写文件) | LSM 天然支持覆盖写，墓碑删除 |
| **时间范围查询** | 需外部索引 | 内置时间索引，O(log n) 定位 |
| **降采样** | 外部实现 | 内置 Compaction 降采样 |
| **TTL/过期** | 外部管理 | LSM 层级自动淘汰 |
| **流式写入** | 需缓冲批量写入 | MemTable 支持高频小写入 |

---

## 代码骨架示例

```rust
// 存储引擎入口
pub struct TimeSeriesEngine {
    db: Arc<DB>,  // RocksDB 实例
    memtable: Arc<RwLock<TimeSeriesMemTable>>,
    wal: Arc<TimeSeriesWAL>,
    executor: Arc<ExecutionContext>,
    compaction_manager: Arc<CompactionManager>,
}

impl TimeSeriesEngine {
    /// 写入时序数据点
    pub async fn write(&self, points: Vec<DataPoint>) -> Result<()> {
        // 1. 写入 WAL (顺序磁盘 IO)
        let batch = self.wal.append(points).await?;
        
        // 2. 写入 MemTable (内存操作)
        let mut mem = self.memtable.write().await;
        mem.write(batch)?;
        
        // 3. 检查是否需要 flush
        if mem.should_flush() {
            drop(mem);  // 释放锁
            self.flush_memtable().await?;
        }
        
        Ok(())
    }
    
    /// 执行 SQL 查询
    pub async fn query(&self, sql: &str) -> Result<RecordBatchStream> {
        // 1. SQL 解析
        let statements = parse_sql(sql)?;
        
        // 2. 生成逻辑计划
        let logical_plan = self.create_logical_plan(&statements[0])?;
        
        // 3. 优化器
        let optimized_plan = self.optimize(logical_plan)?;
        
        // 4. 生成物理计划
        let physical_plan = self.create_physical_plan(optimized_plan)?;
        
        // 5. 执行
        self.executor.execute(physical_plan).await
    }
    
    /// 后台 Compaction
    async fn compaction_loop(&self) {
        loop {
            let task = self.compaction_manager.next_task().await;
            self.execute_compaction(task).await;
        }
    }
}
```

---

这套设计的核心优势在于：**保留 RocksDB 的高可靠写入路径和成熟的分层存储管理，同时通过 Arrow 内存格式和列式 TSST 文件格式获得分析型查询的性能**。对于时序场景（高写入、时间范围查询为主、需要 TTL），这比通用 Parquet 方案更为高效。