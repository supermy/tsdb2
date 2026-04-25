# tsdb2 技术架构文档

## 1. 系统架构图

```
┌──────────────────────────────────────────────────┐
│                 Application                       │
├──────────────────────────────────────────────────┤
│           ArrowStorageEngine (统一入口)            │
│  ┌──────────┬──────────────┬──────────────────┐  │
│  │  write() │  read_range()│  execute_sql()   │  │
│  └────┬─────┴──────┬───────┴────────┬─────────┘  │
├───────┼────────────┼────────────────┼────────────┤
│       ▼            ▼                ▼            │
│  ┌─────────┐  ┌──────────┐  ┌───────────────┐   │
│  │AsyncBuf │  │ Parquet  │  │ DataFusion    │   │
│  │ + WAL   │  │ Reader   │  │ Engine        │   │
│  └────┬────┘  └────┬─────┘  └───────┬───────┘   │
│       ▼            ▼                ▼            │
│  ┌─────────┐  ┌──────────┐  ┌───────────────┐   │
│  │ Parquet │  │ Partition│  │ TableProvider │   │
│  │ Writer  │  │ Manager  │  │ + UDF         │   │
│  └─────────┘  └──────────┘  └───────────────┘   │
├──────────────────────────────────────────────────┤
│              tsdb-arrow (数据模型)                 │
│  DataPoint | FieldValue | Schema | Converter     │
├──────────────────────────────────────────────────┤
│         Arrow | Parquet | DataFusion             │
└──────────────────────────────────────────────────┘
```

## 1.1 RocksDB 引擎架构

```
┌──────────────────────────────────────────────────────────────┐
│                    TsdbRocksDb                                │
├──────────────────────────────────────────────────────────────┤
│  写入 API                                                     │
│  ┌──────────┬──────────────┬──────────────────────────┐      │
│  │ put()    │ merge()      │ write_batch()             │      │
│  │ 单点写入  │ 字段合并写入  │ 批量写入 (Tags 去重优化)   │      │
│  └────┬─────┴──────┬───────┴────────────┬─────────────┘      │
├───────┼────────────┼────────────────────┼───────────────────┤
│  读取 API                                                     │
│  ┌──────────┬──────────────┬──────────────┬────────────┐    │
│  │ get()    │ multi_get()  │ read_range() │prefix_scan │    │
│  │ 单点查询  │ 批量点查(3x) │ 范围查询      │ 前缀扫描   │    │
│  └──────────┴──────────────┴──────────────┴────────────┘    │
├──────────────────────────────────────────────────────────────┤
│  管理 API                                                     │
│  ┌──────────┬──────────────┬──────────────┬────────────┐    │
│  │ compact  │ drop_cf      │ snapshot()   │ doctor     │    │
│  │ 压缩     │ 删除CF       │ 一致性快照    │ 健康检查   │    │
│  └──────────┴──────────────┴──────────────┴────────────┘    │
├──────────────────────────────────────────────────────────────┤
│  Column Family 架构                                           │
│  ┌──────────────────────────────────────────────────────┐    │
│  │ _series_meta: tags_hash → tags 映射 (全局唯一)        │    │
│  │ ts_cpu_20260418: Key=hash+ts, Val=fields (按日分区)   │    │
│  │ ts_cpu_20260419: Key=hash+ts, Val=fields              │    │
│  │ ts_memory_20260419: Key=hash+ts, Val=fields           │    │
│  └──────────────────────────────────────────────────────┘    │
├──────────────────────────────────────────────────────────────┤
│  RocksDB 内部                                                 │
│  ┌─────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐    │
│  │MemTable │ │L0 SST    │ │L1+ SST   │ │ Block Cache  │    │
│  │(内存)    │ │(无序)     │ │(有序)     │ │ (LRU 32MB)  │    │
│  └─────────┘ └──────────┘ └──────────┘ └──────────────┘    │
│  ┌──────────────────────────────────────────────────────┐    │
│  │ WAL (预写日志) | Bloom Filter | CompactionFilter     │    │
│  └──────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
```

## 2. 类图

### tsdb-arrow

```
┌──────────────────────┐
│      DataPoint       │
├──────────────────────┤
│ measurement: String  │
│ tags: BTreeMap       │
│ fields: BTreeMap     │
│ timestamp: i64       │
├──────────────────────┤
│ + new()              │
│ + with_tag()         │
│ + with_field()       │
│ + series_key()       │
└──────────────────────┘
         │
         ▼
┌──────────────────────┐
│     FieldValue       │
├──────────────────────┤
│ Float(f64)           │
│ Integer(i64)         │
│ String(String)       │
│ Boolean(bool)        │
├──────────────────────┤
│ + as_f64()           │
│ + as_i64()           │
│ + as_str()           │
│ + as_bool()          │
└──────────────────────┘

┌──────────────────────┐
│  TsdbSchemaBuilder   │
├──────────────────────┤
│ + new(measurement)   │
│ + with_tag_key()     │
│ + with_float_field() │
│ + with_int_field()   │
│ + with_string_field()│
│ + with_bool_field()  │
│ + compact()          │
│ + build() → SchemaRef│
└──────────────────────┘

┌──────────────────────┐
│   TsdbMemoryPool     │
├──────────────────────┤
│ + new(limit)         │
│ + allocate(n)        │
│ + release(n)         │
│ + used() → usize     │
│ + available() → usize│
└──────────────────────┘
```

### tsdb-rocksdb

```
┌──────────────────────────────────────────────────────┐
│                    TsdbRocksDb                        │
├──────────────────────────────────────────────────────┤
│ - db: DB                                             │
│ - config: RocksDbConfig                              │
│ - cache: Cache                                       │
│ - base_dir: PathBuf                                  │
├──────────────────────────────────────────────────────┤
│ + open(path, config) → Result<Self>                  │
│ + put(measurement, tags, ts, fields) → Result        │
│ + merge(measurement, tags, ts, fields) → Result      │
│ + write_batch(dps) → Result             [Tags 去重]  │
│ + get(measurement, tags, ts) → Result<Option<DP>>    │
│ + multi_get(measurement, keys) → Result<Vec<Option>> [批量点查] │
│ + read_range(measurement, start, end) → Result<Vec>  │
│ + prefix_scan(measurement, tags, start, end)         │
│ + snapshot() → TsdbSnapshot                          │
│ + drop_cf(cf_name) → Result                         │
│ + compact_cf(cf_name) → Result                      │
│ + list_ts_cfs() → Vec<String>                       │
│ + stats() → String                                  │
│ + cf_stats(cf_name) → Option<String>                │
└──────────────────────────────────────────────────────┘

┌──────────────────────┐     ┌──────────────────────┐
│      TsdbKey         │     │    RocksDbConfig     │
├──────────────────────┤     ├──────────────────────┤
│ tags_hash: u64       │     │ cache_size: usize    │
│ timestamp: i64       │     │ cf_write_buffer_size │
├──────────────────────┤     │ cf_max_bytes_level   │
│ + new() → Self       │     │ default_ttl_secs     │
│ + encode() → Vec<u8> │     └──────────────────────┘
│ + decode() → Result  │
│ + prefix_encode()    │
└──────────────────────┘
```

### tsdb-parquet

```
┌──────────────────────┐     ┌──────────────────────┐
│  TsdbParquetWriter   │     │  TsdbParquetReader   │
├──────────────────────┤     ├──────────────────────┤
│ - partition_manager  │     │ - partition_manager  │
│ - config             │     ├──────────────────────┤
│ - buffers            │     │ + new(pm)            │
│ - schema             │     │ + read_range()       │
├──────────────────────┤     │ + read_range_arrow() │
│ + new(pm, config)    │     │ + get_point()        │
│ + write(dp)          │     │ + read_parquet_file()│
│ + write_batch(dps)   │     │ + read_all_datapoints│
│ + flush_all()        │     └──────────────────────┘
└──────────────────────┘

┌──────────────────────┐     ┌──────────────────────┐
│  PartitionManager    │     │      TsdbWAL         │
├──────────────────────┤     ├──────────────────────┤
│ - base_dir           │     │ - path               │
│ - config             │     │ - file               │
│ - known_partitions   │     │ - sequence           │
├──────────────────────┤     ├──────────────────────┤
│ + new(dir, config)   │     │ + create(path)       │
│ + refresh()          │     │ + append(type, data) │
│ + ensure_partition() │     │ + sync()             │
│ + get_partitions()   │     │ + recover(path)      │
│ + cleanup_expired()  │     └──────────────────────┘
│ + list_parquet_files │
└──────────────────────┘

┌──────────────────────┐
│   ParquetCompactor   │
├──────────────────────┤
│ + new(pm, config)    │
│ + compact_partition()│
│ + compact_all()      │
└──────────────────────┘
```

### tsdb-datafusion

```
┌──────────────────────┐     ┌──────────────────────┐
│ DataFusionQueryEngine│     │  TsdbTableProvider   │
├──────────────────────┤     ├──────────────────────┤
│ - ctx: SessionContext│     │ - schema: SchemaRef  │
│ - base_dir: PathBuf  │     │ - measurement: String│
├──────────────────────┤     │ - base_dir: PathBuf  │
│ + new(base_dir)      │     ├──────────────────────┤
│ + register_measure() │     │ + new(name, schema)  │
│ + register_from_dp() │     │ + from_datapoints()  │
│ + execute(sql)       │     │ + scan() [TableProv] │
│ + execute_arrow(sql) │     └──────────────────────┘
└──────────────────────┘

┌──────────────────────┐
│   time_bucket UDF    │
├──────────────────────┤
│ time_bucket(ts, int) │
│ → Timestamp bucketed │
└──────────────────────┘
```

### tsdb-storage-arrow

```
┌──────────────────────┐     ┌──────────────────────┐
│ ArrowStorageEngine   │     │   WriteBuffer        │
├──────────────────────┤     ├──────────────────────┤
│ - partition_manager  │     │ - buffers: BTreeMap  │
│ - _reader            │     │ - max_buffer_rows    │
│ - compactor          │     │ - total_rows         │
│ - wal                │     ├──────────────────────┤
│ - async_writer       │     │ + new(max_rows)      │
│ - query_engine       │     │ + write(dp)          │
│ - _config            │     │ + write_batch(dps)   │
│ - base_dir           │     │ + drain()            │
├──────────────────────┤     │ + total_rows()       │
│ + open(path, config) │     └──────────────────────┘
│ + write(dp)          │
│ + write_batch(dps)   │     ┌──────────────────────┐
│ + read_range()       │     │  AsyncWriteBuffer    │
│ + get_point()        │     ├──────────────────────┤
│ + execute_sql()      │     │ - inner: Mutex<Buf>  │
│ + flush()            │     │ - writer: Mutex<Wr>  │
│ + compact()          │     │ - handle: JoinHandle │
│ + cleanup()          │     ├──────────────────────┤
└──────────────────────┘     │ + new(writer, config)│
                             │ + write(dp)          │
                             │ + write_batch(dps)   │
                             │ + flush()            │
                             │ + stop()             │
                             └──────────────────────┘
```

## 3. 写入流程时序图

```mermaid
sequenceDiagram
    participant App
    participant Engine as ArrowStorageEngine
    participant WAL as TsdbWAL
    participant Buffer as AsyncWriteBuffer
    participant Writer as TsdbParquetWriter
    participant FS as FileSystem

    App->>Engine: write_batch(datapoints)
    Engine->>WAL: append(Insert, payload)
    WAL->>FS: append to WAL file
    Engine->>Buffer: write_batch(datapoints)
    Buffer->>Buffer: lock & add to WriteBuffer

    Note over Buffer: 定时/缓冲满触发
    Buffer->>Writer: drain() → write_batch()
    Writer->>Writer: DataPoint → RecordBatch
    Writer->>FS: flush_all() → write Parquet
    FS-->>Writer: 文件写入完成
```

## 4. 读取流程时序图

```mermaid
sequenceDiagram
    participant App
    participant Engine as ArrowStorageEngine
    participant Buffer as AsyncWriteBuffer
    participant PM as PartitionManager
    participant Reader as TsdbParquetReader
    participant FS as FileSystem

    App->>Engine: read_range(measurement, tags, start, end)
    Engine->>Buffer: flush()
    Engine->>PM: new(base_dir)
    PM->>PM: refresh() → discover partitions
    Engine->>Reader: new(pm)
    Reader->>PM: get_partitions_in_range(start_date, end_date)
    PM-->>Reader: List<PartitionInfo>
    loop 每个分区
        Reader->>PM: list_parquet_files(date)
        PM-->>Reader: List<PathBuf>
        loop 每个文件
            Reader->>FS: read Parquet file
            FS-->>Reader: RecordBatch
            Reader->>Reader: RecordBatch → DataPoint
            Reader->>Reader: tag 过滤 + 时间过滤
        end
    end
    Reader-->>Engine: Vec<DataPoint>
    Engine-->>App: Vec<DataPoint>
```

## 5. SQL 查询时序图

```mermaid
sequenceDiagram
    participant App
    participant Engine as ArrowStorageEngine
    participant DF as DataFusionQueryEngine
    participant TP as TsdbTableProvider
    participant MT as MemTable
    participant DFExec as DataFusion Executor

    App->>Engine: execute_sql(sql, measurement, dps)
    Engine->>Buffer: flush()
    Engine->>DF: register_from_datapoints(measurement, dps)
    DF->>TP: from_datapoints(measurement, dps)
    TP-->>DF: TsdbTableProvider
    DF->>DF: ctx.register_table(measurement, provider)
    App->>DF: execute(sql)
    DF->>DF: ctx.sql(sql) → DataFrame
    DF->>TP: scan(projection, filters, limit)
    TP->>TP: load_data() → read from Parquet
    TP->>MT: MemTable::try_new(schema, partitions)
    MT-->>TP: MemTable
    TP-->>DF: ExecutionPlan
    DF->>DFExec: collect()
    DFExec-->>DF: Vec<RecordBatch>
    DF->>DF: extract columns & rows
    DF-->>App: QueryResult { columns, rows }
```

## 6. Compaction 流程图

```mermaid
flowchart TD
    A[compact_partition date] --> B[list_parquet_files date]
    B --> C{files.len >= min_files?}
    C -->|No| D[返回 None]
    C -->|Yes| E[读取所有文件 → RecordBatch]
    E --> F[合并 RecordBatch]
    F --> G[写入新文件 cold_writer_props ZSTD]
    G --> H[删除旧文件]
    H --> I[返回 CompactionResult]
```

## 7. 数据模型 ER 图

```mermaid
erDiagram
    DataPoint {
        String measurement
        BTreeMap tags
        BTreeMap fields
        i64 timestamp
    }
    FieldValue {
        f64 Float
        i64 Integer
        String String
        bool Boolean
    }
    Tags {
        String host
        String region
    }
    DataPoint ||--o{ FieldValue : contains
    DataPoint ||--o{ Tags : has
```

## 8. Schema 模式

### Extended Schema

```
| timestamp | measurement | tags_hash | tag_keys | tag_values | usage | idle | count |
```

适用于 tag key 不固定的场景，所有 tag 存储为列表。

### Compact Schema

```
| timestamp | tag_host | tag_region | usage | idle | count |
```

适用于 tag key 固定的场景，每个 tag 作为独立列，查询性能更优。
