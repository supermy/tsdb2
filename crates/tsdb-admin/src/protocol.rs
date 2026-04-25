use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AdminRequest {
    #[serde(rename = "service.list")]
    ServiceList,
    #[serde(rename = "service.create")]
    ServiceCreate {
        name: String,
        port: u16,
        config: String,
        data_dir: String,
        engine: String,
        enable_iceberg: bool,
    },
    #[serde(rename = "service.start")]
    ServiceStart { name: String },
    #[serde(rename = "service.stop")]
    ServiceStop { name: String },
    #[serde(rename = "service.restart")]
    ServiceRestart { name: String },
    #[serde(rename = "service.status")]
    ServiceStatus { name: String },
    #[serde(rename = "service.delete")]
    ServiceDelete { name: String },
    #[serde(rename = "service.logs")]
    ServiceLogs {
        name: String,
        lines: Option<usize>,
    },

    #[serde(rename = "config.list_profiles")]
    ConfigListProfiles,
    #[serde(rename = "config.get_profile")]
    ConfigGetProfile { name: String },
    #[serde(rename = "config.save_profile")]
    ConfigSaveProfile { name: String, content: String },
    #[serde(rename = "config.delete_profile")]
    ConfigDeleteProfile { name: String },
    #[serde(rename = "config.apply")]
    ConfigApply { service: String, profile: String },
    #[serde(rename = "config.compare")]
    ConfigCompare { profile_a: String, profile_b: String },

    #[serde(rename = "test.sql")]
    TestSql {
        service: String,
        sql: String,
    },
    #[serde(rename = "test.write_bench")]
    TestWriteBench {
        service: String,
        measurement: String,
        total_points: usize,
        workers: usize,
        batch_size: usize,
    },
    #[serde(rename = "test.read_bench")]
    TestReadBench {
        service: String,
        measurement: String,
        queries: usize,
        workers: usize,
    },
    #[serde(rename = "test.iceberg")]
    TestIceberg {
        service: String,
        action: String,
        params: serde_json::Value,
    },

    #[serde(rename = "metrics.health")]
    MetricsHealth { service: String },
    #[serde(rename = "metrics.stats")]
    MetricsStats { service: String },
    #[serde(rename = "metrics.timeseries")]
    MetricsTimeseries {
        service: String,
        metric: String,
        range_secs: u64,
    },
    #[serde(rename = "metrics.alerts")]
    MetricsAlerts { service: String },

    #[serde(rename = "collector.status")]
    CollectorStatus,
    #[serde(rename = "collector.configure")]
    CollectorConfigure {
        interval_secs: u64,
        enabled: bool,
    },
    #[serde(rename = "collector.start")]
    CollectorStart,
    #[serde(rename = "collector.stop")]
    CollectorStop,

    #[serde(rename = "rocksdb.stats")]
    RocksdbStats,
    #[serde(rename = "rocksdb.cf_list")]
    RocksdbCfList,
    #[serde(rename = "rocksdb.cf_detail")]
    RocksdbCfDetail { name: String },
    #[serde(rename = "rocksdb.compact")]
    RocksdbCompact { cf: Option<String> },
    #[serde(rename = "rocksdb.kv_scan")]
    RocksdbKvScan {
        cf: String,
        prefix: Option<String>,
        start_key: Option<String>,
        limit: Option<usize>,
    },
    #[serde(rename = "rocksdb.kv_get")]
    RocksdbKvGet {
        cf: String,
        key: String,
    },
    #[serde(rename = "rocksdb.series_schema")]
    RocksdbSeriesSchema,

    #[serde(rename = "parquet.list")]
    ParquetList,
    #[serde(rename = "parquet.file_detail")]
    ParquetFileDetail { path: String },
    #[serde(rename = "parquet.preview")]
    ParquetPreview { path: String, limit: Option<usize> },

    #[serde(rename = "sql.execute")]
    SqlExecute { sql: String },
    #[serde(rename = "sql.tables")]
    SqlTables,

    #[serde(rename = "lifecycle.status")]
    LifecycleStatus,
    #[serde(rename = "lifecycle.archive")]
    LifecycleArchive { older_than_days: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl AdminResponse {
    pub fn ok(data: serde_json::Value) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn err(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(msg.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub name: String,
    pub status: ServiceStatus,
    pub port: u16,
    pub config: String,
    pub data_dir: String,
    pub engine: String,
    pub enable_iceberg: bool,
    pub pid: Option<u32>,
    pub uptime_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ServiceStatus {
    Running,
    Stopped,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub service: String,
    pub healthy: bool,
    pub storage_ok: bool,
    pub memory_usage_pct: f64,
    pub disk_usage_pct: f64,
    pub l0_file_count: i32,
    pub compaction_idle: bool,
    pub measurements_count: usize,
    pub total_data_points: u64,
    pub cpu_pct: f64,
    pub sys_memory_pct: f64,
    pub sys_disk_pct: f64,
    pub load_avg_1m: f64,
    pub uptime_secs: u64,
    pub cpu_temp_c: f64,
    pub gpu_temp_c: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub timestamp_ms: u64,
    pub write_rate: f64,
    pub read_rate: f64,
    pub write_latency_us: f64,
    pub read_latency_us: f64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub l0_file_count: i32,
    pub compaction_count: u64,
    pub cpu_pct: f64,
    pub sys_memory_total_bytes: u64,
    pub sys_memory_used_bytes: u64,
    pub sys_memory_pct: f64,
    pub sys_disk_total_bytes: u64,
    pub sys_disk_used_bytes: u64,
    pub sys_disk_pct: f64,
    pub load_avg_1m: f64,
    pub net_rx_bytes: u64,
    pub net_tx_bytes: u64,
    pub cpu_temp_c: f64,
    pub gpu_temp_c: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorStatus {
    pub running: bool,
    pub interval_secs: u64,
    pub snapshots_collected: u64,
    pub last_collect_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksdbOverview {
    pub total_cf_count: usize,
    pub ts_cf_count: usize,
    pub measurements: Vec<String>,
    pub total_sst_size_bytes: u64,
    pub total_memtable_bytes: u64,
    pub total_block_cache_bytes: u64,
    pub total_keys: u64,
    pub l0_file_count: i32,
    pub compaction_pending: bool,
    pub stats_text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvEntry {
    pub key_hex: String,
    pub key_ascii: String,
    pub value_hex: String,
    pub value_ascii: String,
    pub value_size: usize,
    pub decoded: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvScanResult {
    pub cf: String,
    pub entries: Vec<KvEntry>,
    pub total_scanned: usize,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesSchema {
    pub measurements: Vec<MeasurementSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementSchema {
    pub name: String,
    pub column_families: Vec<String>,
    pub series: Vec<SeriesInfo>,
    pub tag_keys: Vec<String>,
    pub field_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesInfo {
    pub tags_hash: String,
    pub tags: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetFileInfo {
    pub path: String,
    pub file_size: u64,
    pub modified: String,
    pub num_rows: usize,
    pub num_columns: usize,
    pub num_row_groups: usize,
    pub columns: Vec<ColumnMeta>,
    pub compression: String,
    pub created_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: String,
    pub compressed_size: i64,
    pub uncompressed_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetPreview {
    pub path: String,
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
    pub total_rows: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlResult {
    pub sql: String,
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
    pub total_rows: usize,
    pub elapsed_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleStatus {
    pub hot_cfs: Vec<DataTierInfo>,
    pub warm_cfs: Vec<DataTierInfo>,
    pub cold_cfs: Vec<DataTierInfo>,
    pub archive_files: Vec<ArchiveFileInfo>,
    pub parquet_partitions: Vec<ParquetPartitionInfo>,
    pub total_hot_bytes: u64,
    pub total_warm_bytes: u64,
    pub total_cold_bytes: u64,
    pub total_archive_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataTierInfo {
    pub cf_name: String,
    pub measurement: String,
    pub date: String,
    pub age_days: u64,
    pub sst_size: u64,
    pub num_keys: u64,
    pub tier: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveFileInfo {
    pub name: String,
    pub size: u64,
    pub modified: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetPartitionInfo {
    pub date: String,
    pub files: Vec<String>,
    pub total_size: u64,
    pub tier: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CfDetail {
    pub name: String,
    pub estimate_num_keys: u64,
    pub total_sst_file_size: u64,
    pub num_files_at_level: Vec<i64>,
    pub memtable_size: u64,
    pub block_cache_usage: u64,
    pub compaction_pending: bool,
    pub stats_text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub level: AlertLevel,
    pub message: String,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertLevel {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigProfile {
    pub name: String,
    pub content: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDiff {
    pub key: String,
    pub value_a: String,
    pub value_b: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResult {
    pub operation: String,
    pub total_points: usize,
    pub elapsed_secs: f64,
    pub rate_per_sec: f64,
    pub avg_latency_us: f64,
    pub p99_latency_us: f64,
}
