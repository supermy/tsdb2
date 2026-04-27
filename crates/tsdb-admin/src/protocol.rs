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
    ServiceLogs { name: String, lines: Option<usize> },

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
    ConfigCompare {
        profile_a: String,
        profile_b: String,
    },

    #[serde(rename = "test.sql")]
    TestSql { service: String, sql: String },
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
    #[serde(rename = "test.generate_business_data")]
    TestGenerateBusinessData {
        scenario: String,
        points_per_series: usize,
        #[serde(default)]
        skip_auto_demote: bool,
    },
    #[serde(rename = "iceberg.list_tables")]
    IcebergListTables,
    #[serde(rename = "iceberg.create_table")]
    IcebergCreateTable {
        name: String,
        schema: serde_json::Value,
        partition_type: String,
    },
    #[serde(rename = "iceberg.drop_table")]
    IcebergDropTable { name: String },
    #[serde(rename = "iceberg.table_detail")]
    IcebergTableDetail { name: String },
    #[serde(rename = "iceberg.append")]
    IcebergAppend {
        table: String,
        datapoints: Vec<serde_json::Value>,
    },
    #[serde(rename = "iceberg.scan")]
    IcebergScan { table: String, limit: Option<usize> },
    #[serde(rename = "iceberg.snapshots")]
    IcebergSnapshots { table: String },
    #[serde(rename = "iceberg.rollback")]
    IcebergRollback { table: String, snapshot_id: i64 },
    #[serde(rename = "iceberg.compact")]
    IcebergCompact { table: String },
    #[serde(rename = "iceberg.expire")]
    IcebergExpire { table: String, keep_days: u64 },
    #[serde(rename = "iceberg.update_schema")]
    IcebergUpdateSchema {
        table: String,
        changes: Vec<serde_json::Value>,
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
    CollectorConfigure { interval_secs: u64, enabled: bool },
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
    RocksdbKvGet { cf: String, key: String },
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
    #[serde(rename = "lifecycle.demote_to_warm")]
    LifecycleDemoteToWarm { cf_names: Vec<String> },
    #[serde(rename = "lifecycle.demote_to_cold")]
    LifecycleDemoteToCold { cf_names: Vec<String> },
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
    #[serde(default)]
    pub tier: String,
    #[serde(default)]
    pub measurement: String,
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
    pub truncated: bool,
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
    #[serde(default)]
    pub path: String,
    #[serde(default = "default_storage")]
    pub storage: String,
    #[serde(default = "default_demote_eligible")]
    pub demote_eligible: String,
}

fn default_storage() -> String {
    "rocksdb".to_string()
}

fn default_demote_eligible() -> String {
    "none".to_string()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admin_response_ok() {
        let resp = AdminResponse::ok(serde_json::json!({"key": "value"}));
        assert!(resp.success);
        assert!(resp.data.is_some());
        assert!(resp.error.is_none());
    }

    #[test]
    fn test_admin_response_err() {
        let resp = AdminResponse::err("something failed");
        assert!(!resp.success);
        assert!(resp.data.is_none());
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap(), "something failed");
    }

    #[test]
    fn test_admin_request_serde_roundtrip() {
        let req = AdminRequest::ServiceStart {
            name: "test-svc".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: AdminRequest = serde_json::from_str(&json).unwrap();
        if let AdminRequest::ServiceStart { name } = decoded {
            assert_eq!(name, "test-svc");
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn test_admin_request_service_list_serde() {
        let req = AdminRequest::ServiceList;
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("service.list"));
        let decoded: AdminRequest = serde_json::from_str(&json).unwrap();
        matches!(decoded, AdminRequest::ServiceList);
    }

    #[test]
    fn test_admin_request_sql_execute_serde() {
        let req = AdminRequest::SqlExecute {
            sql: "SELECT 1".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: AdminRequest = serde_json::from_str(&json).unwrap();
        if let AdminRequest::SqlExecute { sql } = decoded {
            assert_eq!(sql, "SELECT 1");
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn test_admin_response_serde_roundtrip() {
        let resp = AdminResponse::ok(serde_json::json!([1, 2, 3]));
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: AdminResponse = serde_json::from_str(&json).unwrap();
        assert!(decoded.success);
        assert!(decoded.data.is_some());
    }

    #[test]
    fn test_service_status_serde() {
        let status = ServiceStatus::Running;
        let json = serde_json::to_string(&status).unwrap();
        let decoded: ServiceStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, ServiceStatus::Running);
    }

    #[test]
    fn test_health_status_serde() {
        let health = HealthStatus {
            service: "test".to_string(),
            healthy: true,
            storage_ok: true,
            memory_usage_pct: 50.0,
            disk_usage_pct: 30.0,
            l0_file_count: 2,
            compaction_idle: true,
            measurements_count: 10,
            total_data_points: 1000,
            cpu_pct: 25.0,
            sys_memory_pct: 60.0,
            sys_disk_pct: 40.0,
            load_avg_1m: 1.5,
            uptime_secs: 3600,
            cpu_temp_c: 55.0,
            gpu_temp_c: 45.0,
        };
        let json = serde_json::to_string(&health).unwrap();
        let decoded: HealthStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.service, "test");
        assert!(decoded.healthy);
        assert_eq!(decoded.total_data_points, 1000);
    }

    #[test]
    fn test_lifecycle_status_serde() {
        let status = LifecycleStatus {
            hot_cfs: vec![],
            warm_cfs: vec![],
            cold_cfs: vec![],
            archive_files: vec![],
            parquet_partitions: vec![],
            total_hot_bytes: 100,
            total_warm_bytes: 200,
            total_cold_bytes: 300,
            total_archive_bytes: 400,
        };
        let json = serde_json::to_string(&status).unwrap();
        let decoded: LifecycleStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.total_hot_bytes, 100);
        assert_eq!(decoded.total_cold_bytes, 300);
    }

    #[test]
    fn test_sql_result_serde() {
        let result = SqlResult {
            sql: "SELECT * FROM cpu".to_string(),
            columns: vec!["timestamp".to_string(), "value".to_string()],
            rows: vec![serde_json::json!({"timestamp": 1, "value": 42})],
            total_rows: 1,
            elapsed_ms: 5.3,
            truncated: false,
        };
        let json = serde_json::to_string(&result).unwrap();
        let decoded: SqlResult = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.sql, "SELECT * FROM cpu");
        assert_eq!(decoded.columns.len(), 2);
        assert!(!decoded.truncated);
    }

    #[test]
    fn test_data_tier_info_storage_field() {
        let info = DataTierInfo {
            cf_name: "ts_cpu_20260401".to_string(),
            measurement: "cpu".to_string(),
            date: "20260401".to_string(),
            age_days: 5,
            sst_size: 1024,
            num_keys: 100,
            tier: "warm".to_string(),
            path: "/tmp/parquet/warm".to_string(),
            storage: "parquet".to_string(),
            demote_eligible: "cold".to_string(),
        };
        let json = serde_json::to_string(&info).unwrap();
        let decoded: DataTierInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.storage, "parquet");
        assert_eq!(decoded.tier, "warm");
        assert_eq!(decoded.demote_eligible, "cold");
    }

    #[test]
    fn test_data_tier_info_default_storage() {
        let json = r#"{"cf_name":"ts_cpu_20260401","measurement":"cpu","date":"20260401","age_days":1,"sst_size":512,"num_keys":50,"tier":"hot","path":"/tmp/rocksdb"}"#;
        let decoded: DataTierInfo = serde_json::from_str(json).unwrap();
        assert_eq!(decoded.storage, "rocksdb");
        assert_eq!(decoded.demote_eligible, "none");
    }
}
