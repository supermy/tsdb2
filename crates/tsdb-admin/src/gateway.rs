use crate::error::AdminError;
use crate::protocol::*;
use crate::utils;
use axum::extract::ws::Message as WsMessage;
use axum::{
    extract::{ws::WebSocket, Path, Query, State, WebSocketUpgrade},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use nng::{Message as NngMessage, Protocol, Socket as NngSocket};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;

fn validate_name(name: &str) -> std::result::Result<(), String> {
    utils::validate_name(name)
}

fn validate_parquet_path(path: &str) -> std::result::Result<(), String> {
    if path.contains("..") {
        return Err("path traversal not allowed".to_string());
    }
    if path.starts_with('/')
        && !path.starts_with("/tmp/")
        && !path.starts_with("/data/")
        && !path.starts_with("/var/")
    {
        return Err("absolute path must be within allowed directories".to_string());
    }
    Ok(())
}

type WsClients = Arc<RwLock<Vec<(uuid::Uuid, tokio::sync::mpsc::Sender<String>)>>>;

#[derive(Clone)]
pub struct GatewayState {
    nng_req: Arc<NngSocket>,
    nng_sub: Arc<NngSocket>,
    nng_mutex: Arc<tokio::sync::Mutex<()>>,
    ws_clients: WsClients,
}

impl GatewayState {
    pub fn new(req_url: &str, pub_url: &str) -> std::result::Result<Self, AdminError> {
        let req_sock = NngSocket::new(Protocol::Req0)
            .map_err(|e| AdminError::Nng(format!("create req socket: {:?}", e)))?;
        Self::dial_with_retry(&req_sock, req_url, "req")?;

        let sub_sock = NngSocket::new(Protocol::Sub0)
            .map_err(|e| AdminError::Nng(format!("create sub socket: {:?}", e)))?;
        Self::dial_with_retry(&sub_sock, pub_url, "sub")?;

        let state = Self {
            nng_req: Arc::new(req_sock),
            nng_sub: Arc::new(sub_sock),
            nng_mutex: Arc::new(tokio::sync::Mutex::new(())),
            ws_clients: Arc::new(RwLock::new(Vec::new())),
        };

        state.start_subscriber();
        Ok(state)
    }

    fn dial_with_retry(
        sock: &NngSocket,
        url: &str,
        label: &str,
    ) -> std::result::Result<(), AdminError> {
        let mut attempts = 0;
        let max_attempts = 50;
        loop {
            match sock.dial(url) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(AdminError::Nng(format!(
                            "dial {} {} after {} attempts: {:?}",
                            label, url, max_attempts, e
                        )));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        }
    }

    fn start_subscriber(&self) {
        let sub_sock = self.nng_sub.clone();
        let clients = self.ws_clients.clone();
        let rt_handle = tokio::runtime::Handle::current();

        std::thread::spawn(move || loop {
            match sub_sock.recv() {
                Ok(msg) => {
                    let data = String::from_utf8_lossy(msg.as_slice()).to_string();
                    rt_handle.block_on(async {
                        let mut clients = clients.write().await;
                        clients.retain(|(_, tx)| tx.try_send(data.clone()).is_ok());
                    });
                }
                Err(_) => {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        });
    }

    pub async fn send_request(
        &self,
        request: AdminRequest,
    ) -> std::result::Result<AdminResponse, String> {
        let _guard = self.nng_mutex.lock().await;
        let json = serde_json::to_string(&request).map_err(|e| e.to_string())?;
        let result = tokio::task::spawn_blocking({
            let nng_req = self.nng_req.clone();
            move || {
                let mut msg = NngMessage::new();
                msg.push_back(json.as_bytes());
                nng_req.send(msg).map_err(|e| format!("send: {:?}", e))?;
                let reply = nng_req.recv().map_err(|e| format!("recv: {:?}", e))?;
                let response: AdminResponse = serde_json::from_slice(reply.as_slice())
                    .map_err(|e| format!("parse: {}", e))?;
                Ok(response)
            }
        })
        .await
        .map_err(|e| format!("spawn_blocking: {}", e))?;
        result
    }
}

pub fn build_router(state: GatewayState) -> Router {
    let cors = match std::env::var("TSDB_CORS_ORIGINS") {
        Ok(origins) if !origins.is_empty() => {
            let origin_list: Vec<String> = origins
                .split(',')
                .map(|o| o.trim().to_string())
                .filter(|o| !o.is_empty())
                .collect();
            if origin_list.is_empty() {
                CorsLayer::new()
                    .allow_origin([
                        "http://localhost:3000".parse().unwrap(),
                        "http://127.0.0.1:3000".parse().unwrap(),
                    ])
                    .allow_methods([
                        axum::http::Method::GET,
                        axum::http::Method::POST,
                        axum::http::Method::DELETE,
                    ])
                    .allow_headers([axum::http::header::CONTENT_TYPE])
                    .allow_credentials(true)
            } else {
                let origins: Vec<axum::http::HeaderValue> =
                    origin_list.iter().filter_map(|o| o.parse().ok()).collect();
                CorsLayer::new()
                    .allow_origin(origins)
                    .allow_methods([
                        axum::http::Method::GET,
                        axum::http::Method::POST,
                        axum::http::Method::DELETE,
                    ])
                    .allow_headers([axum::http::header::CONTENT_TYPE])
                    .allow_credentials(true)
            }
        }
        _ => CorsLayer::new()
            .allow_origin([
                "http://localhost:3000".parse().unwrap(),
                "http://127.0.0.1:3000".parse().unwrap(),
            ])
            .allow_methods([
                axum::http::Method::GET,
                axum::http::Method::POST,
                axum::http::Method::DELETE,
            ])
            .allow_headers([axum::http::header::CONTENT_TYPE])
            .allow_credentials(true),
    };

    let api = Router::new()
        .route("/api/services", get(list_services).post(create_service))
        .route(
            "/api/services/{name}",
            get(get_service).delete(delete_service),
        )
        .route("/api/services/{name}/start", post(start_service))
        .route("/api/services/{name}/stop", post(stop_service))
        .route("/api/services/{name}/restart", post(restart_service))
        .route("/api/services/{name}/logs", get(get_service_logs))
        .route("/api/configs", get(list_configs).post(save_config))
        .route("/api/configs/{name}", get(get_config).delete(delete_config))
        .route("/api/configs/apply", post(apply_config))
        .route("/api/configs/compare", post(compare_config))
        .route("/api/test/sql", post(test_sql))
        .route("/api/test/write-bench", post(test_write_bench))
        .route("/api/test/read-bench", post(test_read_bench))
        .route(
            "/api/test/generate-business-data",
            post(test_generate_business_data),
        )
        .route("/api/metrics/health", get(metrics_health))
        .route("/api/metrics/stats", get(metrics_stats))
        .route("/api/metrics/timeseries", get(metrics_timeseries))
        .route("/api/metrics/alerts", get(metrics_alerts))
        .route("/api/collector/status", get(collector_status))
        .route("/api/collector/configure", post(collector_configure))
        .route("/api/collector/start", post(collector_start))
        .route("/api/collector/stop", post(collector_stop))
        .route("/api/rocksdb/stats", get(rocksdb_stats))
        .route("/api/rocksdb/cf-list", get(rocksdb_cf_list))
        .route("/api/rocksdb/cf-detail/{name}", get(rocksdb_cf_detail))
        .route("/api/rocksdb/compact", post(rocksdb_compact))
        .route("/api/rocksdb/series-schema", get(rocksdb_series_schema))
        .route("/api/rocksdb/kv-scan", get(rocksdb_kv_scan))
        .route("/api/rocksdb/kv-get", get(rocksdb_kv_get))
        .route("/api/parquet/list", get(parquet_list))
        .route("/api/parquet/file-detail", get(parquet_file_detail))
        .route("/api/parquet/preview", get(parquet_preview))
        .route("/api/sql/execute", post(sql_execute))
        .route("/api/sql/tables", get(sql_tables))
        .route("/api/lifecycle/status", get(lifecycle_status))
        .route("/api/lifecycle/archive", post(lifecycle_archive))
        .route(
            "/api/lifecycle/demote-to-warm",
            post(lifecycle_demote_to_warm),
        )
        .route(
            "/api/lifecycle/demote-to-cold",
            post(lifecycle_demote_to_cold),
        )
        .route(
            "/api/iceberg/tables",
            get(iceberg_list_tables).post(iceberg_create_table),
        )
        .route(
            "/api/iceberg/tables/{name}",
            get(iceberg_table_detail).delete(iceberg_drop_table),
        )
        .route("/api/iceberg/tables/{name}/append", post(iceberg_append))
        .route("/api/iceberg/tables/{name}/scan", get(iceberg_scan))
        .route(
            "/api/iceberg/tables/{name}/snapshots",
            get(iceberg_snapshots),
        )
        .route("/api/iceberg/tables/{name}/rollback", post(iceberg_rollback))
        .route("/api/iceberg/tables/{name}/compact", post(iceberg_compact))
        .route("/api/iceberg/tables/{name}/expire", post(iceberg_expire))
        .route(
            "/api/iceberg/tables/{name}/schema",
            post(iceberg_update_schema),
        )
        .route("/api/ws", get(ws_handler))
        .with_state(state);

    let dashboard_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("dashboard")))
        .unwrap_or_else(|| std::path::PathBuf::from("dashboard"));

    let static_files = tower_http::services::ServeDir::new(&dashboard_dir).fallback(
        tower_http::services::ServeFile::new(dashboard_dir.join("index.html")),
    );

    Router::new()
        .merge(api)
        .fallback_service(static_files)
        .layer(cors)
}

macro_rules! api_handler {
    ($state:expr, $req:expr) => {
        match $state.send_request($req).await {
            Ok(resp) => (StatusCode::OK, Json(resp)),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AdminResponse::err(e)),
            ),
        }
    };
}

async fn list_services(State(state): State<GatewayState>) -> impl IntoResponse {
    api_handler!(state, AdminRequest::ServiceList)
}

async fn create_service(
    State(state): State<GatewayState>,
    Json(body): Json<CreateServiceBody>,
) -> impl IntoResponse {
    if let Err(e) = validate_name(&body.name) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(
        state,
        AdminRequest::ServiceCreate {
            name: body.name,
            port: body.port,
            config: body.config,
            data_dir: body.data_dir,
            engine: body.engine,
            enable_iceberg: body.enable_iceberg.unwrap_or(false),
        }
    )
}

#[derive(Deserialize)]
struct CreateServiceBody {
    name: String,
    port: u16,
    config: String,
    data_dir: String,
    engine: String,
    enable_iceberg: Option<bool>,
}

async fn get_service(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = validate_name(&name) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(state, AdminRequest::ServiceStatus { name })
}

async fn delete_service(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = validate_name(&name) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(state, AdminRequest::ServiceDelete { name })
}

async fn start_service(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = validate_name(&name) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(state, AdminRequest::ServiceStart { name })
}

async fn stop_service(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = validate_name(&name) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(state, AdminRequest::ServiceStop { name })
}

async fn restart_service(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = validate_name(&name) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(state, AdminRequest::ServiceRestart { name })
}

#[derive(Deserialize)]
struct LogsQuery {
    lines: Option<usize>,
}

async fn get_service_logs(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
    Query(query): Query<LogsQuery>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::ServiceLogs {
            name,
            lines: query.lines
        }
    )
}

async fn list_configs(State(state): State<GatewayState>) -> impl IntoResponse {
    api_handler!(state, AdminRequest::ConfigListProfiles)
}

async fn get_config(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = validate_name(&name) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(state, AdminRequest::ConfigGetProfile { name })
}

async fn save_config(
    State(state): State<GatewayState>,
    Json(body): Json<SaveConfigBody>,
) -> impl IntoResponse {
    if let Err(e) = validate_name(&body.name) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(
        state,
        AdminRequest::ConfigSaveProfile {
            name: body.name,
            content: body.content
        }
    )
}

#[derive(Deserialize)]
struct SaveConfigBody {
    name: String,
    content: String,
}

async fn delete_config(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = validate_name(&name) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(state, AdminRequest::ConfigDeleteProfile { name })
}

async fn apply_config(
    State(state): State<GatewayState>,
    Json(body): Json<ApplyConfigBody>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::ConfigApply {
            service: body.service,
            profile: body.profile
        }
    )
}

#[derive(Deserialize)]
struct ApplyConfigBody {
    service: String,
    profile: String,
}

async fn compare_config(
    State(state): State<GatewayState>,
    Json(body): Json<CompareConfigBody>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::ConfigCompare {
            profile_a: body.profile_a,
            profile_b: body.profile_b
        }
    )
}

#[derive(Deserialize)]
struct CompareConfigBody {
    profile_a: String,
    profile_b: String,
}

async fn test_sql(
    State(state): State<GatewayState>,
    Json(body): Json<TestSqlBody>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::TestSql {
            service: body.service,
            sql: body.sql
        }
    )
}

#[derive(Deserialize)]
struct TestSqlBody {
    service: String,
    sql: String,
}

async fn test_write_bench(
    State(state): State<GatewayState>,
    Json(body): Json<TestWriteBenchBody>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::TestWriteBench {
            service: body.service,
            measurement: body.measurement,
            total_points: body.total_points,
            workers: body.workers,
            batch_size: body.batch_size,
        }
    )
}

#[derive(Deserialize)]
struct TestWriteBenchBody {
    service: String,
    measurement: String,
    total_points: usize,
    workers: usize,
    batch_size: usize,
}

async fn test_read_bench(
    State(state): State<GatewayState>,
    Json(body): Json<TestReadBenchBody>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::TestReadBench {
            service: body.service,
            measurement: body.measurement,
            queries: body.queries,
            workers: body.workers,
        }
    )
}

#[derive(Deserialize)]
struct TestReadBenchBody {
    service: String,
    measurement: String,
    queries: usize,
    workers: usize,
}

#[derive(Deserialize)]
struct GenerateBusinessDataBody {
    scenario: String,
    points_per_series: Option<usize>,
    skip_auto_demote: Option<bool>,
}

async fn test_generate_business_data(
    State(state): State<GatewayState>,
    Json(body): Json<GenerateBusinessDataBody>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::TestGenerateBusinessData {
            scenario: body.scenario,
            points_per_series: body.points_per_series.unwrap_or(60),
            skip_auto_demote: body.skip_auto_demote.unwrap_or(false),
        }
    )
}

async fn metrics_health(
    State(state): State<GatewayState>,
    Query(query): Query<MetricsHealthQuery>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::MetricsHealth {
            service: query.service
        }
    )
}

#[derive(Deserialize)]
struct MetricsHealthQuery {
    service: String,
}

async fn metrics_stats(
    State(state): State<GatewayState>,
    Query(query): Query<MetricsStatsQuery>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::MetricsStats {
            service: query.service.unwrap_or_else(|| "default".to_string())
        }
    )
}

#[derive(Deserialize)]
struct MetricsStatsQuery {
    service: Option<String>,
}

async fn metrics_timeseries(
    State(state): State<GatewayState>,
    Query(query): Query<MetricsTimeseriesQuery>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::MetricsTimeseries {
            service: query.service.unwrap_or_else(|| "default".to_string()),
            metric: query.metric.unwrap_or_else(|| "all".to_string()),
            range_secs: query.range_secs.unwrap_or(60),
        }
    )
}

#[derive(Deserialize)]
struct MetricsTimeseriesQuery {
    service: Option<String>,
    metric: Option<String>,
    range_secs: Option<u64>,
}

async fn metrics_alerts(
    State(state): State<GatewayState>,
    Query(query): Query<MetricsAlertsQuery>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::MetricsAlerts {
            service: query.service
        }
    )
}

#[derive(Deserialize)]
struct MetricsAlertsQuery {
    service: String,
}

async fn collector_status(State(state): State<GatewayState>) -> impl IntoResponse {
    api_handler!(state, AdminRequest::CollectorStatus)
}

async fn collector_configure(
    State(state): State<GatewayState>,
    Json(body): Json<CollectorConfigureBody>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::CollectorConfigure {
            interval_secs: body.interval_secs,
            enabled: body.enabled,
        }
    )
}

#[derive(Deserialize)]
struct CollectorConfigureBody {
    interval_secs: u64,
    enabled: bool,
}

async fn collector_start(State(state): State<GatewayState>) -> impl IntoResponse {
    api_handler!(state, AdminRequest::CollectorStart)
}

async fn collector_stop(State(state): State<GatewayState>) -> impl IntoResponse {
    api_handler!(state, AdminRequest::CollectorStop)
}

async fn rocksdb_stats(State(state): State<GatewayState>) -> impl IntoResponse {
    api_handler!(state, AdminRequest::RocksdbStats)
}

async fn rocksdb_cf_list(State(state): State<GatewayState>) -> impl IntoResponse {
    api_handler!(state, AdminRequest::RocksdbCfList)
}

async fn rocksdb_cf_detail(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = validate_name(&name) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(state, AdminRequest::RocksdbCfDetail { name })
}

async fn rocksdb_compact(
    State(state): State<GatewayState>,
    Json(body): Json<RocksdbCompactBody>,
) -> impl IntoResponse {
    api_handler!(state, AdminRequest::RocksdbCompact { cf: body.cf })
}

#[derive(Deserialize)]
struct RocksdbCompactBody {
    cf: Option<String>,
}

#[derive(Deserialize)]
struct KvScanQuery {
    cf: String,
    prefix: Option<String>,
    start_key: Option<String>,
    limit: Option<usize>,
}

async fn rocksdb_kv_scan(
    State(state): State<GatewayState>,
    Query(query): Query<KvScanQuery>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::RocksdbKvScan {
            cf: query.cf,
            prefix: query.prefix,
            start_key: query.start_key,
            limit: query.limit,
        }
    )
}

#[derive(Deserialize)]
struct KvGetQuery {
    cf: String,
    key: String,
}

async fn rocksdb_kv_get(
    State(state): State<GatewayState>,
    Query(query): Query<KvGetQuery>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::RocksdbKvGet {
            cf: query.cf,
            key: query.key
        }
    )
}

async fn rocksdb_series_schema(State(state): State<GatewayState>) -> impl IntoResponse {
    api_handler!(state, AdminRequest::RocksdbSeriesSchema)
}

async fn parquet_list(State(state): State<GatewayState>) -> impl IntoResponse {
    api_handler!(state, AdminRequest::ParquetList)
}

#[derive(Deserialize)]
struct ParquetFileDetailQuery {
    path: String,
}

async fn parquet_file_detail(
    State(state): State<GatewayState>,
    Query(query): Query<ParquetFileDetailQuery>,
) -> impl IntoResponse {
    if let Err(e) = validate_parquet_path(&query.path) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(state, AdminRequest::ParquetFileDetail { path: query.path })
}

#[derive(Deserialize)]
struct ParquetPreviewQuery {
    path: String,
    limit: Option<usize>,
}

async fn parquet_preview(
    State(state): State<GatewayState>,
    Query(query): Query<ParquetPreviewQuery>,
) -> impl IntoResponse {
    if let Err(e) = validate_parquet_path(&query.path) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(
        state,
        AdminRequest::ParquetPreview {
            path: query.path,
            limit: query.limit
        }
    )
}

#[derive(Deserialize)]
struct SqlExecuteBody {
    sql: String,
}

async fn sql_execute(
    State(state): State<GatewayState>,
    Json(body): Json<SqlExecuteBody>,
) -> impl IntoResponse {
    api_handler!(state, AdminRequest::SqlExecute { sql: body.sql })
}

async fn sql_tables(State(state): State<GatewayState>) -> impl IntoResponse {
    api_handler!(state, AdminRequest::SqlTables)
}

async fn lifecycle_status(State(state): State<GatewayState>) -> impl IntoResponse {
    api_handler!(state, AdminRequest::LifecycleStatus)
}

#[derive(Deserialize)]
struct LifecycleArchiveBody {
    older_than_days: u64,
}

async fn lifecycle_archive(
    State(state): State<GatewayState>,
    Json(body): Json<LifecycleArchiveBody>,
) -> impl IntoResponse {
    if body.older_than_days < 7 {
        return (
            StatusCode::BAD_REQUEST,
            Json(AdminResponse::err("older_than_days must be >= 7")),
        );
    }
    api_handler!(
        state,
        AdminRequest::LifecycleArchive {
            older_than_days: body.older_than_days
        }
    )
}

#[derive(Deserialize)]
struct LifecycleDemoteBody {
    cf_names: Vec<String>,
}

async fn lifecycle_demote_to_warm(
    State(state): State<GatewayState>,
    Json(body): Json<LifecycleDemoteBody>,
) -> impl IntoResponse {
    if body.cf_names.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(AdminResponse::err("cf_names cannot be empty")),
        );
    }
    if body.cf_names.len() > 100 {
        return (
            StatusCode::BAD_REQUEST,
            Json(AdminResponse::err("too many cf_names (max 100)")),
        );
    }
    for name in &body.cf_names {
        if let Err(e) = validate_name(name) {
            return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
        }
    }
    api_handler!(
        state,
        AdminRequest::LifecycleDemoteToWarm {
            cf_names: body.cf_names
        }
    )
}

async fn lifecycle_demote_to_cold(
    State(state): State<GatewayState>,
    Json(body): Json<LifecycleDemoteBody>,
) -> impl IntoResponse {
    if body.cf_names.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(AdminResponse::err("cf_names cannot be empty")),
        );
    }
    if body.cf_names.len() > 100 {
        return (
            StatusCode::BAD_REQUEST,
            Json(AdminResponse::err("too many cf_names (max 100)")),
        );
    }
    for name in &body.cf_names {
        if let Err(e) = validate_name(name) {
            return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
        }
    }
    api_handler!(
        state,
        AdminRequest::LifecycleDemoteToCold {
            cf_names: body.cf_names
        }
    )
}

async fn iceberg_list_tables(State(state): State<GatewayState>) -> impl IntoResponse {
    api_handler!(state, AdminRequest::IcebergListTables)
}

async fn iceberg_create_table(
    State(state): State<GatewayState>,
    Json(body): Json<IcebergCreateTableBody>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::IcebergCreateTable {
            name: body.name,
            schema: body.schema,
            partition_type: body.partition_type,
        }
    )
}

#[derive(Deserialize)]
struct IcebergCreateTableBody {
    name: String,
    schema: serde_json::Value,
    partition_type: String,
}

async fn iceberg_table_detail(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = validate_name(&name) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(state, AdminRequest::IcebergTableDetail { name })
}

async fn iceberg_drop_table(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = validate_name(&name) {
        return (StatusCode::BAD_REQUEST, Json(AdminResponse::err(e)));
    }
    api_handler!(state, AdminRequest::IcebergDropTable { name })
}

async fn iceberg_append(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
    Json(body): Json<IcebergAppendBody>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::IcebergAppend {
            table: name,
            datapoints: body.datapoints,
        }
    )
}

#[derive(Deserialize)]
struct IcebergAppendBody {
    datapoints: Vec<serde_json::Value>,
}

async fn iceberg_scan(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
    Query(query): Query<IcebergScanQuery>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::IcebergScan {
            table: name,
            limit: query.limit,
        }
    )
}

#[derive(Deserialize)]
struct IcebergScanQuery {
    limit: Option<usize>,
}

async fn iceberg_snapshots(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    api_handler!(state, AdminRequest::IcebergSnapshots { table: name })
}

async fn iceberg_rollback(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
    Json(body): Json<IcebergRollbackBody>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::IcebergRollback {
            table: name,
            snapshot_id: body.snapshot_id,
        }
    )
}

#[derive(Deserialize)]
struct IcebergRollbackBody {
    snapshot_id: i64,
}

async fn iceberg_compact(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    api_handler!(state, AdminRequest::IcebergCompact { table: name })
}

async fn iceberg_expire(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
    Json(body): Json<IcebergExpireBody>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::IcebergExpire {
            table: name,
            keep_days: body.keep_days,
        }
    )
}

#[derive(Deserialize)]
struct IcebergExpireBody {
    keep_days: u64,
}

async fn iceberg_update_schema(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
    Json(body): Json<IcebergUpdateSchemaBody>,
) -> impl IntoResponse {
    api_handler!(
        state,
        AdminRequest::IcebergUpdateSchema {
            table: name,
            changes: body.changes,
        }
    )
}

#[derive(Deserialize)]
struct IcebergUpdateSchemaBody {
    changes: Vec<serde_json::Value>,
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<GatewayState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(socket: WebSocket, state: GatewayState) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(100);
    let client_id = uuid::Uuid::new_v4();

    {
        let mut clients = state.ws_clients.write().await;
        clients.push((client_id, tx));
    }

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender.send(WsMessage::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_receiver.next().await {
            if let WsMessage::Close(_) = msg {
                break;
            }
        }
    });

    tokio::select! {
        _ = send_task => {},
        _ = recv_task => {},
    }

    let mut clients = state.ws_clients.write().await;
    clients.retain(|(id, _)| *id != client_id);
}
