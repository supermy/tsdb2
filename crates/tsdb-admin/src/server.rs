use crate::config_api::ConfigManager;
use crate::error::{AdminError, Result};
use crate::iceberg_api::IcebergApi;
use crate::lifecycle_api::LifecycleApi;
use crate::metrics_api::MetricsCollector;
use crate::parquet_api::ParquetApi;
use crate::protocol::*;
use crate::service_api::ServiceManager;
use crate::sql_api::SqlApi;
use crate::test_api::TestRunner;
use nng::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

type RequestHandler = Arc<
    dyn Fn(
            AdminRequest,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = AdminResponse> + Send>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct AdminServer {
    service_mgr: Arc<ServiceManager>,
    config_mgr: Arc<RwLock<ConfigManager>>,
    metrics: Arc<RwLock<MetricsCollector>>,
    engine: Arc<dyn tsdb_arrow::engine::StorageEngine>,
    parquet_api: Arc<ParquetApi>,
    sql_api: Arc<SqlApi>,
    lifecycle_api: Arc<LifecycleApi>,
    iceberg_api: Arc<Option<IcebergApi>>,
    parquet_dir: PathBuf,
    rep_url: String,
    pub_url: String,
    shutdown: Arc<tokio::sync::watch::Sender<bool>>,
}

pub struct BoundAdminServer {
    inner: AdminServer,
    rep_socket: Arc<Socket>,
    pub_socket: Arc<Socket>,
}

impl AdminServer {
    pub fn new(
        engine: Arc<dyn tsdb_arrow::engine::StorageEngine>,
        base_dir: impl AsRef<Path>,
        parquet_dir: impl AsRef<Path>,
        host: &str,
        admin_port: u16,
        iceberg_dir: Option<PathBuf>,
    ) -> Self {
        let service_mgr = ServiceManager::new(base_dir.as_ref());
        let config_mgr = ConfigManager::new(base_dir);
        let metrics = MetricsCollector::new(engine.clone());
        let parquet_api = ParquetApi::new(engine.clone(), parquet_dir.as_ref().to_path_buf());
        let iceberg_catalog = match iceberg_dir {
            Some(ref dir) => match tsdb_iceberg::IcebergCatalog::open(dir) {
                Ok(catalog) => {
                    tracing::info!("iceberg catalog opened at {}", dir.display());
                    Some(Arc::new(catalog))
                }
                Err(e) => {
                    tracing::warn!("iceberg init failed: {}", e);
                    None
                }
            },
            None => None,
        };
        let iceberg_api = match &iceberg_catalog {
            Some(catalog) => match IcebergApi::new_with_catalog(catalog.clone()) {
                Ok(api) => Some(api),
                Err(e) => {
                    tracing::warn!("iceberg api init failed: {}", e);
                    None
                }
            },
            None => None,
        };
        let sql_api = SqlApi::new(
            engine.clone(),
            parquet_dir.as_ref().to_path_buf(),
            iceberg_catalog.clone(),
        );
        let lifecycle_api = LifecycleApi::new(engine.clone(), parquet_dir.as_ref().to_path_buf());
        let (shutdown, _) = tokio::sync::watch::channel(false);

        Self {
            service_mgr: Arc::new(service_mgr),
            config_mgr: Arc::new(RwLock::new(config_mgr)),
            metrics: Arc::new(RwLock::new(metrics)),
            engine,
            parquet_api: Arc::new(parquet_api),
            sql_api: Arc::new(sql_api),
            lifecycle_api: Arc::new(lifecycle_api),
            iceberg_api: Arc::new(iceberg_api),
            parquet_dir: parquet_dir.as_ref().to_path_buf(),
            rep_url: format!("tcp://{}:{}", host, admin_port),
            pub_url: format!("tcp://{}:{}", host, admin_port + 1),
            shutdown: Arc::new(shutdown),
        }
    }

    pub fn rep_url(&self) -> &str {
        &self.rep_url
    }

    pub fn pub_url(&self) -> &str {
        &self.pub_url
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown.send(true);
    }

    pub fn bind(&self) -> Result<BoundAdminServer> {
        let rep_socket = Socket::new(Protocol::Rep0)
            .map_err(|e| AdminError::Nng(format!("create rep socket: {:?}", e)))?;
        rep_socket
            .listen(&self.rep_url)
            .map_err(|e| AdminError::Nng(format!("listen {}: {:?}", self.rep_url, e)))?;

        let pub_socket = Socket::new(Protocol::Pub0)
            .map_err(|e| AdminError::Nng(format!("create pub socket: {:?}", e)))?;
        pub_socket
            .listen(&self.pub_url)
            .map_err(|e| AdminError::Nng(format!("listen pub {}: {:?}", self.pub_url, e)))?;

        tracing::info!("admin nng rep listening on {}", self.rep_url);
        tracing::info!("admin nng pub listening on {}", self.pub_url);

        Ok(BoundAdminServer {
            inner: self.clone(),
            rep_socket: Arc::new(rep_socket),
            pub_socket: Arc::new(pub_socket),
        })
    }
}

impl BoundAdminServer {
    pub fn inner(&self) -> &AdminServer {
        &self.inner
    }

    pub async fn run(self) -> Result<()> {
        let metrics_collector = self.inner.metrics.clone();
        let pub_sock = self.pub_socket.clone();
        let pub_for_timer = pub_sock.clone();
        let service_mgr = self.inner.service_mgr.clone();
        let mut shutdown_rx = self.inner.shutdown.subscribe();

        service_mgr
            .ensure_default_service("default", 50051, "default", "./data", "rocksdb")
            .await;

        let engine_for_writer = self.inner.engine.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let mgr = metrics_collector.read().await;
                        if !mgr.is_collector_running() {
                            continue;
                        }
                        let configured_interval = mgr.collector_interval();
                        drop(mgr);

                        let mgr = metrics_collector.write().await;
                        let snapshot = mgr.stats();
                        if let Ok(json) = serde_json::to_string(&AdminResponse::ok(
                            serde_json::to_value(&snapshot).unwrap_or_default(),
                        )) {
                            let mut msg = Message::new();
                            msg.push_back(json.as_bytes());
                            let _ = pub_for_timer.send(msg);
                        }

                        let ts_us = snapshot.timestamp_ms as i64 * 1000;
                        let sys_metrics = vec![
                            tsdb_arrow::schema::DataPoint::new("sys_cpu", ts_us)
                                .with_tag("host", "local")
                                .with_field("usage_pct", tsdb_arrow::schema::FieldValue::Float(snapshot.cpu_pct))
                                .with_field("temp_c", tsdb_arrow::schema::FieldValue::Float(snapshot.cpu_temp_c))
                                .with_field("load_avg_1m", tsdb_arrow::schema::FieldValue::Float(snapshot.load_avg_1m)),
                            tsdb_arrow::schema::DataPoint::new("sys_memory", ts_us)
                                .with_tag("host", "local")
                                .with_field("usage_pct", tsdb_arrow::schema::FieldValue::Float(snapshot.sys_memory_pct))
                                .with_field("total_bytes", tsdb_arrow::schema::FieldValue::Integer(snapshot.sys_memory_total_bytes as i64))
                                .with_field("used_bytes", tsdb_arrow::schema::FieldValue::Integer(snapshot.sys_memory_used_bytes as i64)),
                            tsdb_arrow::schema::DataPoint::new("sys_disk", ts_us)
                                .with_tag("host", "local")
                                .with_field("usage_pct", tsdb_arrow::schema::FieldValue::Float(snapshot.sys_disk_pct))
                                .with_field("total_bytes", tsdb_arrow::schema::FieldValue::Integer(snapshot.sys_disk_total_bytes as i64))
                                .with_field("used_bytes", tsdb_arrow::schema::FieldValue::Integer(snapshot.sys_disk_used_bytes as i64)),
                            tsdb_arrow::schema::DataPoint::new("sys_network", ts_us)
                                .with_tag("host", "local")
                                .with_field("rx_bytes", tsdb_arrow::schema::FieldValue::Integer(snapshot.net_rx_bytes as i64))
                                .with_field("tx_bytes", tsdb_arrow::schema::FieldValue::Integer(snapshot.net_tx_bytes as i64)),
                            tsdb_arrow::schema::DataPoint::new("sys_temp", ts_us)
                                .with_tag("host", "local")
                                .with_field("cpu_temp_c", tsdb_arrow::schema::FieldValue::Float(snapshot.cpu_temp_c))
                                .with_field("gpu_temp_c", tsdb_arrow::schema::FieldValue::Float(snapshot.gpu_temp_c)),
                        ];
                        if let Err(e) = engine_for_writer.write_batch(&sys_metrics) {
                            tracing::warn!("write sys metrics: {}", e);
                        }

                        mgr.record_snapshot(snapshot);
                        drop(mgr);

                        interval = tokio::time::interval(std::time::Duration::from_secs(configured_interval));
                        interval.tick().await;
                    }
                    _ = shutdown_rx.changed() => {
                        tracing::info!("metrics publisher shutting down");
                        break;
                    }
                }
            }
        });

        tracing::info!("admin server ready, waiting for requests...");

        let server_clone = self.rep_socket.clone();
        let handle = self.inner.handle_request_ptr();
        let shutdown_rx3 = self.inner.shutdown.subscribe();

        tokio::task::spawn_blocking(move || {
            let shutdown_rx = shutdown_rx3;
            loop {
                if shutdown_rx.has_changed().unwrap_or(true) {
                    tracing::info!("nng recv loop received shutdown signal");
                    break;
                }
                match server_clone.recv() {
                    Ok(msg) => {
                        let data = msg.as_slice();
                        let request: std::result::Result<AdminRequest, _> =
                            serde_json::from_slice(data);
                        let response = match request {
                            Ok(req) => {
                                let rt = tokio::runtime::Handle::current();
                                rt.block_on(async { handle(req).await })
                            }
                            Err(e) => AdminResponse::err(format!("invalid request: {}", e)),
                        };
                        let reply_json = serde_json::to_string(&response).unwrap_or_else(|e| {
                            format!("{{\"success\":false,\"error\":\"{}\"}}", e)
                        });
                        let mut reply = Message::new();
                        reply.push_back(reply_json.as_bytes());
                        if let Err(e) = server_clone.send(reply) {
                            tracing::warn!("send reply error: {:?}", e);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("recv error: {:?}", e);
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                }
            }
        });

        let mut shutdown_rx2 = self.inner.shutdown.subscribe();
        shutdown_rx2
            .changed()
            .await
            .map_err(|_| AdminError::Nng("shutdown channel closed".into()))?;

        tracing::info!("admin server shutting down");
        Ok(())
    }
}

impl AdminServer {
    fn handle_request_ptr(&self) -> RequestHandler {
        let service_mgr = self.service_mgr.clone();
        let config_mgr = self.config_mgr.clone();
        let metrics = self.metrics.clone();
        let engine = self.engine.clone();
        let parquet_api = self.parquet_api.clone();
        let sql_api = self.sql_api.clone();
        let lifecycle_api = self.lifecycle_api.clone();
        let iceberg_api = self.iceberg_api.clone();
        let parquet_dir = self.parquet_dir.clone();

        Arc::new(move |req| {
            let service_mgr = service_mgr.clone();
            let config_mgr = config_mgr.clone();
            let metrics = metrics.clone();
            let engine = engine.clone();
            let parquet_api = parquet_api.clone();
            let sql_api = sql_api.clone();
            let lifecycle_api = lifecycle_api.clone();
            let iceberg_api = iceberg_api.clone();
            let parquet_dir = parquet_dir.clone();

            Box::pin(async move {
                match req {
                    AdminRequest::ServiceList => {
                        let list = service_mgr.list_services().await;
                        AdminResponse::ok(serde_json::to_value(&list).unwrap_or_default())
                    }
                    AdminRequest::ServiceCreate {
                        name,
                        port,
                        config,
                        data_dir,
                        engine: eng,
                        enable_iceberg,
                    } => match service_mgr
                        .create_service(&name, port, &config, &data_dir, &eng, enable_iceberg)
                        .await
                    {
                        Ok(info) => {
                            AdminResponse::ok(serde_json::to_value(&info).unwrap_or_default())
                        }
                        Err(e) => AdminResponse::err(e.to_string()),
                    },
                    AdminRequest::ServiceStart { name } => {
                        match service_mgr.start_service(&name).await {
                            Ok(info) => {
                                AdminResponse::ok(serde_json::to_value(&info).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }
                    AdminRequest::ServiceStop { name } => {
                        match service_mgr.stop_service(&name).await {
                            Ok(info) => {
                                AdminResponse::ok(serde_json::to_value(&info).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }
                    AdminRequest::ServiceRestart { name } => {
                        match service_mgr.restart_service(&name).await {
                            Ok(info) => {
                                AdminResponse::ok(serde_json::to_value(&info).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }
                    AdminRequest::ServiceStatus { name } => {
                        match service_mgr.get_service(&name).await {
                            Ok(info) => {
                                AdminResponse::ok(serde_json::to_value(&info).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }
                    AdminRequest::ServiceDelete { name } => {
                        match service_mgr.delete_service(&name).await {
                            Ok(()) => AdminResponse::ok(serde_json::json!({"deleted": name})),
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }
                    AdminRequest::ServiceLogs { name, lines } => {
                        let log_lines = lines.unwrap_or(100);
                        match service_mgr.get_service_logs(&name, log_lines).await {
                            Ok(logs) => {
                                AdminResponse::ok(serde_json::to_value(&logs).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }

                    AdminRequest::ConfigListProfiles => {
                        let mgr = config_mgr.read().await;
                        let profiles = mgr.list_profiles();
                        AdminResponse::ok(serde_json::to_value(&profiles).unwrap_or_default())
                    }
                    AdminRequest::ConfigGetProfile { name } => {
                        let mgr = config_mgr.read().await;
                        match mgr.get_profile(&name) {
                            Ok(p) => {
                                AdminResponse::ok(serde_json::to_value(&p).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }
                    AdminRequest::ConfigSaveProfile { name, content } => {
                        let mut mgr = config_mgr.write().await;
                        match mgr.save_profile(&name, &content) {
                            Ok(p) => {
                                AdminResponse::ok(serde_json::to_value(&p).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }
                    AdminRequest::ConfigDeleteProfile { name } => {
                        let mut mgr = config_mgr.write().await;
                        match mgr.delete_profile(&name) {
                            Ok(_) => AdminResponse::ok(serde_json::json!({"deleted": name})),
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }
                    AdminRequest::ConfigApply { service, profile } => {
                        let mgr = config_mgr.read().await;
                        match mgr.get_profile(&profile) {
                            Ok(_p) => {
                                drop(mgr);
                                match service_mgr.apply_config(&service, &profile).await {
                                    Ok(info) => AdminResponse::ok(serde_json::json!({
                                        "service": service,
                                        "profile": profile,
                                        "applied": true,
                                        "info": serde_json::to_value(&info).unwrap_or_default()
                                    })),
                                    Err(e) => AdminResponse::err(e.to_string()),
                                }
                            }
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }
                    AdminRequest::ConfigCompare {
                        profile_a,
                        profile_b,
                    } => {
                        let mgr = config_mgr.read().await;
                        match mgr.compare(&profile_a, &profile_b) {
                            Ok(diffs) => {
                                AdminResponse::ok(serde_json::to_value(&diffs).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }

                    AdminRequest::TestSql { service: _, sql } => {
                        let runner = TestRunner::new(engine.clone(), parquet_dir.clone());
                        match runner.run_sql(&sql).await {
                            Ok(result) => {
                                AdminResponse::ok(serde_json::to_value(&result).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }
                    AdminRequest::TestWriteBench {
                        service: _,
                        measurement,
                        total_points,
                        workers,
                        batch_size,
                    } => {
                        let runner = TestRunner::new(engine.clone(), parquet_dir.clone());
                        match runner
                            .run_write_bench(&measurement, total_points, workers, batch_size)
                            .await
                        {
                            Ok(result) => {
                                AdminResponse::ok(serde_json::to_value(&result).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }
                    AdminRequest::TestReadBench {
                        service: _,
                        measurement,
                        queries,
                        workers,
                    } => {
                        let runner = TestRunner::new(engine.clone(), parquet_dir.clone());
                        match runner.run_read_bench(&measurement, queries, workers).await {
                            Ok(result) => {
                                AdminResponse::ok(serde_json::to_value(&result).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }
                    AdminRequest::TestGenerateBusinessData {
                        scenario,
                        points_per_series,
                        skip_auto_demote,
                    } => {
                        let runner = TestRunner::new(engine.clone(), parquet_dir.clone());
                        match runner
                            .generate_business_data(&scenario, points_per_series, skip_auto_demote)
                            .await
                        {
                            Ok(result) => AdminResponse::ok(result),
                            Err(e) => AdminResponse::err(e.to_string()),
                        }
                    }
                    AdminRequest::IcebergListTables => match iceberg_api.as_ref() {
                        Some(api) => match api.list_tables() {
                            Ok(tables) => {
                                AdminResponse::ok(serde_json::to_value(&tables).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e),
                        },
                        None => AdminResponse::err("iceberg not enabled"),
                    },
                    AdminRequest::IcebergCreateTable {
                        name,
                        schema,
                        partition_type,
                    } => match iceberg_api.as_ref() {
                        Some(api) => {
                            let schema_def: crate::iceberg_api::SchemaDefinition =
                                match serde_json::from_value(schema) {
                                    Ok(s) => s,
                                    Err(e) => {
                                        return AdminResponse::err(format!("invalid schema: {}", e))
                                    }
                                };
                            match api.create_table(&name, &schema_def, &partition_type) {
                                Ok(msg) => AdminResponse::ok(serde_json::json!({"message": msg})),
                                Err(e) => AdminResponse::err(e),
                            }
                        }
                        None => AdminResponse::err("iceberg not enabled"),
                    },
                    AdminRequest::IcebergDropTable { name } => match iceberg_api.as_ref() {
                        Some(api) => match api.drop_table(&name) {
                            Ok(msg) => AdminResponse::ok(serde_json::json!({"message": msg})),
                            Err(e) => AdminResponse::err(e),
                        },
                        None => AdminResponse::err("iceberg not enabled"),
                    },
                    AdminRequest::IcebergTableDetail { name } => match iceberg_api.as_ref() {
                        Some(api) => match api.table_detail(&name) {
                            Ok(detail) => {
                                AdminResponse::ok(serde_json::to_value(&detail).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e),
                        },
                        None => AdminResponse::err("iceberg not enabled"),
                    },
                    AdminRequest::IcebergAppend { table, datapoints } => {
                        match iceberg_api.as_ref() {
                            Some(api) => match api.append_data(&table, &datapoints) {
                                Ok(msg) => AdminResponse::ok(serde_json::json!({"message": msg})),
                                Err(e) => AdminResponse::err(e),
                            },
                            None => AdminResponse::err("iceberg not enabled"),
                        }
                    }
                    AdminRequest::IcebergScan { table, limit } => match iceberg_api.as_ref() {
                        Some(api) => match api.scan_table(&table, limit) {
                            Ok(result) => {
                                AdminResponse::ok(serde_json::to_value(&result).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e),
                        },
                        None => AdminResponse::err("iceberg not enabled"),
                    },
                    AdminRequest::IcebergSnapshots { table } => match iceberg_api.as_ref() {
                        Some(api) => match api.snapshots(&table) {
                            Ok(snaps) => {
                                AdminResponse::ok(serde_json::to_value(&snaps).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e),
                        },
                        None => AdminResponse::err("iceberg not enabled"),
                    },
                    AdminRequest::IcebergRollback { table, snapshot_id } => {
                        match iceberg_api.as_ref() {
                            Some(api) => match api.rollback(&table, snapshot_id) {
                                Ok(msg) => AdminResponse::ok(serde_json::json!({"message": msg})),
                                Err(e) => AdminResponse::err(e),
                            },
                            None => AdminResponse::err("iceberg not enabled"),
                        }
                    }
                    AdminRequest::IcebergCompact { table } => match iceberg_api.as_ref() {
                        Some(api) => match api.compact(&table) {
                            Ok(msg) => AdminResponse::ok(serde_json::json!({"message": msg})),
                            Err(e) => AdminResponse::err(e),
                        },
                        None => AdminResponse::err("iceberg not enabled"),
                    },
                    AdminRequest::IcebergExpire { table, keep_days } => {
                        match iceberg_api.as_ref() {
                            Some(api) => match api.expire_snapshots(&table, keep_days) {
                                Ok(msg) => AdminResponse::ok(serde_json::json!({"message": msg})),
                                Err(e) => AdminResponse::err(e),
                            },
                            None => AdminResponse::err("iceberg not enabled"),
                        }
                    }
                    AdminRequest::IcebergUpdateSchema { table, changes } => {
                        match iceberg_api.as_ref() {
                            Some(api) => {
                                let schema_changes: Vec<tsdb_iceberg::SchemaChange> = changes
                                    .iter()
                                    .filter_map(|c| serde_json::from_value(c.clone()).ok())
                                    .collect();
                                if schema_changes.is_empty() {
                                    AdminResponse::err("no valid schema changes")
                                } else {
                                    match api.update_schema(&table, schema_changes) {
                                        Ok(msg) => {
                                            AdminResponse::ok(serde_json::json!({"message": msg}))
                                        }
                                        Err(e) => AdminResponse::err(e),
                                    }
                                }
                            }
                            None => AdminResponse::err("iceberg not enabled"),
                        }
                    }

                    AdminRequest::MetricsHealth { service } => {
                        let mgr = metrics.read().await;
                        let health = mgr.health(&service);
                        AdminResponse::ok(serde_json::to_value(&health).unwrap_or_default())
                    }
                    AdminRequest::MetricsStats { service: _ } => {
                        let mgr = metrics.read().await;
                        let stats = mgr.stats();
                        AdminResponse::ok(serde_json::to_value(&stats).unwrap_or_default())
                    }
                    AdminRequest::MetricsTimeseries {
                        service: _,
                        metric,
                        range_secs,
                    } => {
                        let mgr = metrics.read().await;
                        let ts = mgr.timeseries(&metric, range_secs);
                        AdminResponse::ok(serde_json::to_value(&ts).unwrap_or_default())
                    }
                    AdminRequest::MetricsAlerts { service } => {
                        let mgr = metrics.read().await;
                        let health = mgr.health(&service);
                        let alerts = mgr.alerts(&health);
                        AdminResponse::ok(serde_json::to_value(&alerts).unwrap_or_default())
                    }

                    AdminRequest::CollectorStatus => {
                        let mgr = metrics.read().await;
                        let status = mgr.collector_status();
                        AdminResponse::ok(serde_json::to_value(&status).unwrap_or_default())
                    }
                    AdminRequest::CollectorConfigure {
                        interval_secs,
                        enabled,
                    } => {
                        let mgr = metrics.read().await;
                        mgr.configure_collector(interval_secs, enabled);
                        let status = mgr.collector_status();
                        AdminResponse::ok(serde_json::to_value(&status).unwrap_or_default())
                    }
                    AdminRequest::CollectorStart => {
                        let mgr = metrics.read().await;
                        mgr.start_collector();
                        let status = mgr.collector_status();
                        AdminResponse::ok(serde_json::to_value(&status).unwrap_or_default())
                    }
                    AdminRequest::CollectorStop => {
                        let mgr = metrics.read().await;
                        mgr.stop_collector();
                        let status = mgr.collector_status();
                        AdminResponse::ok(serde_json::to_value(&status).unwrap_or_default())
                    }

                    AdminRequest::RocksdbStats => {
                        let mgr = metrics.read().await;
                        let overview = mgr.rocksdb_overview();
                        AdminResponse::ok(serde_json::to_value(&overview).unwrap_or_default())
                    }
                    AdminRequest::RocksdbCfList => {
                        let mgr = metrics.read().await;
                        let cfs = mgr.rocksdb_cf_list();
                        AdminResponse::ok(serde_json::to_value(&cfs).unwrap_or_default())
                    }
                    AdminRequest::RocksdbCfDetail { name } => {
                        let mgr = metrics.read().await;
                        match mgr.rocksdb_cf_detail(&name) {
                            Some(detail) => {
                                AdminResponse::ok(serde_json::to_value(&detail).unwrap_or_default())
                            }
                            None => AdminResponse::err(format!("CF '{}' not found", name)),
                        }
                    }
                    AdminRequest::RocksdbCompact { cf } => {
                        let mgr = metrics.read().await;
                        match mgr.rocksdb_compact(cf.as_deref()) {
                            Ok(()) => {
                                AdminResponse::ok(serde_json::json!({"compacted": true, "cf": cf}))
                            }
                            Err(e) => AdminResponse::err(e),
                        }
                    }
                    AdminRequest::RocksdbKvScan {
                        cf,
                        prefix,
                        start_key,
                        limit,
                    } => {
                        let mgr = metrics.read().await;
                        match mgr.rocksdb_kv_scan(
                            &cf,
                            prefix.as_deref(),
                            start_key.as_deref(),
                            limit.unwrap_or(20),
                        ) {
                            Ok(result) => {
                                AdminResponse::ok(serde_json::to_value(&result).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e),
                        }
                    }
                    AdminRequest::RocksdbKvGet { cf, key } => {
                        let mgr = metrics.read().await;
                        match mgr.rocksdb_kv_get(&cf, &key) {
                            Ok(entry) => {
                                AdminResponse::ok(serde_json::to_value(&entry).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e),
                        }
                    }
                    AdminRequest::RocksdbSeriesSchema => {
                        let mgr = metrics.read().await;
                        match mgr.rocksdb_series_schema() {
                            Ok(schema) => {
                                AdminResponse::ok(serde_json::to_value(&schema).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e),
                        }
                    }

                    AdminRequest::ParquetList => {
                        let files = parquet_api.list_parquet_files();
                        AdminResponse::ok(serde_json::to_value(&files).unwrap_or_default())
                    }
                    AdminRequest::ParquetFileDetail { path } => {
                        match parquet_api.file_detail(&path) {
                            Ok(info) => {
                                AdminResponse::ok(serde_json::to_value(&info).unwrap_or_default())
                            }
                            Err(e) => AdminResponse::err(e),
                        }
                    }
                    AdminRequest::ParquetPreview { path, limit } => {
                        match parquet_api.preview(&path, limit.unwrap_or(50)) {
                            Ok(preview) => AdminResponse::ok(
                                serde_json::to_value(&preview).unwrap_or_default(),
                            ),
                            Err(e) => AdminResponse::err(e),
                        }
                    }

                    AdminRequest::SqlExecute { sql } => match sql_api.execute(&sql).await {
                        Ok(result) => {
                            AdminResponse::ok(serde_json::to_value(&result).unwrap_or_default())
                        }
                        Err(e) => AdminResponse::err(e),
                    },
                    AdminRequest::SqlTables => match sql_api.tables().await {
                        Ok(tables) => {
                            AdminResponse::ok(serde_json::to_value(&tables).unwrap_or_default())
                        }
                        Err(e) => AdminResponse::err(e),
                    },

                    AdminRequest::LifecycleStatus => {
                        let status = lifecycle_api.status();
                        AdminResponse::ok(serde_json::to_value(&status).unwrap_or_default())
                    }
                    AdminRequest::LifecycleArchive { older_than_days } => {
                        match lifecycle_api.archive(older_than_days) {
                            Ok(archived) => {
                                AdminResponse::ok(serde_json::json!({"archived": archived}))
                            }
                            Err(e) => AdminResponse::err(e),
                        }
                    }
                    AdminRequest::LifecycleDemoteToWarm { cf_names } => {
                        match lifecycle_api.demote_to_warm(&cf_names) {
                            Ok(demoted) => {
                                AdminResponse::ok(serde_json::json!({"demoted": demoted}))
                            }
                            Err(e) => AdminResponse::err(e),
                        }
                    }
                    AdminRequest::LifecycleDemoteToCold { cf_names } => {
                        match lifecycle_api.demote_to_cold(&cf_names) {
                            Ok(demoted) => {
                                AdminResponse::ok(serde_json::json!({"demoted": demoted}))
                            }
                            Err(e) => AdminResponse::err(e),
                        }
                    }
                }
            })
        })
    }
}
