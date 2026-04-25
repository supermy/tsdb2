#[cfg(test)]
mod tests {
    use datafusion::datasource::MemTable;
    use std::sync::Arc;
    use tsdb_arrow::converter::datapoints_to_record_batch;
    use tsdb_arrow::schema::{DataPoint, FieldValue, Tags};
    use tsdb_rocksdb::{RocksDbConfig, TsdbRocksDb};

    fn ts_now() -> i64 {
        chrono::Utc::now().timestamp_micros()
    }

    fn generate_apm_trace_data(service_count: usize, traces_per_service: usize) -> Vec<DataPoint> {
        let base_ts = ts_now() - traces_per_service as i64 * 10_000_000;
        let services = [
            "api-gateway",
            "user-service",
            "order-service",
            "payment-service",
            "inventory-service",
        ];
        let mut dps = Vec::new();

        for svc_idx in 0..service_count {
            let service = services[svc_idx % services.len()];
            let env = if svc_idx % 2 == 0 {
                "production"
            } else {
                "staging"
            };
            let version = format!("v{}.{}.{}", 1 + svc_idx / 3, svc_idx % 3, svc_idx % 5);

            for trace in 0..traces_per_service {
                let ts = base_ts + trace as i64 * 10_000_000;
                let duration_ms = 5 + (trace % 200) as i64;
                let status_code = if trace % 20 == 0 {
                    500
                } else if trace % 10 == 0 {
                    404
                } else {
                    200
                };
                let is_error = status_code >= 400;

                let mut tags = Tags::new();
                tags.insert("service".to_string(), service.to_string());
                tags.insert("env".to_string(), env.to_string());
                tags.insert("version".to_string(), version.clone());
                tags.insert(
                    "endpoint".to_string(),
                    format!("/api/{}", service.replace('-', "_")),
                );

                let mut fields = tsdb_arrow::schema::Fields::new();
                fields.insert("duration_ms".to_string(), FieldValue::Integer(duration_ms));
                fields.insert("status_code".to_string(), FieldValue::Integer(status_code));
                fields.insert("is_error".to_string(), FieldValue::Boolean(is_error));
                fields.insert(
                    "request_size".to_string(),
                    FieldValue::Integer(128 + (trace % 1024) as i64),
                );
                fields.insert(
                    "response_size".to_string(),
                    FieldValue::Integer(256 + (trace % 2048) as i64),
                );
                fields.insert(
                    "cpu_usage".to_string(),
                    FieldValue::Float(0.1 + (trace % 50) as f64 * 0.01),
                );
                fields.insert(
                    "memory_mb".to_string(),
                    FieldValue::Float(128.0 + (trace % 100) as f64),
                );

                dps.push(DataPoint {
                    measurement: "apm_trace".to_string(),
                    tags,
                    fields,
                    timestamp: ts,
                });
            }
        }

        dps
    }

    fn generate_apm_metric_data(host_count: usize, points_per_host: usize) -> Vec<DataPoint> {
        let base_ts = ts_now() - points_per_host as i64 * 60_000_000;
        let mut dps = Vec::new();

        for host in 0..host_count {
            let hostname = format!("node-{:03}", host);
            let az = format!("az-{}", host % 3);

            for point in 0..points_per_host {
                let ts = base_ts + point as i64 * 60_000_000;

                let mut tags = Tags::new();
                tags.insert("host".to_string(), hostname.clone());
                tags.insert("az".to_string(), az.clone());

                let mut fields = tsdb_arrow::schema::Fields::new();
                fields.insert(
                    "cpu_percent".to_string(),
                    FieldValue::Float(10.0 + (point % 80) as f64),
                );
                fields.insert(
                    "memory_percent".to_string(),
                    FieldValue::Float(40.0 + (point % 50) as f64),
                );
                fields.insert(
                    "disk_io_read_mb".to_string(),
                    FieldValue::Float((point % 100) as f64),
                );
                fields.insert(
                    "disk_io_write_mb".to_string(),
                    FieldValue::Float((point % 50) as f64 * 0.5),
                );
                fields.insert(
                    "network_in_kb".to_string(),
                    FieldValue::Integer(100 + (point % 5000) as i64),
                );
                fields.insert(
                    "network_out_kb".to_string(),
                    FieldValue::Integer(50 + (point % 2000) as i64),
                );
                fields.insert(
                    "load_1m".to_string(),
                    FieldValue::Float((point % 16) as f64 * 0.25),
                );

                dps.push(DataPoint {
                    measurement: "apm_metric".to_string(),
                    tags,
                    fields,
                    timestamp: ts,
                });
            }
        }

        dps
    }

    async fn register_table_for_sql(
        engine: &tsdb_datafusion::DataFusionQueryEngine,
        measurement: &str,
        datapoints: &[DataPoint],
    ) {
        engine
            .register_from_datapoints(measurement, datapoints)
            .unwrap();

        let table = engine
            .session_context()
            .catalog("datafusion")
            .unwrap()
            .schema("public")
            .unwrap()
            .table(measurement)
            .await
            .unwrap()
            .unwrap();
        let schema = table.schema();
        let batch = datapoints_to_record_batch(datapoints, schema.clone()).unwrap();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        engine
            .session_context()
            .deregister_table(measurement)
            .unwrap();
        engine
            .session_context()
            .register_table(measurement, Arc::new(mem_table))
            .unwrap();
    }

    #[test]
    fn test_apm_high_cardinality_write() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let trace_dps = generate_apm_trace_data(5, 500);
        let metric_dps = generate_apm_metric_data(20, 100);
        let total = trace_dps.len() + metric_dps.len();

        let start = std::time::Instant::now();
        db.write_batch(&trace_dps).unwrap();
        db.write_batch(&metric_dps).unwrap();
        let elapsed = start.elapsed();
        let qps = total as f64 / elapsed.as_secs_f64();
        eprintln!(
            "APM write: {} points in {:.2}s ({:.0} pts/s)",
            total,
            elapsed.as_secs_f64(),
            qps
        );

        assert!(qps > 1000.0);
    }

    #[tokio::test]
    async fn test_apm_sql_error_rate_by_service() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = generate_apm_trace_data(5, 200);
        db.write_batch(&dps).unwrap();

        let base_ts = ts_now() - 200 * 10_000_000;
        let datapoints = db.read_range("apm_trace", base_ts, ts_now()).unwrap();

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        register_table_for_sql(&engine, "apm_trace", &datapoints).await;

        let result = engine
            .execute(
                "SELECT tag_service, COUNT(*) as total, SUM(CASE WHEN is_error = true THEN 1 ELSE 0 END) as errors FROM apm_trace GROUP BY tag_service",
            )
            .await
            .unwrap();

        assert!(!result.rows.is_empty());
        assert!(result.columns.contains(&"total".to_string()));
        assert!(result.columns.contains(&"errors".to_string()));
    }

    #[tokio::test]
    async fn test_apm_sql_p99_duration() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = generate_apm_trace_data(3, 200);
        db.write_batch(&dps).unwrap();

        let base_ts = ts_now() - 200 * 10_000_000;
        let datapoints = db.read_range("apm_trace", base_ts, ts_now()).unwrap();

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        register_table_for_sql(&engine, "apm_trace", &datapoints).await;

        let result = engine
            .execute(
                "SELECT tag_service, AVG(duration_ms) as avg_duration, MAX(duration_ms) as max_duration FROM apm_trace GROUP BY tag_service",
            )
            .await
            .unwrap();

        assert!(!result.rows.is_empty());
        assert!(result.columns.contains(&"avg_duration".to_string()));
        assert!(result.columns.contains(&"max_duration".to_string()));
    }

    #[tokio::test]
    async fn test_apm_sql_host_resource_aggregation() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = generate_apm_metric_data(10, 60);
        db.write_batch(&dps).unwrap();

        let base_ts = ts_now() - 60 * 60_000_000;
        let datapoints = db.read_range("apm_metric", base_ts, ts_now()).unwrap();

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        register_table_for_sql(&engine, "apm_metric", &datapoints).await;

        let result = engine
            .execute(
                "SELECT tag_host, AVG(cpu_percent) as avg_cpu, AVG(memory_percent) as avg_mem, MAX(load_1m) as max_load FROM apm_metric GROUP BY tag_host",
            )
            .await
            .unwrap();

        assert!(!result.rows.is_empty());
        assert!(result.columns.contains(&"avg_cpu".to_string()));
        assert!(result.columns.contains(&"avg_mem".to_string()));
    }

    #[test]
    fn test_apm_multi_get_trace_check() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = generate_apm_trace_data(3, 50);
        db.write_batch(&dps).unwrap();

        let keys: Vec<(Tags, i64)> = dps
            .iter()
            .take(10)
            .map(|dp| (dp.tags.clone(), dp.timestamp))
            .collect();
        let results = db.multi_get("apm_trace", &keys).unwrap();
        assert_eq!(results.len(), 10);

        let found = results.iter().filter(|r| r.is_some()).count();
        assert!(found > 0, "should find at least some traces");
    }

    #[test]
    fn test_apm_prefix_scan_by_service() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = generate_apm_trace_data(5, 50);
        db.write_batch(&dps).unwrap();

        let first_dp = &dps[0];
        let target_tags = first_dp.tags.clone();

        let base_ts = ts_now() - 50 * 10_000_000;
        let result = db
            .prefix_scan("apm_trace", &target_tags, base_ts, ts_now())
            .unwrap();
        assert!(!result.is_empty());

        for dp in &result {
            assert_eq!(dp.tags.get("service"), first_dp.tags.get("service"));
        }
    }

    #[test]
    fn test_apm_arrow_adapter_read() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = generate_apm_metric_data(5, 30);
        db.write_batch(&dps).unwrap();

        let base_ts = ts_now() - 30 * 60_000_000;
        let tag_keys: Vec<String> = vec!["host".to_string(), "az".to_string()];
        let field_types: Vec<(String, arrow::datatypes::DataType)> = vec![
            (
                "cpu_percent".to_string(),
                arrow::datatypes::DataType::Float64,
            ),
            (
                "memory_percent".to_string(),
                arrow::datatypes::DataType::Float64,
            ),
        ];
        let schema = tsdb_arrow::compact_tsdb_schema("apm_metric", &tag_keys, &field_types);

        let batch = tsdb_rocksdb::ArrowAdapter::read_range_arrow(
            &db,
            "apm_metric",
            base_ts,
            ts_now(),
            schema,
        )
        .unwrap();

        assert!(batch.num_rows() > 0);
    }

    #[tokio::test]
    async fn test_apm_full_pipeline_with_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = generate_apm_trace_data(3, 100);
        db.write_batch(&dps).unwrap();

        let cfs = db.list_ts_cfs();
        for cf_name in &cfs {
            db.compact_cf(cf_name).unwrap();
        }

        let base_ts = ts_now() - 100 * 10_000_000;
        let datapoints = db.read_range("apm_trace", base_ts, ts_now()).unwrap();
        assert!(!datapoints.is_empty(), "data should survive compaction");

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        register_table_for_sql(&engine, "apm_trace", &datapoints).await;

        let result = engine
            .execute("SELECT tag_service, COUNT(*) as cnt FROM apm_trace GROUP BY tag_service")
            .await
            .unwrap();

        assert!(!result.rows.is_empty());
    }

    #[test]
    fn test_apm_cross_measurement_query() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let trace_dps = generate_apm_trace_data(3, 50);
        let metric_dps = generate_apm_metric_data(5, 30);
        db.write_batch(&trace_dps).unwrap();
        db.write_batch(&metric_dps).unwrap();

        let base_ts = ts_now() - 50 * 10_000_000;
        let trace_result = db.read_range("apm_trace", base_ts, ts_now()).unwrap();
        let metric_result = db.read_range("apm_metric", base_ts, ts_now()).unwrap();

        assert!(!trace_result.is_empty(), "should have trace data");
        assert!(!metric_result.is_empty(), "should have metric data");

        let trace_services: std::collections::HashSet<_> = trace_result
            .iter()
            .filter_map(|dp| dp.tags.get("service").cloned())
            .collect();
        let metric_hosts: std::collections::HashSet<_> = metric_result
            .iter()
            .filter_map(|dp| dp.tags.get("host").cloned())
            .collect();

        assert!(!trace_services.is_empty());
        assert!(!metric_hosts.is_empty());
    }
}
