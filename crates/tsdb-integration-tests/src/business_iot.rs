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

    fn generate_iot_sensor_data(device_count: usize, interval_secs: i64, points_per_device: usize) -> Vec<DataPoint> {
        let base_ts = ts_now() - (points_per_device as i64 * interval_secs * 1_000_000);
        let mut dps = Vec::new();

        for device in 0..device_count {
            let device_id = format!("sensor_{:04}", device);
            let location = match device % 3 {
                0 => "factory_a",
                1 => "factory_b",
                _ => "warehouse",
            };
            let device_type = match device % 4 {
                0 => "temperature",
                1 => "humidity",
                2 => "pressure",
                _ => "vibration",
            };

            for point in 0..points_per_device {
                let ts = base_ts + point as i64 * interval_secs * 1_000_000;
                let noise = ((device * 7 + point * 13) % 100) as f64 * 0.01;

                let mut fields = tsdb_arrow::schema::Fields::new();
                match device_type {
                    "temperature" => {
                        fields.insert("value".to_string(), FieldValue::Float(22.0 + noise * 10.0));
                        fields.insert("unit".to_string(), FieldValue::String("celsius".to_string()));
                    }
                    "humidity" => {
                        fields.insert("value".to_string(), FieldValue::Float(55.0 + noise * 20.0));
                        fields.insert("unit".to_string(), FieldValue::String("percent".to_string()));
                    }
                    "pressure" => {
                        fields.insert("value".to_string(), FieldValue::Float(1013.0 + noise * 5.0));
                        fields.insert("unit".to_string(), FieldValue::String("hpa".to_string()));
                    }
                    _ => {
                        fields.insert("value".to_string(), FieldValue::Float(noise * 50.0));
                        fields.insert("unit".to_string(), FieldValue::String("mm_s".to_string()));
                    }
                }
                fields.insert("battery".to_string(), FieldValue::Float(100.0 - point as f64 * 0.1));
                fields.insert("signal_dbm".to_string(), FieldValue::Integer(-40 - (point % 20) as i64));

                let mut tags = Tags::new();
                tags.insert("device_id".to_string(), device_id.clone());
                tags.insert("location".to_string(), location.to_string());
                tags.insert("device_type".to_string(), device_type.to_string());

                dps.push(DataPoint {
                    measurement: "iot_sensor".to_string(),
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
        engine.register_from_datapoints(measurement, datapoints).unwrap();

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
        engine.session_context().deregister_table(measurement).unwrap();
        engine
            .session_context()
            .register_table(measurement, Arc::new(mem_table))
            .unwrap();
    }

    #[test]
    fn test_iot_high_frequency_write() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = generate_iot_sensor_data(50, 10, 1000);
        assert_eq!(dps.len(), 50 * 1000);

        let start = std::time::Instant::now();
        db.write_batch(&dps).unwrap();
        let elapsed = start.elapsed();
        let qps = dps.len() as f64 / elapsed.as_secs_f64();
        eprintln!("IoT write: {} points in {:.2}s ({:.0} pts/s)", dps.len(), elapsed.as_secs_f64(), qps);

        assert!(qps > 1000.0, "IoT write QPS should be > 1000, got {:.0}", qps);
    }

    #[test]
    fn test_iot_multi_device_range_query() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = generate_iot_sensor_data(20, 60, 100);
        db.write_batch(&dps).unwrap();

        let base_ts = ts_now() - 100 * 60 * 1_000_000;
        let result = db.read_range("iot_sensor", base_ts, ts_now()).unwrap();
        assert!(!result.is_empty());

        let device_ids: std::collections::HashSet<_> = result.iter()
            .filter_map(|dp| dp.tags.get("device_id").cloned())
            .collect();
        assert!(device_ids.len() >= 10, "should have data from multiple devices");
    }

    #[tokio::test]
    async fn test_iot_sql_aggregation_per_location() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = generate_iot_sensor_data(30, 60, 50);
        db.write_batch(&dps).unwrap();

        let base_ts = ts_now() - 50 * 60 * 1_000_000;
        let datapoints = db.read_range("iot_sensor", base_ts, ts_now()).unwrap();

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        register_table_for_sql(&engine, "iot_sensor", &datapoints).await;

        let result = engine
            .execute("SELECT tag_location, AVG(value) as avg_value FROM iot_sensor GROUP BY tag_location")
            .await
            .unwrap();

        assert!(!result.rows.is_empty(), "should have aggregation results per location");
        assert!(result.columns.contains(&"avg_value".to_string()));
        assert!(result.columns.contains(&"tag_location".to_string()));
    }

    #[tokio::test]
    async fn test_iot_sql_filter_by_device_type() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = generate_iot_sensor_data(20, 60, 50);
        db.write_batch(&dps).unwrap();

        let base_ts = ts_now() - 50 * 60 * 1_000_000;
        let datapoints = db.read_range("iot_sensor", base_ts, ts_now()).unwrap();

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        register_table_for_sql(&engine, "iot_sensor", &datapoints).await;

        let result = engine
            .execute("SELECT * FROM iot_sensor WHERE tag_device_type = 'temperature'")
            .await
            .unwrap();

        assert!(!result.rows.is_empty());
    }

    #[test]
    fn test_iot_multi_get_latest_values() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = generate_iot_sensor_data(10, 10, 100);
        db.write_batch(&dps).unwrap();

        let keys: Vec<(Tags, i64)> = dps.iter()
            .filter(|dp| dp.tags.get("device_id").map(|d| d.starts_with("sensor_000")).unwrap_or(false))
            .take(5)
            .map(|dp| (dp.tags.clone(), dp.timestamp))
            .collect();

        let results = db.multi_get("iot_sensor", &keys).unwrap();
        assert_eq!(results.len(), 5);
        for r in &results {
            assert!(r.is_some());
        }
    }

    #[test]
    fn test_iot_arrow_adapter_projection() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = generate_iot_sensor_data(5, 60, 20);
        db.write_batch(&dps).unwrap();

        let base_ts = ts_now() - 20 * 60 * 1_000_000;
        let result = db.read_range_projection("iot_sensor", base_ts, ts_now(), &["value", "battery"]).unwrap();
        assert!(!result.is_empty());

        for dp in &result {
            assert!(dp.fields.contains_key("value"), "should contain projected field 'value'");
            assert!(dp.fields.contains_key("battery"), "should contain projected field 'battery'");
            assert!(!dp.fields.contains_key("signal_dbm"), "should not contain non-projected field");
        }
    }

    #[tokio::test]
    async fn test_iot_full_pipeline_rocksdb_to_parquet_to_sql() {
        let rocks_dir = tempfile::tempdir().unwrap();
        let parquet_dir = tempfile::tempdir().unwrap();

        let db = TsdbRocksDb::open(rocks_dir.path(), RocksDbConfig::default()).unwrap();
        let dps = generate_iot_sensor_data(10, 60, 30);
        db.write_batch(&dps).unwrap();

        let base_ts = ts_now() - 30 * 60 * 1_000_000;
        let rocks_result = db.read_range("iot_sensor", base_ts, ts_now()).unwrap();
        assert!(!rocks_result.is_empty());

        use tsdb_parquet::partition::{PartitionConfig, PartitionManager};
        use tsdb_parquet::writer::{TsdbParquetWriter, WriteBufferConfig};

        let pm = PartitionManager::new(parquet_dir.path(), PartitionConfig::default()).unwrap();
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default());
        writer.write_batch(&rocks_result).unwrap();
        writer.flush_all().unwrap();

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(parquet_dir.path());
        engine.register_from_datapoints("iot_sensor", &rocks_result).unwrap();

        let result = engine
            .execute("SELECT tag_device_type, AVG(value) as avg_val, COUNT(*) as cnt FROM iot_sensor GROUP BY tag_device_type")
            .await
            .unwrap();

        assert!(!result.rows.is_empty());
        assert!(result.columns.contains(&"avg_val".to_string()));
        assert!(result.columns.contains(&"cnt".to_string()));
    }

    #[test]
    fn test_iot_ttl_and_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let mut dps = Vec::new();
        let now = ts_now();

        for day in 0..5i64 {
            let ts = now - day * 86_400_000_000;
            let mut tags = Tags::new();
            tags.insert("device_id".to_string(), "sensor_0001".to_string());
            let mut fields = tsdb_arrow::schema::Fields::new();
            fields.insert("value".to_string(), FieldValue::Float(22.0 + day as f64));
            dps.push(DataPoint::new("iot_sensor", ts).with_tag("device_id", "sensor_0001").with_field("value", FieldValue::Float(22.0 + day as f64)));
        }

        db.write_batch(&dps).unwrap();

        let cfs = db.list_ts_cfs();
        assert!(cfs.len() >= 2, "should have multiple day-partitioned CFs");

        let today = chrono::Utc::now().date_naive();
        let dropped = tsdb_rocksdb::TtlManager::drop_expired_cfs(&db, today, 3).unwrap();
        eprintln!("Dropped {} expired CFs (retention=3d)", dropped.len());
    }
}
