#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;
    use tsdb_arrow::engine::StorageEngine;
    use tsdb_arrow::schema::{DataPoint, FieldValue, Tags};
    use tsdb_rocksdb::RocksDbConfig;
    use tsdb_rocksdb::TsdbRocksDb;
    use tsdb_storage_arrow::config::ArrowStorageConfig;
    use tsdb_storage_arrow::engine::ArrowStorageEngine;

    fn ts_today() -> i64 {
        chrono::Utc::now()
            .date_naive()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros()
    }

    fn make_cpu_datapoints(n: usize) -> Vec<DataPoint> {
        let base_ts = ts_today();
        (0..n)
            .map(|i| {
                DataPoint::new("cpu", base_ts + i as i64 * 1_000_000)
                    .with_tag("host", format!("host_{:03}", i % 10))
                    .with_tag("region", if i % 2 == 0 { "us-west" } else { "us-east" })
                    .with_field("usage", FieldValue::Float(0.3 + i as f64 * 0.01))
                    .with_field("idle", FieldValue::Float(0.7 - i as f64 * 0.01))
                    .with_field("count", FieldValue::Integer(i as i64 * 10))
            })
            .collect()
    }

    fn open_rocksdb(dir: &TempDir) -> Arc<dyn StorageEngine> {
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        Arc::new(db)
    }

    fn open_arrow(dir: &TempDir) -> Arc<dyn StorageEngine> {
        let config = ArrowStorageConfig {
            wal_enabled: false,
            max_buffer_rows: 500,
            flush_interval_ms: 100,
            ..Default::default()
        };
        let engine = ArrowStorageEngine::open(dir.path(), config).unwrap();
        Arc::new(engine)
    }

    #[test]
    fn test_dual_engine_write_read_consistency() {
        let rocks_dir = TempDir::new().unwrap();
        let arrow_dir = TempDir::new().unwrap();

        let rocks = open_rocksdb(&rocks_dir);
        let arrow = open_arrow(&arrow_dir);

        let dps = make_cpu_datapoints(100);

        rocks.write_batch(&dps).unwrap();
        arrow.write_batch(&dps).unwrap();
        arrow.flush().unwrap();

        let base_ts = ts_today();
        let end_ts = base_ts + 100 * 1_000_000;

        let rocks_result = rocks
            .read_range("cpu", &Tags::new(), base_ts, end_ts)
            .unwrap();
        let arrow_result = arrow
            .read_range("cpu", &Tags::new(), base_ts, end_ts)
            .unwrap();

        assert!(!rocks_result.is_empty(), "RocksDB should return data");
        assert!(!arrow_result.is_empty(), "Arrow should return data");

        let mut rocks_ts: Vec<i64> = rocks_result.iter().map(|dp| dp.timestamp).collect();
        let mut arrow_ts: Vec<i64> = arrow_result.iter().map(|dp| dp.timestamp).collect();
        rocks_ts.sort();
        arrow_ts.sort();
        assert_eq!(
            rocks_ts.len(),
            arrow_ts.len(),
            "both engines should return same count"
        );
    }

    #[test]
    fn test_dual_engine_get_point() {
        let rocks_dir = TempDir::new().unwrap();
        let arrow_dir = TempDir::new().unwrap();

        let rocks = open_rocksdb(&rocks_dir);
        let arrow = open_arrow(&arrow_dir);

        let dps = make_cpu_datapoints(10);
        let target_ts = dps[5].timestamp;

        rocks.write_batch(&dps).unwrap();
        arrow.write_batch(&dps).unwrap();
        arrow.flush().unwrap();

        let mut tags = Tags::new();
        tags.insert("host".to_string(), dps[5].tags.get("host").unwrap().clone());
        tags.insert(
            "region".to_string(),
            dps[5].tags.get("region").unwrap().clone(),
        );

        let rocks_point = rocks.get_point("cpu", &tags, target_ts).unwrap();
        let arrow_point = arrow.get_point("cpu", &tags, target_ts).unwrap();

        assert!(rocks_point.is_some(), "RocksDB should find point");
        assert!(arrow_point.is_some(), "Arrow should find point");

        let rp = rocks_point.unwrap();
        let ap = arrow_point.unwrap();
        assert_eq!(rp.timestamp, ap.timestamp);
        assert_eq!(rp.measurement, ap.measurement);
    }

    #[test]
    fn test_dual_engine_list_measurements() {
        let rocks_dir = TempDir::new().unwrap();
        let arrow_dir = TempDir::new().unwrap();

        let rocks = open_rocksdb(&rocks_dir);
        let arrow = open_arrow(&arrow_dir);

        let cpu_dps = make_cpu_datapoints(5);
        let mem_dps: Vec<DataPoint> = (0..5)
            .map(|i| {
                DataPoint::new("mem", ts_today() + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_field("used", FieldValue::Float(50.0 + i as f64))
            })
            .collect();

        rocks.write_batch(&cpu_dps).unwrap();
        rocks.write_batch(&mem_dps).unwrap();
        arrow.write_batch(&cpu_dps).unwrap();
        arrow.write_batch(&mem_dps).unwrap();
        arrow.flush().unwrap();

        let rocks_measurements = rocks.list_measurements();
        let arrow_measurements = arrow.list_measurements();

        assert!(rocks_measurements.contains(&"cpu".to_string()));
        assert!(rocks_measurements.contains(&"mem".to_string()));
        assert!(arrow_measurements.contains(&"cpu".to_string()));
        assert!(arrow_measurements.contains(&"mem".to_string()));
    }

    #[test]
    fn test_dual_engine_measurement_schema() {
        let rocks_dir = TempDir::new().unwrap();
        let arrow_dir = TempDir::new().unwrap();

        let rocks = open_rocksdb(&rocks_dir);
        let arrow = open_arrow(&arrow_dir);

        let dps = make_cpu_datapoints(20);
        rocks.write_batch(&dps).unwrap();
        arrow.write_batch(&dps).unwrap();
        arrow.flush().unwrap();

        let rocks_schema = rocks.measurement_schema("cpu");
        let arrow_schema = arrow.measurement_schema("cpu");

        assert!(rocks_schema.is_some(), "RocksDB should return schema");
        assert!(arrow_schema.is_some(), "Arrow should return schema");

        let rs = rocks_schema.unwrap();
        let as_ = arrow_schema.unwrap();

        let rocks_names: Vec<&str> = rs.fields().iter().map(|f| f.name().as_str()).collect();
        let arrow_names: Vec<&str> = as_.fields().iter().map(|f| f.name().as_str()).collect();

        assert!(rocks_names.contains(&"timestamp"));
        assert!(rocks_names.contains(&"usage"));
        assert!(arrow_names.contains(&"timestamp"));
        assert!(arrow_names.contains(&"usage"));
    }

    #[test]
    fn test_dual_engine_flush_semantics() {
        let rocks_dir = TempDir::new().unwrap();
        let arrow_dir = TempDir::new().unwrap();

        let rocks = open_rocksdb(&rocks_dir);
        let arrow = open_arrow(&arrow_dir);

        let dps = make_cpu_datapoints(10);
        rocks.write_batch(&dps).unwrap();
        arrow.write_batch(&dps).unwrap();

        rocks.flush().unwrap();
        arrow.flush().unwrap();

        let base_ts = ts_today();
        let end_ts = base_ts + 10 * 1_000_000;

        let rocks_result = rocks
            .read_range("cpu", &Tags::new(), base_ts, end_ts)
            .unwrap();
        let arrow_result = arrow
            .read_range("cpu", &Tags::new(), base_ts, end_ts)
            .unwrap();

        assert!(!rocks_result.is_empty());
        assert!(!arrow_result.is_empty());
    }

    #[test]
    fn test_dual_engine_read_range_arrow() {
        let rocks_dir = TempDir::new().unwrap();
        let arrow_dir = TempDir::new().unwrap();

        let rocks = open_rocksdb(&rocks_dir);
        let arrow = open_arrow(&arrow_dir);

        let dps = make_cpu_datapoints(50);
        rocks.write_batch(&dps).unwrap();
        arrow.write_batch(&dps).unwrap();
        arrow.flush().unwrap();

        let base_ts = ts_today();
        let end_ts = base_ts + 50 * 1_000_000;

        let rocks_batches = rocks
            .read_range_arrow("cpu", base_ts, end_ts, None)
            .unwrap();
        let arrow_batches = arrow
            .read_range_arrow("cpu", base_ts, end_ts, None)
            .unwrap();

        assert!(
            !rocks_batches.is_empty(),
            "RocksDB should return Arrow batches"
        );
        assert!(
            !arrow_batches.is_empty(),
            "Arrow should return Arrow batches"
        );

        let rocks_rows: usize = rocks_batches.iter().map(|b| b.num_rows()).sum();
        let arrow_rows: usize = arrow_batches.iter().map(|b| b.num_rows()).sum();

        assert!(rocks_rows > 0);
        assert!(arrow_rows > 0);
    }

    #[test]
    fn test_dual_engine_tag_filtering() {
        let rocks_dir = TempDir::new().unwrap();
        let arrow_dir = TempDir::new().unwrap();

        let rocks = open_rocksdb(&rocks_dir);
        let arrow = open_arrow(&arrow_dir);

        let dps = make_cpu_datapoints(50);
        rocks.write_batch(&dps).unwrap();
        arrow.write_batch(&dps).unwrap();
        arrow.flush().unwrap();

        let mut filter_tags = Tags::new();
        filter_tags.insert("host".to_string(), "host_000".to_string());
        filter_tags.insert("region".to_string(), "us-west".to_string());

        let base_ts = ts_today();
        let end_ts = base_ts + 50 * 1_000_000;

        let rocks_filtered = rocks
            .read_range("cpu", &filter_tags, base_ts, end_ts)
            .unwrap();
        let arrow_filtered = arrow
            .read_range("cpu", &filter_tags, base_ts, end_ts)
            .unwrap();

        assert!(
            !rocks_filtered.is_empty(),
            "RocksDB should return filtered data"
        );
        assert!(
            !arrow_filtered.is_empty(),
            "Arrow should return filtered data"
        );

        for dp in &rocks_filtered {
            assert_eq!(dp.tags.get("host").unwrap(), "host_000");
        }
        for dp in &arrow_filtered {
            assert_eq!(dp.tags.get("host").unwrap(), "host_000");
        }
    }

    #[test]
    fn test_dual_engine_empty_read() {
        let rocks_dir = TempDir::new().unwrap();
        let arrow_dir = TempDir::new().unwrap();

        let rocks = open_rocksdb(&rocks_dir);
        let arrow = open_arrow(&arrow_dir);

        let now = chrono::Utc::now().timestamp_micros();
        let start = now - 86_400_000_000i64;
        let end = now + 86_400_000_000i64;

        let rocks_result = rocks.read_range("nonexistent", &Tags::new(), start, end);
        let arrow_result = arrow.read_range("nonexistent", &Tags::new(), start, end);

        if let Ok(ref result) = rocks_result {
            assert!(result.is_empty());
        }
        if let Ok(ref result) = arrow_result {
            assert!(result.is_empty());
        }
    }

    #[test]
    fn test_dual_engine_nonexistent_schema() {
        let rocks_dir = TempDir::new().unwrap();
        let arrow_dir = TempDir::new().unwrap();

        let rocks = open_rocksdb(&rocks_dir);
        let arrow = open_arrow(&arrow_dir);

        assert!(rocks.measurement_schema("nonexistent").is_none());
        assert!(arrow.measurement_schema("nonexistent").is_none());
    }
}
