#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tsdb_arrow::schema::{DataPoint, FieldValue, Tags};
    use tsdb_storage_arrow::config::ArrowStorageConfig;
    use tsdb_storage_arrow::engine::ArrowStorageEngine;

    fn make_devops_datapoints(device_count: usize, hours: usize) -> Vec<DataPoint> {
        let mut dps = Vec::new();
        let base_ts = chrono::Utc::now().timestamp_micros();
        let interval = 10_000_000i64;

        for device in 0..device_count {
            let hostname = format!("host_{:04}", device);
            let region = if device % 2 == 0 {
                "us-west"
            } else {
                "us-east"
            };

            for minute in 0..(hours * 60) {
                let ts = base_ts + minute as i64 * interval;
                let cpu_usage = 0.3 + (device as f64 * 0.01) + (minute as f64 * 0.0001);
                let disk_io = (device * 100 + minute) as i64;

                dps.push(
                    DataPoint::new("cpu", ts)
                        .with_tag("host", &hostname)
                        .with_tag("region", region)
                        .with_field("usage", FieldValue::Float(cpu_usage.min(0.99)))
                        .with_field("idle", FieldValue::Float((1.0 - cpu_usage).max(0.01)))
                        .with_field("count", FieldValue::Integer(disk_io)),
                );
            }
        }

        dps
    }

    #[test]
    fn test_e2e_write_read_large() {
        let dir = TempDir::new().unwrap();
        let config = ArrowStorageConfig {
            wal_enabled: false,
            max_buffer_rows: 500,
            flush_interval_ms: 100,
            ..Default::default()
        };

        let engine = ArrowStorageEngine::open(dir.path(), config).unwrap();

        let dps = make_devops_datapoints(10, 1);
        assert!(!dps.is_empty());

        engine.write_batch(&dps).unwrap();
        engine.flush().unwrap();

        let tags = Tags::new();
        let base_ts = chrono::Utc::now().timestamp_micros();
        let result = engine
            .read_range("cpu", &tags, base_ts, base_ts + 3_600_000_000)
            .unwrap();

        assert!(!result.is_empty());
        assert!(result.len() > 100);
    }

    #[test]
    fn test_e2e_tag_filtering() {
        let dir = TempDir::new().unwrap();
        let config = ArrowStorageConfig {
            wal_enabled: false,
            max_buffer_rows: 500,
            flush_interval_ms: 100,
            ..Default::default()
        };

        let engine = ArrowStorageEngine::open(dir.path(), config).unwrap();

        let dps = make_devops_datapoints(5, 1);
        engine.write_batch(&dps).unwrap();
        engine.flush().unwrap();

        let mut tags = Tags::new();
        tags.insert("host".to_string(), "host_0000".to_string());

        let base_ts = chrono::Utc::now().timestamp_micros();
        let result = engine
            .read_range("cpu", &tags, base_ts, base_ts + 3_600_000_000)
            .unwrap();

        assert!(!result.is_empty());
        for dp in &result {
            assert_eq!(dp.tags.get("host").unwrap(), "host_0000");
        }
    }

    #[test]
    fn test_e2e_wal_recovery() {
        let dir = TempDir::new().unwrap();
        let config = ArrowStorageConfig {
            wal_enabled: true,
            max_buffer_rows: 500,
            flush_interval_ms: 100,
            ..Default::default()
        };

        let engine = ArrowStorageEngine::open(dir.path(), config).unwrap();

        let dps: Vec<DataPoint> = (0..20)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_field("usage", FieldValue::Float(0.5))
            })
            .collect();

        engine.write_batch(&dps).unwrap();
        engine.flush().unwrap();

        let tags = Tags::new();
        let result = engine
            .read_range("cpu", &tags, 1_000_000, 21_000_000)
            .unwrap();
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn test_e2e_sql_aggregation() {
        let dir = TempDir::new().unwrap();
        let config = ArrowStorageConfig {
            wal_enabled: false,
            max_buffer_rows: 500,
            flush_interval_ms: 100,
            ..Default::default()
        };

        let engine = ArrowStorageEngine::open(dir.path(), config).unwrap();

        let dps = make_devops_datapoints(3, 1);
        engine.write_batch(&dps).unwrap();
        engine.flush().unwrap();

        let result = engine
            .execute_sql("SELECT AVG(usage) as avg_usage FROM cpu", "cpu", &dps)
            .await
            .unwrap();
        assert!(!result.rows.is_empty());
    }

    #[test]
    fn test_e2e_compaction() {
        let dir = TempDir::new().unwrap();
        let config = ArrowStorageConfig {
            wal_enabled: false,
            max_buffer_rows: 50,
            flush_interval_ms: 100,
            ..Default::default()
        };

        let engine = ArrowStorageEngine::open(dir.path(), config).unwrap();

        let dps = make_devops_datapoints(5, 1);
        engine.write_batch(&dps).unwrap();
        engine.flush().unwrap();

        engine.compact().unwrap();
    }

    #[test]
    fn test_e2e_mixed_field_types() {
        let dir = TempDir::new().unwrap();
        let config = ArrowStorageConfig {
            wal_enabled: false,
            max_buffer_rows: 100,
            flush_interval_ms: 100,
            ..Default::default()
        };

        let engine = ArrowStorageEngine::open(dir.path(), config).unwrap();

        let dps: Vec<DataPoint> = (0..30)
            .map(|i| {
                DataPoint::new("metrics", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_field("cpu", FieldValue::Float(0.5 + i as f64 * 0.01))
                    .with_field("requests", FieldValue::Integer(i as i64 * 100))
                    .with_field(
                        "status",
                        FieldValue::String(if i % 2 == 0 { "ok" } else { "error" }.to_string()),
                    )
                    .with_field("active", FieldValue::Boolean(i % 3 != 0))
            })
            .collect();

        engine.write_batch(&dps).unwrap();
        engine.flush().unwrap();

        let tags = Tags::new();
        let result = engine
            .read_range("metrics", &tags, 1_000_000, 31_000_000)
            .unwrap();

        assert!(!result.is_empty());
        for dp in &result {
            assert!(dp.fields.contains_key("cpu"));
            assert!(dp.fields.contains_key("requests"));
            assert!(dp.fields.contains_key("status"));
            assert!(dp.fields.contains_key("active"));
        }
    }
}
