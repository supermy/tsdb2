#[cfg(test)]
mod tests {
    use std::time::Instant;
    use tsdb_arrow::schema::{FieldValue, Tags};
    use tsdb_rocksdb::{RocksDbConfig, TsdbRocksDb};

    fn make_tags(host: &str) -> Tags {
        let mut tags = Tags::new();
        tags.insert("host".to_string(), host.to_string());
        tags
    }

    fn make_fields(i: usize) -> tsdb_arrow::schema::Fields {
        let mut fields = tsdb_arrow::schema::Fields::new();
        fields.insert("usage".to_string(), FieldValue::Float(i as f64 * 0.01));
        fields.insert("idle".to_string(), FieldValue::Float(1.0 - i as f64 * 0.01));
        fields.insert("count".to_string(), FieldValue::Integer(i as i64 * 10));
        fields
    }

    fn ts_today() -> i64 {
        chrono::Utc::now()
            .date_naive()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros()
    }

    #[allow(dead_code)]
    fn ts_days_ago(days: i64) -> i64 {
        ts_today() - days * 86_400_000_000
    }

    #[test]
    fn stress_rocksdb_write_100k_single_thread() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let tags = make_tags("server01");
        let base_ts = ts_today();
        let count = 100_000usize;

        let start = Instant::now();
        for i in 0..count {
            let fields = make_fields(i);
            db.put("cpu", &tags, base_ts + i as i64 * 1_000, &fields)
                .unwrap();
        }
        let elapsed = start.elapsed();

        let pts_per_sec = count as f64 / elapsed.as_secs_f64();
        eprintln!(
            "write {} points: {:.2}s ({:.0} points/sec)",
            count,
            elapsed.as_secs_f64(),
            pts_per_sec
        );
        assert!(
            elapsed.as_secs() < 120,
            "write took too long: {:.2}s",
            elapsed.as_secs_f64()
        );
    }

    #[test]
    fn stress_rocksdb_write_batch_100k() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let base_ts = ts_today();
        let count = 100_000usize;

        let dps: Vec<tsdb_arrow::schema::DataPoint> = (0..count)
            .map(|i| {
                let mut tags = Tags::new();
                tags.insert("host".to_string(), format!("host_{:04}", i % 100));
                tsdb_arrow::schema::DataPoint {
                    measurement: "cpu".to_string(),
                    tags,
                    fields: make_fields(i),
                    timestamp: base_ts + i as i64 * 1_000,
                }
            })
            .collect();

        let start = Instant::now();
        db.write_batch(&dps).unwrap();
        let elapsed = start.elapsed();

        let pts_per_sec = count as f64 / elapsed.as_secs_f64();
        eprintln!(
            "batch write {} points: {:.2}s ({:.0} points/sec)",
            count,
            elapsed.as_secs_f64(),
            pts_per_sec
        );
        assert!(elapsed.as_secs() < 60, "batch write took too long");
    }

    #[test]
    fn stress_rocksdb_write_multi_measurement() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let tags = make_tags("server01");
        let base_ts = ts_today();
        let measurements = ["cpu", "memory", "disk", "network", "load"];
        let points_per_m = 10_000usize;

        let start = Instant::now();
        for m in &measurements {
            for i in 0..points_per_m {
                let fields = make_fields(i);
                db.put(m, &tags, base_ts + i as i64 * 1_000, &fields)
                    .unwrap();
            }
        }
        let elapsed = start.elapsed();

        let total = measurements.len() * points_per_m;
        eprintln!(
            "multi-measurement write {} points: {:.2}s ({:.0} points/sec)",
            total,
            elapsed.as_secs_f64(),
            total as f64 / elapsed.as_secs_f64()
        );
        let cfs = db.list_ts_cfs();
        assert_eq!(
            cfs.len(),
            measurements.len(),
            "each measurement should have its own CF"
        );
    }

    #[test]
    fn stress_rocksdb_write_batch_sizes() {
        let base_ts = ts_today();
        let batch_sizes = [1, 100, 1000, 10000];

        for &batch_size in &batch_sizes {
            let dir = tempfile::tempdir().unwrap();
            let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
            let total = 10_000usize;

            let dps: Vec<tsdb_arrow::schema::DataPoint> = (0..total)
                .map(|i| {
                    let mut tags = Tags::new();
                    tags.insert("host".to_string(), format!("host_{:04}", i % 100));
                    tsdb_arrow::schema::DataPoint {
                        measurement: format!("cpu_b{}", batch_size),
                        tags,
                        fields: make_fields(i),
                        timestamp: base_ts + i as i64 * 1_000,
                    }
                })
                .collect();

            let start = Instant::now();
            for chunk in dps.chunks(batch_size) {
                db.write_batch(chunk).unwrap();
            }
            let elapsed = start.elapsed();

            let pts_per_sec = total as f64 / elapsed.as_secs_f64();
            eprintln!(
                "batch_size={}: {:.2}s ({:.0} points/sec)",
                batch_size,
                elapsed.as_secs_f64(),
                pts_per_sec
            );
        }
    }
}
