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

    fn ts_days_ago(days: i64) -> i64 {
        ts_today() - days * 86_400_000_000
    }

    fn seed_db(count: usize) -> (tempfile::TempDir, TsdbRocksDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let base_ts = ts_today();

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

        db.write_batch(&dps).unwrap();
        (dir, db)
    }

    #[test]
    fn stress_rocksdb_range_scan_100k() {
        let (_dir, db) = seed_db(100_000);
        let base_ts = ts_today();

        let start = Instant::now();
        let result = db.read_range("cpu", base_ts, base_ts + 99_999_000).unwrap();
        let elapsed = start.elapsed();

        eprintln!(
            "range scan 100K: {:.2}s ({} points, {:.0} points/sec)",
            elapsed.as_secs_f64(),
            result.len(),
            result.len() as f64 / elapsed.as_secs_f64()
        );

        assert!(!result.is_empty());
    }

    #[test]
    fn stress_rocksdb_prefix_scan_100_series() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let base_ts = ts_today();

        for s in 0..100usize {
            let tags = make_tags(&format!("host_{:04}", s));
            for i in 0..1000usize {
                let fields = make_fields(i);
                db.put("cpu", &tags, base_ts + i as i64 * 1_000, &fields)
                    .unwrap();
            }
        }

        let target_tags = make_tags("host_0050");
        let start = Instant::now();
        let result = db
            .prefix_scan("cpu", &target_tags, base_ts, base_ts + 999_000)
            .unwrap();
        let elapsed = start.elapsed();

        eprintln!(
            "prefix scan 1 series from 100: {:.2}s ({} points)",
            elapsed.as_secs_f64(),
            result.len()
        );

        assert_eq!(result.len(), 1000);
    }

    #[test]
    fn stress_rocksdb_cross_day_scan_7days() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let tags = make_tags("server01");

        let base_ts = ts_days_ago(7);
        let one_day = 86_400_000_000i64;

        for day in 0..7i64 {
            for i in 0..1000i64 {
                let mut fields = tsdb_arrow::schema::Fields::new();
                fields.insert("usage".to_string(), FieldValue::Float(i as f64 * 0.001));
                db.put(
                    "cpu",
                    &tags,
                    base_ts + day * one_day + i * 60_000_000,
                    &fields,
                )
                .unwrap();
            }
        }

        let start = Instant::now();
        let result = db
            .read_range("cpu", base_ts, base_ts + 7 * one_day)
            .unwrap();
        let elapsed = start.elapsed();

        eprintln!(
            "cross-day scan 7 days: {:.2}s ({} points)",
            elapsed.as_secs_f64(),
            result.len()
        );

        assert_eq!(result.len(), 7000);
        assert!(db.list_ts_cfs().len() >= 7);
    }

    #[test]
    fn stress_rocksdb_batched_multi_get() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let base_ts = ts_today();
        let tags = make_tags("server01");

        for i in 0..10_000i64 {
            let fields = make_fields(i as usize);
            db.put("cpu", &tags, base_ts + i * 1_000, &fields).unwrap();
        }

        let start = Instant::now();
        let mut found = 0;
        for i in (0..10_000i64).step_by(10) {
            if db.get("cpu", &tags, base_ts + i * 1_000).unwrap().is_some() {
                found += 1;
            }
        }
        let elapsed = start.elapsed();

        let queries = 1000usize;
        let p99_threshold = std::time::Duration::from_millis(20);
        let avg_per_query = elapsed / queries as u32;

        eprintln!(
            "batched multi_get 1000 keys: {:.2}s (avg {:.2?}/query, found {})",
            elapsed.as_secs_f64(),
            avg_per_query,
            found
        );

        assert_eq!(found, queries);
        assert!(
            avg_per_query < p99_threshold,
            "avg query time {:?} exceeds P99 threshold {:?}",
            avg_per_query,
            p99_threshold
        );
    }

    #[test]
    fn stress_rocksdb_cross_day_scan_30days() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let tags = make_tags("server01");

        let base_ts = ts_days_ago(30);
        let one_day = 86_400_000_000i64;

        for day in 0..30i64 {
            for i in 0..100i64 {
                let mut fields = tsdb_arrow::schema::Fields::new();
                fields.insert("usage".to_string(), FieldValue::Float(i as f64 * 0.01));
                db.put(
                    "cpu",
                    &tags,
                    base_ts + day * one_day + i * 600_000_000,
                    &fields,
                )
                .unwrap();
            }
        }

        let start = Instant::now();
        let result = db
            .read_range("cpu", base_ts, base_ts + 30 * one_day)
            .unwrap();
        let elapsed = start.elapsed();

        eprintln!(
            "cross-day scan 30 days: {:.2}s ({} points, {:.0} pts/s)",
            elapsed.as_secs_f64(),
            result.len(),
            result.len() as f64 / elapsed.as_secs_f64()
        );

        assert_eq!(
            result.len(),
            3000,
            "should read all 3000 points across 30 days"
        );
    }
}
