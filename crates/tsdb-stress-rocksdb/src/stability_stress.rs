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
        fields.insert("count".to_string(), FieldValue::Integer(i as i64));
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

    #[test]
    fn stress_long_running_continuous_write() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let base_ts = ts_today();
        let batch_size = 10_000usize;
        let num_batches = 30;

        let start = Instant::now();
        for batch in 0..num_batches {
            let dps: Vec<tsdb_arrow::schema::DataPoint> = (0..batch_size)
                .map(|i| {
                    let idx = batch * batch_size + i;
                    let mut tags = Tags::new();
                    tags.insert("host".to_string(), format!("host_{:04}", idx % 100));
                    tsdb_arrow::schema::DataPoint {
                        measurement: "cpu".to_string(),
                        tags,
                        fields: make_fields(idx),
                        timestamp: base_ts + idx as i64 * 1_000,
                    }
                })
                .collect();
            db.write_batch(&dps).unwrap();
        }
        let elapsed = start.elapsed();

        let total = batch_size * num_batches;
        eprintln!(
            "continuous write {} points in {:.2}s ({:.0} pts/s)",
            total,
            elapsed.as_secs_f64(),
            total as f64 / elapsed.as_secs_f64()
        );

        let result = db
            .read_range("cpu", base_ts, base_ts + (total as i64 - 1) * 1_000)
            .unwrap();
        assert_eq!(result.len(), total, "all data should be readable");
    }

    #[test]
    fn stress_cf_creation_stability() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let tags = make_tags("server01");
        let base_ts = ts_today();

        for day in 0..30i64 {
            let ts = base_ts + day * 86_400_000_000;
            let fields = make_fields(day as usize);
            db.put("cpu", &tags, ts, &fields).unwrap();
        }

        let cfs = db.list_ts_cfs();
        assert!(cfs.len() >= 30, "should have at least 30 CFs for 30 days");

        for cf_name in &cfs {
            db.compact_cf(cf_name).unwrap();
        }

        let result = db
            .read_range("cpu", base_ts, base_ts + 29 * 86_400_000_000)
            .unwrap();
        assert_eq!(
            result.len(),
            30,
            "all 30 days of data should survive compaction"
        );
    }
}
