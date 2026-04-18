#[cfg(test)]
mod tests {
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

    #[allow(dead_code)]
    fn ts_days_ago(days: i64) -> i64 {
        ts_today() - days * 86_400_000_000
    }

    #[test]
    fn stress_restart_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let base_ts = ts_today();
        let tags = make_tags("server01");

        let count = 10_000usize;
        {
            let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
            let dps: Vec<tsdb_arrow::schema::DataPoint> = (0..count)
                .map(|i| tsdb_arrow::schema::DataPoint {
                    measurement: "cpu".to_string(),
                    tags: tags.clone(),
                    fields: make_fields(i),
                    timestamp: base_ts + i as i64 * 1_000,
                })
                .collect();
            db.write_batch(&dps).unwrap();
        }

        {
            let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
            let result = db
                .read_range("cpu", base_ts, base_ts + (count as i64 - 1) * 1_000)
                .unwrap();
            assert_eq!(result.len(), count, "all data should survive restart");
        }

        {
            let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
            let result = db
                .read_range("cpu", base_ts, base_ts + (count as i64 - 1) * 1_000)
                .unwrap();
            assert_eq!(result.len(), count, "data should survive multiple restarts");
        }
    }

    #[test]
    fn stress_restart_5_times_data_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let base_ts = ts_today();
        let tags = make_tags("server01");

        for round in 0..5 {
            let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
            let mut fields = tsdb_arrow::schema::Fields::new();
            fields.insert("round".to_string(), FieldValue::Integer(round));
            db.put("metrics", &tags, base_ts + round * 1_000_000, &fields)
                .unwrap();
        }

        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let result = db
            .read_range("metrics", base_ts, base_ts + 4_000_000)
            .unwrap();
        assert_eq!(
            result.len(),
            5,
            "all 5 rounds should be present after restarts"
        );
    }

    #[test]
    fn stress_compaction_then_read() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let tags = make_tags("server01");
        let base_ts = ts_today();

        for i in 0..5000usize {
            let fields = make_fields(i);
            db.put("cpu", &tags, base_ts + i as i64 * 1_000, &fields)
                .unwrap();
        }

        let cfs = db.list_ts_cfs();
        for cf_name in &cfs {
            db.compact_cf(cf_name).unwrap();
        }

        let result = db.read_range("cpu", base_ts, base_ts + 4_999_000).unwrap();
        assert_eq!(result.len(), 5000, "all data should survive compaction");
    }
}
