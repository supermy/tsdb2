#[cfg(test)]
mod tests {
    use tsdb_arrow::schema::{FieldValue, Tags};
    use tsdb_rocksdb::{RocksDbConfig, TsdbRocksDb, TtlManager};

    fn make_tags(host: &str) -> Tags {
        let mut tags = Tags::new();
        tags.insert("host".to_string(), host.to_string());
        tags
    }

    fn make_fields(i: usize) -> tsdb_arrow::schema::Fields {
        let mut fields = tsdb_arrow::schema::Fields::new();
        fields.insert("usage".to_string(), FieldValue::Float(i as f64 * 0.01));
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

    #[test]
    fn stress_ttl_filter_mixed_data() {
        let dir = tempfile::tempdir().unwrap();
        let config = RocksDbConfig {
            default_ttl_secs: 86400,
            ..Default::default()
        };
        let db = TsdbRocksDb::open(dir.path(), config).unwrap();

        let tags = make_tags("server01");
        let now = chrono::Utc::now().timestamp_micros();
        let one_day = 86_400_000_000i64;

        for i in 0..1000usize {
            let fields = make_fields(i);
            let ts = now - one_day * 2 + i as i64 * 172_800_000;
            db.put("cpu", &tags, ts, &fields).unwrap();
        }

        let cfs = db.list_ts_cfs();
        assert!(!cfs.is_empty());

        for cf_name in &cfs {
            db.compact_cf(cf_name).unwrap();
        }

        let result = db.read_range("cpu", 0, now + one_day).unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn stress_ttl_drop_expired_cfs() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let fields = make_fields(0);

        db.put("cpu", &tags, ts_days_ago(47), &fields).unwrap();
        db.put("cpu", &tags, ts_days_ago(33), &fields).unwrap();
        db.put("cpu", &tags, ts_today(), &fields).unwrap();

        assert_eq!(db.list_ts_cfs().len(), 3);

        let now = chrono::Utc::now().date_naive();
        let _dropped = TtlManager::drop_expired_cfs(&db, now, 30).unwrap();

        let remaining = db.list_ts_cfs();
        assert!(
            remaining.len() <= 2,
            "at least the oldest CF should be dropped"
        );
    }

    #[test]
    fn stress_ttl_high_cardinality_series() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let base_ts = ts_today();

        for s in 0..1000usize {
            let mut tags = Tags::new();
            tags.insert("host".to_string(), format!("host_{:06}", s));
            let fields = make_fields(s);
            db.put("cpu", &tags, base_ts + s as i64 * 1_000, &fields)
                .unwrap();
        }

        let result = db.read_range("cpu", base_ts, base_ts + 999_000).unwrap();
        assert_eq!(result.len(), 1000, "all 1000 series should be readable");
    }
}
