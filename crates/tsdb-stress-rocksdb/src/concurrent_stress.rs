#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
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
    fn stress_concurrent_write_4_threads() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap());
        let base_ts = ts_today();

        let tags = make_tags("server01");
        let fields = make_fields(0);
        for t in 0..4 {
            let measurement = format!("metric_{}", t);
            db.put(&measurement, &tags, base_ts, &fields).unwrap();
        }

        let threads = 4;
        let points_per_thread = 5_000usize;
        let mut handles = Vec::new();

        for t in 0..threads {
            let db_clone = Arc::clone(&db);
            let handle = thread::spawn(move || {
                let measurement = format!("metric_{}", t);
                let tags = make_tags(&format!("thread_{}", t));
                for i in 0..points_per_thread {
                    let fields = make_fields(i);
                    db_clone
                        .put(&measurement, &tags, base_ts + i as i64 * 1_000, &fields)
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        let cfs = db.list_ts_cfs();
        assert_eq!(
            cfs.len(),
            threads,
            "each thread's measurement should have its own CF"
        );

        for t in 0..threads {
            let measurement = format!("metric_{}", t);
            let tags = make_tags(&format!("thread_{}", t));
            let result = db
                .prefix_scan(
                    &measurement,
                    &tags,
                    base_ts,
                    base_ts + (points_per_thread as i64 - 1) * 1_000,
                )
                .unwrap();
            assert_eq!(
                result.len(),
                points_per_thread,
                "thread {} data should be complete",
                t
            );
        }
    }

    #[test]
    fn stress_concurrent_merge_same_key() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap());
        let base_ts = ts_today();
        let tags = make_tags("server01");

        let mut fields0 = tsdb_arrow::schema::Fields::new();
        fields0.insert("cpu".to_string(), FieldValue::Float(1.0));
        db.put("metrics", &tags, base_ts, &fields0).unwrap();

        let threads = 4;
        let mut handles = Vec::new();

        for t in 0..threads {
            let db_clone = Arc::clone(&db);
            let handle = thread::spawn(move || {
                let mut fields = tsdb_arrow::schema::Fields::new();
                fields.insert(format!("field_{}", t), FieldValue::Float(t as f64));
                db_clone
                    .merge("metrics", &make_tags("server01"), base_ts, &fields)
                    .unwrap();
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        let result = db.get("metrics", &tags, base_ts).unwrap().unwrap();
        assert!(
            result.fields.contains_key("cpu"),
            "original field should be preserved"
        );
        for t in 0..threads {
            assert!(
                result.fields.contains_key(&format!("field_{}", t)),
                "merged field_{} should be present",
                t
            );
        }
    }

    #[test]
    fn stress_mixed_read_write() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap());
        let base_ts = ts_today();

        for i in 0..1000usize {
            let fields = make_fields(i);
            db.put(
                "cpu",
                &make_tags("writer"),
                base_ts + i as i64 * 1_000,
                &fields,
            )
            .unwrap();
        }

        let db_writer = Arc::clone(&db);
        let db_reader = Arc::clone(&db);

        let write_handle = thread::spawn(move || {
            for i in 1000..2000usize {
                let fields = make_fields(i);
                db_writer
                    .put(
                        "cpu",
                        &make_tags("writer"),
                        base_ts + i as i64 * 1_000,
                        &fields,
                    )
                    .unwrap();
            }
        });

        let read_handle = thread::spawn(move || {
            let result = db_reader
                .read_range("cpu", base_ts, base_ts + 1_999_000)
                .unwrap();
            assert!(!result.is_empty());
        });

        write_handle.join().unwrap();
        read_handle.join().unwrap();
    }

    #[test]
    fn stress_concurrent_write_8_threads() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap());
        let base_ts = ts_today();

        let threads = 8usize;
        let points_per_thread = 2_500usize;
        let total_expected = threads * points_per_thread;

        let mut handles = Vec::new();

        for t in 0..threads {
            let db_clone = Arc::clone(&db);
            let handle = thread::spawn(move || {
                let measurement = format!("metric_{}", t);
                let tags = make_tags(&format!("thread_{}", t));
                let dps: Vec<tsdb_arrow::schema::DataPoint> = (0..points_per_thread)
                    .map(|i| {
                        let mut fields = tsdb_arrow::schema::Fields::new();
                        fields.insert("usage".to_string(), FieldValue::Float(i as f64 * 0.01));
                        fields.insert("count".to_string(), FieldValue::Integer(i as i64));
                        tsdb_arrow::schema::DataPoint {
                            measurement: measurement.clone(),
                            tags: tags.clone(),
                            fields,
                            timestamp: base_ts + i as i64 * 1_000,
                        }
                    })
                    .collect();
                db_clone.write_batch(&dps).unwrap();
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        let mut total_written = 0;
        for t in 0..threads {
            let measurement = format!("metric_{}", t);
            let tags = make_tags(&format!("thread_{}", t));
            let result = db
                .prefix_scan(
                    &measurement,
                    &tags,
                    base_ts,
                    base_ts + (points_per_thread as i64 - 1) * 1_000,
                )
                .unwrap();
            total_written += result.len();
        }

        assert_eq!(
            total_written, total_expected,
            "total written should equal total expected, no data loss"
        );
    }

    #[test]
    fn stress_concurrent_snapshot_consistency() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap());
        let base_ts = ts_today();

        let tags = make_tags("server01");
        for i in 0..1000usize {
            let fields = make_fields(i);
            db.put("cpu", &tags, base_ts + i as i64 * 1_000, &fields)
                .unwrap();
        }

        let pre_snap_count = db
            .read_range("cpu", base_ts, base_ts + 999_000)
            .unwrap()
            .len();

        let _snapshot = db.snapshot();

        let db_writer = Arc::clone(&db);
        let write_handle = thread::spawn(move || {
            for i in 1000..2000usize {
                let fields = make_fields(i);
                db_writer
                    .put(
                        "cpu",
                        &make_tags("server01"),
                        base_ts + i as i64 * 1_000,
                        &fields,
                    )
                    .unwrap();
            }
        });

        write_handle.join().unwrap();

        assert_eq!(
            pre_snap_count, 1000,
            "pre-snapshot count should be exactly 1000"
        );

        let post_count = db
            .read_range("cpu", base_ts, base_ts + 1_999_000)
            .unwrap()
            .len();
        assert!(
            post_count >= 2000,
            "post-snapshot count should include all data"
        );
    }
}
