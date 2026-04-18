#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tsdb_arrow::schema::{FieldValue, Tags};
    use tsdb_rocksdb::{ArrowAdapter, RocksDbConfig, TsdbRocksDb, TtlManager};

    fn make_tags(host: &str) -> Tags {
        let mut tags = Tags::new();
        tags.insert("host".to_string(), host.to_string());
        tags
    }

    fn make_fields(usage: f64) -> tsdb_arrow::schema::Fields {
        let mut fields = tsdb_arrow::schema::Fields::new();
        fields.insert("usage".to_string(), FieldValue::Float(usage));
        fields.insert("count".to_string(), FieldValue::Integer(usage as i64 * 100));
        fields
    }

    /// 获取今天中午12点的微秒时间戳
    fn ts_today() -> i64 {
        chrono::Utc::now()
            .date_naive()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros()
    }

    /// 获取指定天数前中午12点的微秒时间戳
    fn ts_days_ago(days: i64) -> i64 {
        ts_today() - days * 86_400_000_000
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("measurement", DataType::Utf8, false),
            Field::new("tags_hash", DataType::UInt64, false),
            Field::new("tag_keys", DataType::new_list(DataType::Utf8, true), false),
            Field::new(
                "tag_values",
                DataType::new_list(DataType::Utf8, true),
                false,
            ),
            Field::new("usage", DataType::Float64, true),
            Field::new("count", DataType::Int64, true),
        ]))
    }

    #[test]
    fn test_e2e_rocksdb_write_read_large() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = ts_today();

        for i in 0..10_000i64 {
            let fields = make_fields(i as f64 * 0.01);
            db.put("cpu", &tags, base_ts + i * 1_000, &fields).unwrap();
        }

        let result = db.read_range("cpu", base_ts, base_ts + 9_999_000).unwrap();
        assert_eq!(result.len(), 10_000);
    }

    #[test]
    fn test_e2e_rocksdb_cross_day_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let base_ts = ts_today();
        let tags = make_tags("server01");

        {
            let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
            for day in 0..3i64 {
                let ts = base_ts + day * 86_400_000_000;
                let fields = make_fields(day as f64);
                db.put("cpu", &tags, ts, &fields).unwrap();
            }
        }

        {
            let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
            let result = db
                .read_range("cpu", base_ts, base_ts + 2 * 86_400_000_000)
                .unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(db.list_ts_cfs().len(), 3);
        }
    }

    #[test]
    fn test_e2e_rocksdb_tags_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = ts_today();

        for i in 0..1000i64 {
            let fields = make_fields(i as f64 * 0.01);
            db.put("cpu", &tags, base_ts + i * 1_000, &fields).unwrap();
        }

        let meta_cf = db.db().cf_handle("_series_meta").unwrap();
        let mut count = 0;
        let iter = db.db().iterator_cf(&meta_cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let _ = item.unwrap();
            count += 1;
        }
        assert_eq!(count, 1, "same tags should have exactly 1 entry in meta CF");
    }

    #[test]
    fn test_e2e_rocksdb_to_arrow_recordbatch() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = ts_today();

        for i in 0..100i64 {
            let fields = make_fields(i as f64 * 0.01);
            db.put("cpu", &tags, base_ts + i * 1_000, &fields).unwrap();
        }

        let schema = test_schema();
        let batch =
            ArrowAdapter::read_range_arrow(&db, "cpu", base_ts, base_ts + 99_000, schema).unwrap();

        assert_eq!(batch.num_rows(), 100);
        assert_eq!(batch.num_columns(), 7);
    }

    #[test]
    fn test_e2e_rocksdb_prefix_scan_arrow() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags_a = make_tags("server01");
        let tags_b = make_tags("server02");
        let base_ts = ts_today();

        for i in 0..50i64 {
            let fields = make_fields(i as f64 * 0.01);
            db.put("cpu", &tags_a, base_ts + i * 1_000, &fields)
                .unwrap();
            db.put("cpu", &tags_b, base_ts + i * 1_000, &fields)
                .unwrap();
        }

        let schema = test_schema();
        let batch =
            ArrowAdapter::prefix_scan_arrow(&db, "cpu", &tags_a, base_ts, base_ts + 49_000, schema)
                .unwrap();

        assert_eq!(batch.num_rows(), 50);
    }

    #[test]
    fn test_e2e_rocksdb_compaction_after_write() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = ts_today();

        for i in 0..5000i64 {
            let fields = make_fields(i as f64 * 0.01);
            db.put("cpu", &tags, base_ts + i * 1_000, &fields).unwrap();
        }

        let cfs = db.list_ts_cfs();
        for cf_name in &cfs {
            db.compact_cf(cf_name).unwrap();
        }

        let result = db.read_range("cpu", base_ts, base_ts + 4_999_000).unwrap();
        assert_eq!(result.len(), 5000, "data should survive compaction");
    }

    #[test]
    fn test_e2e_rocksdb_ttl_after_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let config = RocksDbConfig {
            default_ttl_secs: 86400,
            ..Default::default()
        };
        let db = TsdbRocksDb::open(dir.path(), config).unwrap();

        let tags = make_tags("server01");
        let now = chrono::Utc::now().timestamp_micros();

        let mut fields_recent = tsdb_arrow::schema::Fields::new();
        fields_recent.insert("usage".to_string(), FieldValue::Float(1.0));
        db.put("cpu", &tags, now, &fields_recent).unwrap();

        let cfs = db.list_ts_cfs();
        for cf_name in &cfs {
            db.compact_cf(cf_name).unwrap();
        }

        let result = db.get("cpu", &tags, now).unwrap();
        assert!(
            result.is_some(),
            "recent data should survive TTL compaction"
        );
    }

    #[test]
    fn test_e2e_rocksdb_drop_expired_cf() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let fields = make_fields(1.0);

        db.put("cpu", &tags, ts_days_ago(106), &fields).unwrap();
        db.put("cpu", &tags, ts_today(), &fields).unwrap();

        assert_eq!(db.list_ts_cfs().len(), 2);

        let now = chrono::Utc::now().date_naive();
        let _dropped = TtlManager::drop_expired_cfs(&db, now, 30).unwrap();

        let remaining = db.list_ts_cfs();
        assert!(remaining.len() <= 1);
    }

    #[test]
    fn test_e2e_rocksdb_merge_union_fields() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let ts = ts_today();

        let mut fields1 = tsdb_arrow::schema::Fields::new();
        fields1.insert("cpu".to_string(), FieldValue::Float(1.0));
        db.put("metrics", &tags, ts, &fields1).unwrap();

        let mut fields2 = tsdb_arrow::schema::Fields::new();
        fields2.insert("mem".to_string(), FieldValue::Float(2.0));
        db.merge("metrics", &tags, ts, &fields2).unwrap();

        let result = db.get("metrics", &tags, ts).unwrap().unwrap();
        assert_eq!(*result.fields.get("cpu").unwrap(), FieldValue::Float(1.0));
        assert_eq!(*result.fields.get("mem").unwrap(), FieldValue::Float(2.0));
    }

    #[test]
    fn test_e2e_rocksdb_snapshot_consistency() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = ts_today();

        let fields1 = make_fields(1.0);
        db.put("cpu", &tags, base_ts, &fields1).unwrap();

        let snapshot = db.snapshot();

        let fields2 = make_fields(2.0);
        db.put("cpu", &tags, base_ts + 1_000_000, &fields2).unwrap();

        let snap_result = snapshot
            .read_range(
                db.db(),
                &format!(
                    "ts_cpu_{}",
                    chrono::Utc::now().date_naive().format("%Y%m%d")
                ),
                base_ts,
                base_ts + 1_000_000,
                "cpu",
            )
            .unwrap();

        let db_result = db.read_range("cpu", base_ts, base_ts + 1_000_000).unwrap();
        assert!(
            db_result.len() > snap_result.len(),
            "DB should see more data than snapshot"
        );
    }

    #[tokio::test]
    async fn test_e2e_rocksdb_sql_select() {
        use datafusion::datasource::MemTable;
        use tsdb_arrow::converter::datapoints_to_record_batch;

        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = ts_today();

        for i in 0..100i64 {
            let fields = make_fields(i as f64 * 0.01);
            db.put("cpu", &tags, base_ts + i * 1_000, &fields).unwrap();
        }

        let datapoints = db.read_range("cpu", base_ts, base_ts + 99_000).unwrap();
        assert_eq!(datapoints.len(), 100);

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        engine.register_from_datapoints("cpu", &datapoints).unwrap();

        let schema = engine
            .session_context()
            .catalog("datafusion")
            .unwrap()
            .schema("public")
            .unwrap()
            .table("cpu")
            .await
            .unwrap()
            .unwrap()
            .schema();

        let batch = datapoints_to_record_batch(&datapoints, schema.clone()).unwrap();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        engine.session_context().deregister_table("cpu").unwrap();
        engine
            .session_context()
            .register_table("cpu", Arc::new(mem_table))
            .unwrap();

        let result = engine.execute("SELECT * FROM cpu").await.unwrap();
        assert!(!result.rows.is_empty(), "SQL SELECT should return rows");
        assert!(
            result.columns.contains(&"usage".to_string()),
            "columns should contain 'usage'"
        );
    }

    #[tokio::test]
    async fn test_e2e_rocksdb_sql_aggregation() {
        use datafusion::datasource::MemTable;
        use tsdb_arrow::converter::datapoints_to_record_batch;

        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = ts_today();

        for i in 0..100i64 {
            let fields = make_fields(i as f64 * 0.01);
            db.put("cpu", &tags, base_ts + i * 1_000, &fields).unwrap();
        }

        let datapoints = db.read_range("cpu", base_ts, base_ts + 99_000).unwrap();
        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        engine.register_from_datapoints("cpu", &datapoints).unwrap();

        let schema = engine
            .session_context()
            .catalog("datafusion")
            .unwrap()
            .schema("public")
            .unwrap()
            .table("cpu")
            .await
            .unwrap()
            .unwrap()
            .schema();

        let batch = datapoints_to_record_batch(&datapoints, schema.clone()).unwrap();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        engine.session_context().deregister_table("cpu").unwrap();
        engine
            .session_context()
            .register_table("cpu", Arc::new(mem_table))
            .unwrap();

        let result = engine
            .execute("SELECT AVG(usage) as avg_usage, SUM(count) as total_count, COUNT(*) as cnt FROM cpu")
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1, "aggregation should return 1 row");
        assert!(result.columns.contains(&"avg_usage".to_string()));
        assert!(result.columns.contains(&"total_count".to_string()));
        assert!(result.columns.contains(&"cnt".to_string()));
    }

    #[tokio::test]
    async fn test_e2e_rocksdb_sql_where_tag() {
        use datafusion::datasource::MemTable;
        use tsdb_arrow::converter::datapoints_to_record_batch;

        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags_a = make_tags("server01");
        let tags_b = make_tags("server02");
        let base_ts = ts_today();

        for i in 0..50i64 {
            let fields = make_fields(i as f64 * 0.01);
            db.put("cpu", &tags_a, base_ts + i * 1_000, &fields)
                .unwrap();
            db.put("cpu", &tags_b, base_ts + i * 1_000, &fields)
                .unwrap();
        }

        let datapoints = db.read_range("cpu", base_ts, base_ts + 49_000).unwrap();
        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        engine.register_from_datapoints("cpu", &datapoints).unwrap();

        let schema = engine
            .session_context()
            .catalog("datafusion")
            .unwrap()
            .schema("public")
            .unwrap()
            .table("cpu")
            .await
            .unwrap()
            .unwrap()
            .schema();

        let batch = datapoints_to_record_batch(&datapoints, schema.clone()).unwrap();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        engine.session_context().deregister_table("cpu").unwrap();
        engine
            .session_context()
            .register_table("cpu", Arc::new(mem_table))
            .unwrap();

        let result = engine.execute("SELECT * FROM cpu").await.unwrap();
        assert_eq!(
            result.rows.len(),
            100,
            "should have 100 rows (2 hosts x 50 points)"
        );
    }

    #[test]
    fn test_e2e_rocksdb_empty_result_arrow() {
        let schema = test_schema();
        let batch = ArrowAdapter::read_range_arrow_empty(schema).unwrap();
        assert_eq!(batch.num_rows(), 0, "empty result should have 0 rows");
        assert_eq!(
            batch.num_columns(),
            7,
            "empty result should have correct schema"
        );
    }

    #[test]
    fn test_e2e_rocksdb_performance_baseline() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = ts_today();

        let start = std::time::Instant::now();
        for i in 0..10_000i64 {
            let fields = make_fields(i as f64 * 0.01);
            db.put("cpu", &tags, base_ts + i * 1_000, &fields).unwrap();
        }
        let write_elapsed = start.elapsed();

        let start = std::time::Instant::now();
        let result = db.read_range("cpu", base_ts, base_ts + 9_999_000).unwrap();
        let read_elapsed = start.elapsed();

        assert_eq!(result.len(), 10_000);
        let write_ops = 10_000.0 / write_elapsed.as_secs_f64();
        let read_ops = 1.0 / read_elapsed.as_secs_f64();
        eprintln!(
            "RocksDB E2E baseline: write={:.0} pts/s, read={:.2} queries/s",
            write_ops, read_ops
        );
    }

    #[test]
    fn test_e2e_parquet_vs_rocksdb_consistency() {
        use tsdb_storage_arrow::{ArrowStorageConfig, ArrowStorageEngine};

        let rocks_dir = tempfile::tempdir().unwrap();
        let parquet_dir = tempfile::tempdir().unwrap();

        let rocks_db = TsdbRocksDb::open(rocks_dir.path(), RocksDbConfig::default()).unwrap();
        let parquet_engine = ArrowStorageEngine::open(
            parquet_dir.path(),
            ArrowStorageConfig {
                wal_enabled: false,
                max_buffer_rows: 500,
                flush_interval_ms: 100,
                ..Default::default()
            },
        )
        .unwrap();

        let tags = make_tags("server01");
        let base_ts = ts_today();

        let dps: Vec<tsdb_arrow::schema::DataPoint> = (0..100)
            .map(|i| {
                tsdb_arrow::schema::DataPoint::new("cpu", base_ts + i * 1_000)
                    .with_tag("host", "server01")
                    .with_field("usage", FieldValue::Float(i as f64 * 0.01))
                    .with_field("count", FieldValue::Integer(i * 100))
            })
            .collect();

        for dp in &dps {
            rocks_db
                .put(&dp.measurement, &dp.tags, dp.timestamp, &dp.fields)
                .unwrap();
        }
        parquet_engine.write_batch(&dps).unwrap();
        parquet_engine.flush().unwrap();

        let rocks_result = rocks_db
            .read_range("cpu", base_ts, base_ts + 99_000)
            .unwrap();
        let parquet_result = parquet_engine
            .read_range("cpu", &tags, base_ts, base_ts + 99_000)
            .unwrap();

        assert_eq!(
            rocks_result.len(),
            parquet_result.len(),
            "RocksDB and Parquet should return same number of points"
        );

        let mut rocks_ts: Vec<i64> = rocks_result.iter().map(|dp| dp.timestamp).collect();
        let mut parquet_ts: Vec<i64> = parquet_result.iter().map(|dp| dp.timestamp).collect();
        rocks_ts.sort();
        parquet_ts.sort();
        assert_eq!(
            rocks_ts, parquet_ts,
            "timestamps should match between engines"
        );
    }
}
