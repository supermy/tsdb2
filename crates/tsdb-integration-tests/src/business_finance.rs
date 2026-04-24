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

    fn generate_market_data(symbols: &[&str], ticks_per_symbol: usize) -> Vec<DataPoint> {
        let base_ts = ts_now() - ticks_per_symbol as i64 * 1_000_000;
        let mut dps = Vec::new();

        for (sym_idx, symbol) in symbols.iter().enumerate() {
            let base_price = 100.0 + sym_idx as f64 * 50.0;

            for tick in 0..ticks_per_symbol {
                let ts = base_ts + tick as i64 * 1_000_000;
                let price_delta = ((sym_idx * 31 + tick * 17) % 200) as f64 * 0.01 - 1.0;
                let open = base_price + price_delta;
                let high = open + ((tick % 5) as f64 * 0.05);
                let low = open - ((tick % 3) as f64 * 0.03);
                let close = (open + high + low) / 3.0;
                let volume = 1000 + (tick * 100 + sym_idx * 500) as i64;
                let turnover = (close * volume as f64 * 100.0) as i64;

                let mut tags = Tags::new();
                tags.insert("symbol".to_string(), symbol.to_string());
                tags.insert(
                    "exchange".to_string(),
                    if sym_idx % 2 == 0 { "SSE" } else { "SZSE" }.to_string(),
                );

                let mut fields = tsdb_arrow::schema::Fields::new();
                fields.insert("open".to_string(), FieldValue::Float(open));
                fields.insert("high".to_string(), FieldValue::Float(high));
                fields.insert("low".to_string(), FieldValue::Float(low));
                fields.insert("close".to_string(), FieldValue::Float(close));
                fields.insert("volume".to_string(), FieldValue::Integer(volume));
                fields.insert("turnover".to_string(), FieldValue::Integer(turnover));

                dps.push(DataPoint {
                    measurement: "market_tick".to_string(),
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
        engine
            .register_from_datapoints(measurement, datapoints)
            .unwrap();

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
        engine
            .session_context()
            .deregister_table(measurement)
            .unwrap();
        engine
            .session_context()
            .register_table(measurement, Arc::new(mem_table))
            .unwrap();
    }

    #[test]
    fn test_finance_high_frequency_tick_write() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let symbols = vec!["600000", "000001", "600519", "000858", "601318"];
        let dps = generate_market_data(&symbols, 5000);
        assert_eq!(dps.len(), 5 * 5000);

        let start = std::time::Instant::now();
        db.write_batch(&dps).unwrap();
        let elapsed = start.elapsed();
        let qps = dps.len() as f64 / elapsed.as_secs_f64();
        eprintln!(
            "Finance tick write: {} points in {:.2}s ({:.0} pts/s)",
            dps.len(),
            elapsed.as_secs_f64(),
            qps
        );

        assert!(qps > 1000.0);
    }

    #[test]
    fn test_finance_latest_tick_query() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let symbols = vec!["600000", "000001"];
        let dps = generate_market_data(&symbols, 100);
        db.write_batch(&dps).unwrap();

        let base_ts = ts_now() - 100 * 1_000_000;
        let result = db.read_range("market_tick", base_ts, ts_now()).unwrap();
        assert!(!result.is_empty());

        let latest_per_symbol: std::collections::HashMap<String, i64> = result
            .iter()
            .filter_map(|dp| dp.tags.get("symbol").map(|s| (s.clone(), dp.timestamp)))
            .fold(std::collections::HashMap::new(), |mut acc, (sym, ts)| {
                let entry = acc.entry(sym).or_insert(i64::MIN);
                *entry = (*entry).max(ts);
                acc
            });

        assert_eq!(latest_per_symbol.len(), 2);
        for latest_ts in latest_per_symbol.values() {
            assert!(*latest_ts > base_ts);
        }
    }

    #[tokio::test]
    async fn test_finance_sql_ohlc_aggregation() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let symbols = vec!["600000", "000001", "600519"];
        let dps = generate_market_data(&symbols, 200);
        db.write_batch(&dps).unwrap();

        let base_ts = ts_now() - 200 * 1_000_000;
        let datapoints = db.read_range("market_tick", base_ts, ts_now()).unwrap();

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        register_table_for_sql(&engine, "market_tick", &datapoints).await;

        let result = engine
            .execute(
                "SELECT tag_symbol, AVG(close) as avg_close, MAX(high) as max_high, MIN(low) as min_low, SUM(volume) as total_volume FROM market_tick GROUP BY tag_symbol",
            )
            .await
            .unwrap();

        assert!(!result.rows.is_empty());
        assert!(result.columns.contains(&"avg_close".to_string()));
        assert!(result.columns.contains(&"max_high".to_string()));
        assert!(result.columns.contains(&"min_low".to_string()));
        assert!(result.columns.contains(&"total_volume".to_string()));
    }

    #[tokio::test]
    async fn test_finance_sql_symbol_filter() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let symbols = vec!["600000", "000001", "600519"];
        let dps = generate_market_data(&symbols, 100);
        db.write_batch(&dps).unwrap();

        let base_ts = ts_now() - 100 * 1_000_000;
        let datapoints = db.read_range("market_tick", base_ts, ts_now()).unwrap();

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        register_table_for_sql(&engine, "market_tick", &datapoints).await;

        let result = engine
            .execute("SELECT * FROM market_tick WHERE tag_symbol = '600000'")
            .await
            .unwrap();

        assert!(!result.rows.is_empty());
    }

    #[test]
    fn test_finance_prefix_scan_by_symbol() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let symbols = vec!["600000", "000001"];
        let dps = generate_market_data(&symbols, 50);
        db.write_batch(&dps).unwrap();

        let mut target_tags = Tags::new();
        target_tags.insert("symbol".to_string(), "600000".to_string());
        target_tags.insert("exchange".to_string(), "SSE".to_string());

        let base_ts = ts_now() - 50 * 1_000_000;
        let result = db
            .prefix_scan("market_tick", &target_tags, base_ts, ts_now())
            .unwrap();
        assert!(!result.is_empty());

        for dp in &result {
            assert_eq!(dp.tags.get("symbol").unwrap(), "600000");
        }
    }

    #[test]
    fn test_finance_merge_tick_update() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let ts = ts_now();
        let mut tags = Tags::new();
        tags.insert("symbol".to_string(), "600000".to_string());

        let mut fields1 = tsdb_arrow::schema::Fields::new();
        fields1.insert("open".to_string(), FieldValue::Float(10.0));
        fields1.insert("high".to_string(), FieldValue::Float(10.5));
        db.put("market_tick", &tags, ts, &fields1).unwrap();

        let mut fields2 = tsdb_arrow::schema::Fields::new();
        fields2.insert("low".to_string(), FieldValue::Float(9.8));
        fields2.insert("close".to_string(), FieldValue::Float(10.2));
        db.merge("market_tick", &tags, ts, &fields2).unwrap();

        let result = db.get("market_tick", &tags, ts).unwrap().unwrap();
        assert_eq!(*result.fields.get("open").unwrap(), FieldValue::Float(10.0));
        assert_eq!(*result.fields.get("high").unwrap(), FieldValue::Float(10.5));
        assert_eq!(*result.fields.get("low").unwrap(), FieldValue::Float(9.8));
        assert_eq!(
            *result.fields.get("close").unwrap(),
            FieldValue::Float(10.2)
        );
    }

    #[test]
    fn test_finance_snapshot_consistency() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let symbols = vec!["600000"];
        let dps = generate_market_data(&symbols, 50);
        db.write_batch(&dps).unwrap();

        let _snapshot = db.snapshot();

        let more_dps = generate_market_data(&symbols, 20);
        db.write_batch(&more_dps).unwrap();

        let base_ts = ts_now() - 70 * 1_000_000;
        let snap_result = db.read_range("market_tick", base_ts, ts_now()).unwrap();

        assert!(!snap_result.is_empty());
    }

    #[tokio::test]
    async fn test_finance_dual_engine_consistency() {
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

        let symbols = vec!["600000", "000001"];
        let dps = generate_market_data(&symbols, 100);

        for dp in &dps {
            rocks_db
                .put(&dp.measurement, &dp.tags, dp.timestamp, &dp.fields)
                .unwrap();
        }
        parquet_engine.write_batch(&dps).unwrap();
        parquet_engine.flush().unwrap();

        let base_ts = ts_now() - 100 * 1_000_000;
        let rocks_result = rocks_db
            .read_range("market_tick", base_ts, ts_now())
            .unwrap();

        let empty_tags = Tags::new();
        let parquet_result = parquet_engine
            .read_range("market_tick", &empty_tags, base_ts, ts_now())
            .unwrap();

        assert_eq!(
            rocks_result.len(),
            parquet_result.len(),
            "RocksDB and Parquet should return same count"
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
