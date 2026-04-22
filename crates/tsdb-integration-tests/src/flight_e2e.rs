#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::execution::context::SessionContext;
    use futures::StreamExt;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tsdb_arrow::converter::datapoints_to_record_batch;
    use tsdb_arrow::schema::{DataPoint, FieldValue};
    use tsdb_datafusion::table_provider::TsdbTableProvider;
    use tsdb_flight::TsdbFlightServer;
    use tsdb_parquet::partition::{PartitionConfig, PartitionManager};
    use tsdb_parquet::reader::TsdbParquetReader;
    use tsdb_parquet::writer::{TsdbParquetWriter, WriteBufferConfig};
    use tsdb_rocksdb::{RocksDbConfig, TsdbRocksDb};

    fn make_test_datapoints() -> Vec<DataPoint> {
        (0..100)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", format!("host_{:02}", i % 5))
                    .with_tag("region", if i % 2 == 0 { "us-west" } else { "us-east" })
                    .with_field("usage", FieldValue::Float(0.3 + i as f64 * 0.005))
                    .with_field("idle", FieldValue::Float(0.7 - i as f64 * 0.005))
                    .with_field("count", FieldValue::Integer(i as i64 * 10))
            })
            .collect()
    }

    fn write_datapoints_to_parquet(dir: &std::path::Path, dps: &[DataPoint]) {
        let pm = PartitionManager::new(dir, PartitionConfig::default()).unwrap();
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default());
        writer.write_batch(dps).unwrap();
        writer.flush_all().unwrap();
    }

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("tag_host", DataType::Utf8, true),
            Field::new("tag_region", DataType::Utf8, true),
            Field::new("usage", DataType::Float64, true),
            Field::new("idle", DataType::Float64, true),
            Field::new("count", DataType::Int64, true),
        ]))
    }

    #[tokio::test]
    async fn test_flight_server_creation() {
        let dir = TempDir::new().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let ctx = SessionContext::new();
        let server = TsdbFlightServer::new(db, ctx, "127.0.0.1", 50051);
        assert!(std::mem::size_of_val(&server) > 0);
    }

    #[tokio::test]
    async fn test_flight_server_with_rocksdb() {
        let dir = TempDir::new().unwrap();
        let server = TsdbFlightServer::with_rocksdb(
            dir.path(),
            RocksDbConfig::default(),
            "127.0.0.1",
            50052,
        );
        assert!(server.is_ok());
    }

    #[tokio::test]
    async fn test_flight_list_actions() {
        use arrow_flight::flight_service_server::FlightService;
        use tonic::Request;

        let dir = TempDir::new().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let ctx = SessionContext::new();
        let server = TsdbFlightServer::new(db, ctx, "127.0.0.1", 50053);

        let result = server.list_actions(Request::new(arrow_flight::Empty {})).await;
        assert!(result.is_ok());

        let mut stream = result.unwrap().into_inner();
        let first = stream.next().await;
        assert!(first.is_some());

        let action = first.unwrap().unwrap();
        assert_eq!(action.r#type, "list_measurements");
    }

    #[tokio::test]
    async fn test_flight_do_get_sql_query() {
        use arrow_flight::flight_service_server::FlightService;
        use tonic::Request;

        let dir = TempDir::new().unwrap();
        let dps = make_test_datapoints();
        write_datapoints_to_parquet(dir.path(), &dps);

        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let ctx = SessionContext::new();

        let schema = make_schema();
        let provider = TsdbTableProvider::new("cpu", schema, dir.path());
        ctx.register_table("cpu", Arc::new(provider)).unwrap();

        let server = TsdbFlightServer::new(db, ctx, "127.0.0.1", 50054);

        let ticket = arrow_flight::Ticket::new("SELECT * FROM cpu LIMIT 10");
        let result = server.do_get(Request::new(ticket)).await;
        assert!(result.is_ok());

        let mut stream = result.unwrap().into_inner();
        let first = stream.next().await;
        assert!(first.is_some());
    }

    #[tokio::test]
    async fn test_flight_get_flight_info() {
        use arrow_flight::flight_service_server::FlightService;
        use tonic::Request;

        let dir = TempDir::new().unwrap();
        let dps = make_test_datapoints();
        write_datapoints_to_parquet(dir.path(), &dps);

        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let ctx = SessionContext::new();

        let schema = make_schema();
        let provider = TsdbTableProvider::new("cpu", schema, dir.path());
        ctx.register_table("cpu", Arc::new(provider)).unwrap();

        let server = TsdbFlightServer::new(db, ctx, "127.0.0.1", 50055);

        let descriptor = arrow_flight::FlightDescriptor::new_path(vec!["SELECT * FROM cpu".to_string()]);
        let result = server.get_flight_info(Request::new(descriptor)).await;
        assert!(result.is_ok());

        let info = result.unwrap().into_inner();
        assert!(!info.endpoint.is_empty());
    }

    #[tokio::test]
    async fn test_flight_get_schema() {
        use arrow_flight::flight_service_server::FlightService;
        use tonic::Request;

        let dir = TempDir::new().unwrap();
        let dps = make_test_datapoints();
        write_datapoints_to_parquet(dir.path(), &dps);

        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let ctx = SessionContext::new();

        let schema = make_schema();
        let provider = TsdbTableProvider::new("cpu", schema, dir.path());
        ctx.register_table("cpu", Arc::new(provider)).unwrap();

        let server = TsdbFlightServer::new(db, ctx, "127.0.0.1", 50056);

        let descriptor = arrow_flight::FlightDescriptor::new_path(vec!["SELECT * FROM cpu".to_string()]);
        let result = server.get_schema(Request::new(descriptor)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_flight_do_action_list_measurements() {
        use arrow_flight::flight_service_server::FlightService;
        use tonic::Request;

        let dir = TempDir::new().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps: Vec<DataPoint> = (0..10)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_field("usage", FieldValue::Float(0.5))
            })
            .collect();
        db.write_batch(&dps).unwrap();

        let ctx = SessionContext::new();
        let server = TsdbFlightServer::new(db, ctx, "127.0.0.1", 50057);

        let action = arrow_flight::Action {
            r#type: "list_measurements".to_string(),
            body: Default::default(),
        };
        let result = server.do_action(Request::new(action)).await;
        assert!(result.is_ok());

        let mut stream = result.unwrap().into_inner();
        let first = stream.next().await;
        assert!(first.is_some());

        let body = first.unwrap().unwrap().body;
        let measurements: Vec<String> = serde_json::from_slice(&body).unwrap();
        assert!(measurements.contains(&"cpu".to_string()));
    }

    #[tokio::test]
    async fn test_datafusion_sql_aggregation() {
        let dir = TempDir::new().unwrap();
        let dps = make_test_datapoints();
        write_datapoints_to_parquet(dir.path(), &dps);

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        engine.register_from_datapoints("cpu", &dps).unwrap();

        let result = engine
            .execute("SELECT AVG(usage) as avg_usage FROM cpu")
            .await
            .unwrap();
        assert!(!result.rows.is_empty());
        assert!(result.columns.contains(&"avg_usage".to_string()));
    }

    #[tokio::test]
    async fn test_datafusion_sql_filter() {
        let dir = TempDir::new().unwrap();
        let dps = make_test_datapoints();
        write_datapoints_to_parquet(dir.path(), &dps);

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        engine.register_from_datapoints("cpu", &dps).unwrap();

        let result = engine
            .execute("SELECT * FROM cpu WHERE tag_host = 'host_00'")
            .await
            .unwrap();
        assert!(!result.rows.is_empty());
    }

    #[tokio::test]
    async fn test_datafusion_sql_group_by() {
        let dir = TempDir::new().unwrap();
        let dps = make_test_datapoints();
        write_datapoints_to_parquet(dir.path(), &dps);

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        engine.register_from_datapoints("cpu", &dps).unwrap();

        let result = engine
            .execute("SELECT tag_host, AVG(usage) as avg_usage FROM cpu GROUP BY tag_host")
            .await
            .unwrap();
        assert!(!result.rows.is_empty());
    }

    #[tokio::test]
    async fn test_datafusion_sql_limit() {
        let dir = TempDir::new().unwrap();
        let dps = make_test_datapoints();
        write_datapoints_to_parquet(dir.path(), &dps);

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(dir.path());
        engine.register_from_datapoints("cpu", &dps).unwrap();

        let result = engine
            .execute("SELECT * FROM cpu LIMIT 5")
            .await
            .unwrap();
        assert!(result.rows.len() <= 5);
    }

    #[test]
    fn test_arrow_roundtrip() {
        let dps = make_test_datapoints();
        let schema = make_schema();

        let batch = datapoints_to_record_batch(&dps, schema.clone()).unwrap();
        assert_eq!(batch.num_rows(), 100);

        let restored = tsdb_arrow::converter::record_batch_to_datapoints(&batch).unwrap();
        assert_eq!(restored.len(), 100);

        assert_eq!(restored[0].timestamp, dps[0].timestamp);
        assert_eq!(restored[0].tags.get("host").unwrap(), dps[0].tags.get("host").unwrap());
    }

    #[test]
    fn test_arrow_projection() {
        let dps = make_test_datapoints();
        let schema = make_schema();

        let batch = datapoints_to_record_batch(&dps, schema).unwrap();

        let projected = batch.project(&[0, 3]).unwrap();
        assert_eq!(projected.num_columns(), 2);
        assert_eq!(projected.schema().field(0).name(), "timestamp");
        assert_eq!(projected.schema().field(1).name(), "usage");
    }

    #[test]
    fn test_parquet_write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let dps = make_test_datapoints();
        write_datapoints_to_parquet(dir.path(), &dps);

        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let reader = TsdbParquetReader::new(Arc::new(pm));
        let result = reader.read_all_datapoints("cpu").unwrap();

        assert!(!result.is_empty());
        assert_eq!(result.len(), dps.len());
    }

    #[test]
    fn test_parquet_range_read() {
        let dir = TempDir::new().unwrap();
        let dps = make_test_datapoints();
        write_datapoints_to_parquet(dir.path(), &dps);

        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let reader = TsdbParquetReader::new(Arc::new(pm));

        let tags = std::collections::BTreeMap::new();
        let result = reader.read_range("cpu", &tags, 1_000_000_000, 1_000_050_000_000).unwrap();

        assert!(!result.is_empty());
        for dp in &result {
            assert!(dp.timestamp >= 1_000_000_000);
            assert!(dp.timestamp <= 1_000_050_000_000);
        }
    }

    #[tokio::test]
    async fn test_rocksdb_write_and_query() {
        let dir = TempDir::new().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps: Vec<DataPoint> = (0..50)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", format!("host_{}", i % 3))
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.01))
            })
            .collect();

        db.write_batch(&dps).unwrap();

        let result = db.read_range("cpu", 1_000_000, 50_000_000).unwrap();
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn test_rocksdb_multi_get() {
        let dir = TempDir::new().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps: Vec<DataPoint> = (0..10)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_field("usage", FieldValue::Float(0.5))
            })
            .collect();

        db.write_batch(&dps).unwrap();

        let keys: Vec<(tsdb_arrow::schema::Tags, i64)> = dps
            .iter()
            .map(|dp| (dp.tags.clone(), dp.timestamp))
            .collect();

        let results = db.multi_get("cpu", &keys).unwrap();
        assert_eq!(results.len(), 10);
    }

    #[tokio::test]
    async fn test_full_pipeline_rocksdb_to_datafusion() {
        let dir = TempDir::new().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps: Vec<DataPoint> = (0..100)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", format!("host_{}", i % 5))
                    .with_field("usage", FieldValue::Float(0.3 + i as f64 * 0.005))
                    .with_field("count", FieldValue::Integer(i as i64 * 10))
            })
            .collect();

        db.write_batch(&dps).unwrap();

        let read_result = db.read_range("cpu", 1_000_000_000, 1_000_100_000_000).unwrap();
        assert!(!read_result.is_empty());

        let parquet_dir = TempDir::new().unwrap();
        write_datapoints_to_parquet(parquet_dir.path(), &read_result);

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(parquet_dir.path());
        engine.register_from_datapoints("cpu", &read_result).unwrap();

        let result = engine
            .execute("SELECT AVG(usage) as avg_usage FROM cpu")
            .await
            .unwrap();
        assert!(!result.rows.is_empty());
    }
}
