#[cfg(test)]
mod tests {
    use crate::TsdbFlightServer;
    use arrow_flight::flight_service_server::FlightService;
    use datafusion::execution::context::SessionContext;
    use futures::StreamExt;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tsdb_arrow::schema::{DataPoint, FieldValue};
    use tsdb_rocksdb::{RocksDbConfig, TsdbRocksDb};

    fn make_server_with_data(dir: &TempDir, datapoints: Vec<DataPoint>) -> TsdbFlightServer {
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        if !datapoints.is_empty() {
            db.write_batch(&datapoints).unwrap();
        }
        let ctx = SessionContext::new();
        let server = TsdbFlightServer::new(
            Arc::new(db) as Arc<dyn tsdb_arrow::engine::StorageEngine>,
            ctx,
            "127.0.0.1",
            50051,
        );

        if !datapoints.is_empty() {
            let provider = tsdb_datafusion::TsdbTableProvider::from_datapoints(
                "cpu",
                &datapoints,
                dir.path().to_path_buf(),
            )
            .unwrap();
            server
                .ctx
                .register_table("cpu", Arc::new(provider))
                .unwrap();
        }

        server
    }

    fn make_cpu_datapoints(n: usize) -> Vec<DataPoint> {
        (0..n)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", format!("server-{}", i % 3))
                    .with_tag("region", if i % 2 == 0 { "us-west" } else { "us-east" })
                    .with_field("usage", FieldValue::Float(10.0 + i as f64 * 0.5))
                    .with_field("count", FieldValue::Integer(100 + i as i64))
            })
            .collect()
    }

    #[test]
    fn test_server_creation() {
        let dir = TempDir::new().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let ctx = SessionContext::new();
        let server = TsdbFlightServer::new(
            Arc::new(db) as Arc<dyn tsdb_arrow::engine::StorageEngine>,
            ctx,
            "127.0.0.1",
            50051,
        );
        assert_eq!(server.host, "127.0.0.1");
        assert_eq!(server.port, 50051);
    }

    #[test]
    fn test_server_with_rocksdb() {
        let dir = TempDir::new().unwrap();
        let result = TsdbFlightServer::with_rocksdb(
            dir.path(),
            RocksDbConfig::default(),
            "127.0.0.1",
            50052,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_location_uri() {
        let dir = TempDir::new().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let ctx = SessionContext::new();
        let server = TsdbFlightServer::new(
            Arc::new(db) as Arc<dyn tsdb_arrow::engine::StorageEngine>,
            ctx,
            "localhost",
            8080,
        );
        assert_eq!(server.location_uri(), "grpc://localhost:8080");
    }

    #[tokio::test]
    async fn test_list_actions_returns_list_measurements() {
        let dir = TempDir::new().unwrap();
        let server = make_server_with_data(&dir, vec![]);

        let result = server
            .list_actions(tonic::Request::new(arrow_flight::Empty {}))
            .await;
        assert!(result.is_ok());

        let mut stream = result.unwrap().into_inner();
        let first: Option<_> = stream.next().await;
        assert!(first.is_some());

        let action = first.unwrap().unwrap();
        assert_eq!(action.r#type, "list_measurements");
        assert!(!action.description.is_empty());
    }

    #[tokio::test]
    async fn test_do_action_unknown_action_returns_unimplemented() {
        let dir = TempDir::new().unwrap();
        let server = make_server_with_data(&dir, vec![]);

        let action = arrow_flight::Action {
            r#type: "unknown_action".to_string(),
            body: Default::default(),
        };
        let result = server.do_action(tonic::Request::new(action)).await;
        match result {
            Err(status) => assert!(status.message().contains("not supported")),
            Ok(_) => panic!("expected error for unknown action"),
        }
    }

    #[tokio::test]
    async fn test_get_schema_invalid_descriptor() {
        let dir = TempDir::new().unwrap();
        let server = make_server_with_data(&dir, vec![]);

        let descriptor = arrow_flight::FlightDescriptor::new_path(vec![]);
        let result = server.get_schema(tonic::Request::new(descriptor)).await;
        assert!(
            result.is_err(),
            "expected error for empty flight descriptor path"
        );
    }

    #[tokio::test]
    async fn test_get_flight_info_invalid_descriptor() {
        let dir = TempDir::new().unwrap();
        let server = make_server_with_data(&dir, vec![]);

        let descriptor = arrow_flight::FlightDescriptor::new_path(vec![]);
        let result = server
            .get_flight_info(tonic::Request::new(descriptor))
            .await;
        assert!(
            result.is_err(),
            "expected error for empty flight descriptor path"
        );
    }

    #[tokio::test]
    async fn test_do_action_list_measurements_with_data() {
        let dir = TempDir::new().unwrap();
        let dps = make_cpu_datapoints(3);
        let server = make_server_with_data(&dir, dps);

        let action = arrow_flight::Action {
            r#type: "list_measurements".to_string(),
            body: Default::default(),
        };
        let result = server.do_action(tonic::Request::new(action)).await;
        assert!(result.is_ok());

        let mut stream = result.unwrap().into_inner();
        let first: Option<_> = stream.next().await;
        assert!(first.is_some());

        let body = first.unwrap().unwrap().body;
        let measurements: Vec<String> = serde_json::from_slice(&body).unwrap();
        assert!(measurements.contains(&"cpu".to_string()));
    }

    #[tokio::test]
    async fn test_get_flight_info_with_valid_sql() {
        let dir = TempDir::new().unwrap();
        let dps = make_cpu_datapoints(10);
        let server = make_server_with_data(&dir, dps);

        let descriptor =
            arrow_flight::FlightDescriptor::new_path(vec!["SELECT COUNT(*) FROM cpu".to_string()]);
        let result = server
            .get_flight_info(tonic::Request::new(descriptor))
            .await;
        assert!(
            result.is_ok(),
            "get_flight_info should succeed with valid SQL"
        );

        let info = result.unwrap().into_inner();
        assert_eq!(info.endpoint.len(), 1);
    }

    #[tokio::test]
    async fn test_get_flight_info_with_invalid_sql() {
        let dir = TempDir::new().unwrap();
        let server = make_server_with_data(&dir, vec![]);

        let descriptor = arrow_flight::FlightDescriptor::new_path(vec![
            "SELECT * FROM nonexistent_table".to_string(),
        ]);
        let result = server
            .get_flight_info(tonic::Request::new(descriptor))
            .await;
        assert!(
            result.is_err(),
            "get_flight_info should fail with invalid SQL"
        );
    }

    #[tokio::test]
    async fn test_do_get_with_sql_query() {
        let dir = TempDir::new().unwrap();
        let dps = make_cpu_datapoints(20);
        let server = make_server_with_data(&dir, dps);

        let ticket = arrow_flight::Ticket::new("SELECT COUNT(*) as cnt FROM cpu");
        let result = server.do_get(tonic::Request::new(ticket)).await;
        assert!(result.is_ok(), "do_get should succeed with valid SQL");

        let mut stream = result.unwrap().into_inner();
        let mut flight_data_count = 0;
        while let Some(item) = stream.next().await {
            assert!(item.is_ok(), "flight data should be valid");
            flight_data_count += 1;
        }
        assert!(flight_data_count > 0, "should return flight data");
    }

    #[tokio::test]
    async fn test_do_get_with_invalid_ticket() {
        let dir = TempDir::new().unwrap();
        let server = make_server_with_data(&dir, vec![]);

        let ticket = arrow_flight::Ticket::new("INVALID SQL !!!");
        let result = server.do_get(tonic::Request::new(ticket)).await;
        assert!(
            result.is_err(),
            "do_get should fail with invalid SQL ticket"
        );
    }

    #[tokio::test]
    async fn test_do_get_with_aggregation_query() {
        let dir = TempDir::new().unwrap();
        let dps = make_cpu_datapoints(30);
        let server = make_server_with_data(&dir, dps);

        let ticket = arrow_flight::Ticket::new("SELECT AVG(usage) as avg_usage FROM cpu");
        let result = server.do_get(tonic::Request::new(ticket)).await;
        assert!(result.is_ok(), "do_get aggregation should succeed");

        let mut stream = result.unwrap().into_inner();
        let mut has_data = false;
        while let Some(item) = stream.next().await {
            assert!(item.is_ok());
            has_data = true;
        }
        assert!(has_data);
    }

    #[tokio::test]
    async fn test_do_put_with_flight_data() {
        let dir = TempDir::new().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps = make_cpu_datapoints(5);
        db.write_batch(&dps).unwrap();

        let read_back = db.read_range("cpu", 0, 2_000_000_000).unwrap();
        assert_eq!(read_back.len(), 5);
    }

    #[tokio::test]
    async fn test_concurrent_do_get_queries() {
        let dir = TempDir::new().unwrap();
        let dps = make_cpu_datapoints(50);
        let server = make_server_with_data(&dir, dps);

        for _ in 0..4 {
            let ticket = arrow_flight::Ticket::new("SELECT COUNT(*) as cnt FROM cpu");
            let result = server.do_get(tonic::Request::new(ticket)).await;
            assert!(result.is_ok());
            let mut stream = result.unwrap().into_inner();
            let mut count = 0;
            while let Some(item) = stream.next().await {
                assert!(item.is_ok());
                count += 1;
            }
            assert!(count > 0);
        }
    }
}
