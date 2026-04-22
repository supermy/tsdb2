#[cfg(test)]
mod tests {
    use crate::TsdbFlightServer;
    use arrow_flight::flight_service_server::FlightService;
    use datafusion::execution::context::SessionContext;
    use futures::StreamExt;
    use tempfile::TempDir;
    use tsdb_arrow::schema::{DataPoint, FieldValue};
    use tsdb_rocksdb::{RocksDbConfig, TsdbRocksDb};

    #[test]
    fn test_server_creation() {
        let dir = TempDir::new().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let ctx = SessionContext::new();
        let server = TsdbFlightServer::new(db, ctx, "127.0.0.1", 50051);
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
        let server = TsdbFlightServer::new(db, ctx, "localhost", 8080);
        assert_eq!(server.location_uri(), "grpc://localhost:8080");
    }

    #[tokio::test]
    async fn test_list_actions_returns_list_measurements() {
        let dir = TempDir::new().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let ctx = SessionContext::new();
        let server = TsdbFlightServer::new(db, ctx, "127.0.0.1", 50053);

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
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let ctx = SessionContext::new();
        let server = TsdbFlightServer::new(db, ctx, "127.0.0.1", 50054);

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
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let ctx = SessionContext::new();
        let server = TsdbFlightServer::new(db, ctx, "127.0.0.1", 50058);

        let descriptor = arrow_flight::FlightDescriptor::new_path(vec![]);
        let result = server.get_schema(tonic::Request::new(descriptor)).await;
        assert!(result.is_err(), "expected error for empty flight descriptor path");
    }

    #[tokio::test]
    async fn test_get_flight_info_invalid_descriptor() {
        let dir = TempDir::new().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        let ctx = SessionContext::new();
        let server = TsdbFlightServer::new(db, ctx, "127.0.0.1", 50059);

        let descriptor = arrow_flight::FlightDescriptor::new_path(vec![]);
        let result = server.get_flight_info(tonic::Request::new(descriptor)).await;
        assert!(result.is_err(), "expected error for empty flight descriptor path");
    }

    #[tokio::test]
    async fn test_do_action_list_measurements_with_data() {
        let dir = TempDir::new().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let dps: Vec<DataPoint> = (0..3)
            .map(|i| {
                DataPoint::new(
                    if i < 2 { "cpu" } else { "mem" },
                    1_000_000 + i as i64 * 1_000_000,
                )
                .with_tag("host", "server01")
                .with_field("usage", FieldValue::Float(0.5))
            })
            .collect();
        db.write_batch(&dps).unwrap();

        let ctx = SessionContext::new();
        let server = TsdbFlightServer::new(db, ctx, "127.0.0.1", 50061);

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
        assert!(measurements.contains(&"mem".to_string()));
        assert_eq!(measurements.len(), 2);
    }
}
