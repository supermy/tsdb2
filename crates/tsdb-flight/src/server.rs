use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, PollInfo,
    PutResult, SchemaAsIpc, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use datafusion::execution::context::SessionContext;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use tsdb_arrow::engine::StorageEngine;

pub struct TsdbFlightServer {
    pub(crate) ctx: SessionContext,
    engine: Arc<dyn StorageEngine>,
    pub(crate) host: String,
    pub(crate) port: u16,
}

impl TsdbFlightServer {
    pub fn new(
        engine: Arc<dyn StorageEngine>,
        ctx: SessionContext,
        host: impl Into<String>,
        port: u16,
    ) -> Self {
        let server = Self {
            ctx,
            engine,
            host: host.into(),
            port,
        };
        server.register_existing_measurements();
        server
    }

    pub fn with_rocksdb(
        data_dir: impl AsRef<std::path::Path>,
        config: tsdb_rocksdb::RocksDbConfig,
        host: impl Into<String>,
        port: u16,
    ) -> crate::error::Result<Self> {
        let db = tsdb_rocksdb::TsdbRocksDb::open(data_dir, config)?;
        let ctx = SessionContext::new();
        Ok(Self::new(Arc::new(db), ctx, host, port))
    }

    fn register_existing_measurements(&self) {
        let measurements = self.engine.list_measurements();
        for measurement in &measurements {
            if let Ok(()) = self.register_measurement_from_db(measurement) {
                tracing::info!("auto-registered measurement: {}", measurement);
            }
        }
    }

    fn register_measurement_from_db(&self, measurement: &str) -> crate::error::Result<()> {
        let schema = self.engine.measurement_schema(measurement);
        let schema = match schema {
            Some(s) => s,
            None => return Ok(()),
        };

        let provider =
            tsdb_datafusion::TsdbTableProvider::new(measurement, schema, std::path::PathBuf::new());

        self.ctx
            .register_table(measurement, Arc::new(provider))
            .map_err(|e| crate::error::TsdbFlightError::DataFusion(Box::new(e)))?;

        Ok(())
    }

    pub fn location_uri(&self) -> String {
        format!("grpc://{}:{}", self.host, self.port)
    }
}

#[tonic::async_trait]
impl FlightService for TsdbFlightServer {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = std::result::Result<HandshakeResponse, Status>> + Send>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not supported"))
    }

    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = std::result::Result<FlightInfo, Status>> + Send>>;

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not supported"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let sql = match &descriptor.path {
            path if !path.is_empty() => path[0].clone(),
            _ => {
                return Err(Status::invalid_argument(
                    "missing SQL in flight descriptor path",
                ))
            }
        };

        let plan = self
            .ctx
            .sql(&sql)
            .await
            .map_err(|e| Status::internal(format!("SQL planning failed: {}", e)))?;

        let schema = plan.schema();

        let ticket = Ticket::new(sql);

        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(format!("schema encode: {}", e)))?
            .with_endpoint(
                FlightEndpoint::new()
                    .with_ticket(ticket)
                    .with_location(self.location_uri()),
            )
            .with_descriptor(descriptor);

        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        let descriptor = request.into_inner().clone();
        let info = self
            .get_flight_info(Request::new(descriptor.clone()))
            .await?
            .into_inner();
        let poll_info = PollInfo::new().with_info(info).with_descriptor(descriptor);
        Ok(Response::new(poll_info))
    }

    type DoGetStream = Pin<Box<dyn Stream<Item = std::result::Result<FlightData, Status>> + Send>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let sql = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("invalid ticket: {}", e)))?;

        let df = self
            .ctx
            .sql(&sql)
            .await
            .map_err(|e| Status::internal(format!("SQL execution failed: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| Status::internal(format!("collect failed: {}", e)))?;

        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));

        let flight_data = arrow_flight::utils::batches_to_flight_data(&schema, batches)
            .map_err(|e| Status::internal(format!("encode flight data: {}", e)))?;

        let results: Vec<std::result::Result<FlightData, Status>> =
            flight_data.into_iter().map(Ok).collect();

        Ok(Response::new(Box::pin(futures::stream::iter(results))))
    }

    type DoPutStream = Pin<Box<dyn Stream<Item = std::result::Result<PutResult, Status>> + Send>>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();
        let mut all_flight_data: Vec<FlightData> = Vec::new();

        while let Some(flight_data) = stream.message().await? {
            all_flight_data.push(flight_data);
        }

        if !all_flight_data.is_empty() {
            let batches = arrow_flight::utils::flight_data_to_batches(&all_flight_data)
                .map_err(|e| Status::internal(format!("decode flight data: {}", e)))?;

            let mut datapoints = Vec::new();
            for batch in &batches {
                let dps = tsdb_arrow::converter::record_batch_to_datapoints(batch)
                    .map_err(|e| Status::internal(format!("convert: {}", e)))?;
                datapoints.extend(dps);

                if datapoints.len() >= 10000 {
                    self.engine
                        .write_batch(&datapoints)
                        .map_err(|e| Status::internal(format!("write: {}", e)))?;
                    datapoints.clear();
                }
            }

            if !datapoints.is_empty() {
                self.engine
                    .write_batch(&datapoints)
                    .map_err(|e| Status::internal(format!("write: {}", e)))?;
            }
        }

        let result = PutResult {
            app_metadata: b"ok".to_vec().into(),
        };
        Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(
            result,
        )]))))
    }

    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = std::result::Result<FlightData, Status>> + Send>>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported"))
    }

    type DoActionStream =
        Pin<Box<dyn Stream<Item = std::result::Result<arrow_flight::Result, Status>> + Send>>;

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            "list_measurements" => {
                let measurements = self.engine.list_measurements();
                let body = serde_json::to_vec(&measurements).unwrap_or_default();
                let result = arrow_flight::Result { body: body.into() };
                Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(
                    result,
                )]))))
            }
            _ => Err(Status::unimplemented(format!(
                "action '{}' not supported",
                action.r#type
            ))),
        }
    }

    type ListActionsStream =
        Pin<Box<dyn Stream<Item = std::result::Result<ActionType, Status>> + Send>>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![ActionType {
            r#type: "list_measurements".to_string(),
            description: "列出所有 measurement".to_string(),
        }];
        Ok(Response::new(Box::pin(futures::stream::iter(
            actions.into_iter().map(Ok),
        ))))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<arrow_flight::SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let sql = match &descriptor.path {
            path if !path.is_empty() => path[0].clone(),
            _ => {
                return Err(Status::invalid_argument(
                    "missing SQL in flight descriptor path",
                ))
            }
        };

        let plan = self
            .ctx
            .sql(&sql)
            .await
            .map_err(|e| Status::internal(format!("SQL planning failed: {}", e)))?;

        let schema = plan.schema();
        let options = IpcWriteOptions::default();
        let schema_ipc = SchemaAsIpc::new(schema.as_ref(), &options);
        let flight_data: FlightData = schema_ipc.into();

        let schema_result = arrow_flight::SchemaResult {
            schema: flight_data.data_header,
        };

        Ok(Response::new(schema_result))
    }
}
