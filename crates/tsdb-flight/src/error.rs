#[derive(Debug, thiserror::Error)]
pub enum TsdbFlightError {
    #[error("Flight error: {0}")]
    Flight(#[from] Box<arrow_flight::error::FlightError>),
    #[error("Tonic transport error: {0}")]
    Tonic(#[from] tonic::transport::Error),
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] Box<datafusion::error::DataFusionError>),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("RocksDB engine error: {0}")]
    RocksDb(#[from] tsdb_rocksdb::error::TsdbRocksDbError),
    #[error("Storage engine error: {0}")]
    Storage(#[from] tsdb_arrow::error::TsdbArrowError),
    #[error("DataFusion integration error: {0}")]
    DataFusionIntegration(#[from] tsdb_datafusion::error::TsdbDatafusionError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Internal(String),
}

impl From<arrow_flight::error::FlightError> for TsdbFlightError {
    fn from(e: arrow_flight::error::FlightError) -> Self {
        TsdbFlightError::Flight(Box::new(e))
    }
}

impl From<datafusion::error::DataFusionError> for TsdbFlightError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        TsdbFlightError::DataFusion(Box::new(e))
    }
}

pub type Result<T> = std::result::Result<T, TsdbFlightError>;
