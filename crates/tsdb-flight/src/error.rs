#[derive(Debug, thiserror::Error)]
pub enum TsdbFlightError {
    #[error("Flight error: {0}")]
    Flight(#[from] arrow_flight::error::FlightError),
    #[error("Tonic transport error: {0}")]
    Tonic(#[from] tonic::transport::Error),
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("RocksDB engine error: {0}")]
    RocksDb(#[from] tsdb_rocksdb::error::TsdbRocksDbError),
    #[error("DataFusion integration error: {0}")]
    DataFusionIntegration(#[from] tsdb_datafusion::error::TsdbDatafusionError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, TsdbFlightError>;
