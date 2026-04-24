use thiserror::Error;

#[derive(Error, Debug)]
pub enum IcebergError {
    #[error("catalog error: {0}")]
    Catalog(String),

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("commit conflict: concurrent modification detected")]
    CommitConflict,

    #[error("schema error: {0}")]
    Schema(String),

    #[error("manifest error: {0}")]
    Manifest(String),

    #[error("snapshot not found: {0}")]
    SnapshotNotFound(i64),

    #[error("partition spec error: {0}")]
    PartitionSpec(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("rocksdb error: {0}")]
    Rocksdb(#[from] rocksdb::Error),

    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("datafusion error: {0}")]
    Datafusion(#[from] datafusion::error::DataFusionError),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

impl From<tsdb_arrow::error::TsdbArrowError> for IcebergError {
    fn from(e: tsdb_arrow::error::TsdbArrowError) -> Self {
        match e {
            tsdb_arrow::error::TsdbArrowError::Schema(msg) => IcebergError::Schema(msg),
            tsdb_arrow::error::TsdbArrowError::Io(io_err) => IcebergError::Io(io_err),
            tsdb_arrow::error::TsdbArrowError::Arrow(arrow_err) => IcebergError::Arrow(arrow_err),
            tsdb_arrow::error::TsdbArrowError::Serde(json_err) => IcebergError::Json(json_err),
            other => IcebergError::Internal(other.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, IcebergError>;
