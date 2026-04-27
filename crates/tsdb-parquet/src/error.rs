use thiserror::Error;

/// Parquet 存储层错误类型
#[derive(Error, Debug)]
pub enum TsdbParquetError {
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("wal error: {0}")]
    Wal(String),

    #[error("partition error: {0}")]
    Partition(String),

    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("tsdb-arrow error: {0}")]
    TsdbArrow(#[from] tsdb_arrow::error::TsdbArrowError),

    #[error("conversion error: {0}")]
    Conversion(String),
}

/// Parquet 存储层 Result 类型
pub type Result<T> = std::result::Result<T, TsdbParquetError>;
