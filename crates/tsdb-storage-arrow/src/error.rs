use thiserror::Error;

/// Arrow 存储引擎错误类型
#[derive(Error, Debug)]
pub enum TsdbStorageArrowError {
    /// Arrow 格式错误
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Parquet 存储层错误
    #[error("parquet error: {0}")]
    Parquet(#[from] tsdb_parquet::error::TsdbParquetError),

    /// DataFusion 查询引擎错误
    #[error("datafusion error: {0}")]
    DataFusion(#[from] tsdb_datafusion::error::TsdbDatafusionError),

    /// I/O 错误
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// 通用存储错误
    #[error("storage error: {0}")]
    Storage(String),

    /// 数据未找到
    #[error("not found: {0}")]
    NotFound(String),
}

/// Arrow 存储引擎 Result 类型
pub type Result<T> = std::result::Result<T, TsdbStorageArrowError>;
