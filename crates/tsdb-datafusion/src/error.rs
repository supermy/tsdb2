use thiserror::Error;

/// DataFusion 查询引擎错误类型
#[derive(Error, Debug)]
pub enum TsdbDatafusionError {
    /// Arrow 格式错误
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// DataFusion 查询引擎错误
    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    /// Parquet 存储层错误
    #[error("parquet error: {0}")]
    Parquet(#[from] tsdb_parquet::error::TsdbParquetError),

    /// I/O 错误
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// 查询执行错误
    #[error("query error: {0}")]
    Query(String),

    /// Schema 相关错误
    #[error("schema error: {0}")]
    Schema(String),
}

/// DataFusion 查询引擎 Result 类型
pub type Result<T> = std::result::Result<T, TsdbDatafusionError>;
