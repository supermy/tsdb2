use thiserror::Error;

/// Parquet 存储层错误类型
#[derive(Error, Debug)]
pub enum TsdbParquetError {
    /// Parquet 格式错误
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Arrow 格式错误
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// I/O 错误
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// WAL 相关错误 (CRC 校验失败、截断等)
    #[error("wal error: {0}")]
    Wal(String),

    /// 分区管理错误 (目录创建失败等)
    #[error("partition error: {0}")]
    Partition(String),

    /// 序列化/反序列化错误
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    /// Arrow 数据层转换错误
    #[error("tsdb-arrow error: {0}")]
    TsdbArrow(#[from] tsdb_arrow::error::TsdbArrowError),
}

/// Parquet 存储层 Result 类型
pub type Result<T> = std::result::Result<T, TsdbParquetError>;
