use thiserror::Error;

/// Arrow 数据层错误类型
#[derive(Error, Debug)]
pub enum TsdbArrowError {
    /// Arrow 库操作错误
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Schema 相关错误 (列缺失、类型不匹配等)
    #[error("schema error: {0}")]
    Schema(String),

    /// 数据转换错误 (类型不支持、downcast 失败等)
    #[error("conversion error: {0}")]
    Conversion(String),

    /// I/O 错误
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// 序列化/反序列化错误
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Arrow 数据层 Result 类型
pub type Result<T> = std::result::Result<T, TsdbArrowError>;
