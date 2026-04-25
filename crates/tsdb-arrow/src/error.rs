use thiserror::Error;

#[derive(Error, Debug)]
pub enum TsdbArrowError {
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("conversion error: {0}")]
    Conversion(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("unsupported operation: {0}")]
    UnsupportedOperation(String),

    #[error("storage error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, TsdbArrowError>;
