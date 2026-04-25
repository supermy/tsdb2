use thiserror::Error;

#[derive(Error, Debug)]
pub enum AdminError {
    #[error("nng error: {0}")]
    Nng(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("config error: {0}")]
    Config(String),
    #[error("service not found: {0}")]
    ServiceNotFound(String),
    #[error("service already running: {0}")]
    ServiceAlreadyRunning(String),
    #[error("service not running: {0}")]
    ServiceNotRunning(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

pub type Result<T> = std::result::Result<T, AdminError>;
