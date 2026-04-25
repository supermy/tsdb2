pub mod config_api;
pub mod error;
pub mod gateway;
pub mod lifecycle_api;
pub mod metrics_api;
pub mod parquet_api;
pub mod protocol;
pub mod server;
pub mod service_api;
pub mod sql_api;
pub mod test_api;

pub use error::{AdminError, Result};
pub use gateway::GatewayState;
pub use server::{AdminServer, BoundAdminServer};
