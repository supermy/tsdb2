//! # tsdb-storage-arrow — Arrow 存储引擎 (整合层)
//!
//! 组合 Parquet 存储层、WAL、异步写入缓冲区和 DataFusion 查询引擎,
//! 提供完整的时序数据读写和 SQL 查询能力。
//!
//! ## 模块结构
//! - `buffer`: 同步/异步写入缓冲区
//! - `config`: 存储引擎配置
//! - `engine`: ArrowStorageEngine 主入口
//! - `error`: 错误类型定义

pub mod buffer;
pub mod config;
pub mod engine;
pub mod error;

pub use buffer::{AsyncWriteBuffer, WriteBuffer};
pub use config::ArrowStorageConfig;
pub use engine::ArrowStorageEngine;
pub use error::{Result, TsdbStorageArrowError};
