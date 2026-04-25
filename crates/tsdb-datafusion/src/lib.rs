//! # tsdb-datafusion — DataFusion 查询引擎集成层
//!
//! 基于 Apache DataFusion 为 TSDB 提供 SQL 查询能力,
//! 将时序数据以 Parquet 格式存储并通过 DataFusion 执行 SQL 查询。
//!
//! ## 模块结构
//! - `engine`: DataFusionQueryEngine 查询引擎主入口
//! - `error`: 错误类型定义
//! - `schema`: Measurement 与 Arrow Schema 的转换工具
//! - `table_provider`: TsdbTableProvider, 实现 DataFusion TableProvider trait
//! - `udf`: 自定义标量函数 (如 time_bucket)

pub mod engine;
pub mod error;
pub mod schema;
pub mod table_provider;
pub mod udf;

pub use engine::DataFusionQueryEngine;
pub use error::{Result, TsdbDatafusionError};
pub use table_provider::TsdbTableProvider;
