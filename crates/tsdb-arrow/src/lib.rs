//! # tsdb-arrow — 时序数据库 Arrow 数据模型与转换层
//!
//! 提供 DataPoint ↔ Arrow RecordBatch 的双向转换,
//! 以及时序 Schema 定义和内存池管理。
//!
//! ## 模块结构
//! - `converter`: DataPoint ↔ RecordBatch 转换、tags_hash 计算
//! - `schema`: 时序 Schema (扩展/紧凑模式)、DataPoint/FieldValue 定义
//! - `memory`: 线程安全的内存池 (CAS 无锁分配)
//! - `error`: 错误类型定义

pub mod converter;
pub mod error;
pub mod memory;
pub mod schema;

pub use converter::{datapoints_to_record_batch, record_batch_to_datapoints};
pub use error::{Result, TsdbArrowError};
pub use memory::TsdbMemoryPool;
pub use schema::{compact_tsdb_schema, tsdb_schema, TsdbSchemaBuilder};
