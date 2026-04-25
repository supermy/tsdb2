//! # tsdb-parquet — Parquet 存储引擎
//!
//! 提供基于 Parquet 格式的时序数据持久化, 包括:
//! - 日期分区管理
//! - WAL 预写日志
//! - 批量写入与自动刷盘
//! - 范围读取与标签过滤
//! - 冷热数据 Compaction
//!
//! ## 模块结构
//! - `compaction`: Compaction 配置与合并逻辑
//! - `encoding`: hot/cold Parquet Writer 属性
//! - `error`: 错误类型定义
//! - `partition`: 日期分区管理
//! - `reader`: Parquet 读取器
//! - `wal`: 预写日志 (WAL)
//! - `writer`: Parquet 写入器与缓冲区

pub mod compaction;
pub mod encoding;
pub mod error;
pub mod partition;
pub mod reader;
pub mod wal;
pub mod writer;

pub use compaction::{CompactionConfig, ParquetCompactor};
pub use encoding::{cold_writer_props, default_writer_props, hot_writer_props};
pub use error::{Result, TsdbParquetError};
pub use partition::{PartitionConfig, PartitionManager};
pub use reader::TsdbParquetReader;
pub use wal::{TsdbWAL, WALEntry, WALEntryType};
pub use writer::{TsdbParquetWriter, WriteBufferConfig};
