//! # tsdb-rocksdb — 基于 RocksDB 的时序存储引擎
//!
//! 利用 RocksDB 的插件机制 (Comparator, MergeOperator, CompactionFilter,
//! SliceTransform) 实现时序数据专用存储, 消除 Parquet 的标签冗余问题.
//!
//! ## 架构
//! ```text
//! ┌─────────────────────────────────────────┐
//! │  _series_meta CF                        │  ← 标签元数据 (每个 series 只存一次)
//! │  Key: tags_hash (8B)                    │
//! │  Val: [num_tags:u16][tag_entries...]     │
//! ├─────────────────────────────────────────┤
//! │  ts_{measurement}_{date} CF             │  ← 纯数据 (无标签冗余)
//! │  Key: tags_hash(8B) + timestamp(8B)     │
//! │  Val: [num_fields:u16][field_entries...] │
//! └─────────────────────────────────────────┘
//! ```
//!
//! ## 模块关系
//! - `db` — 核心存储引擎 (TsdbRocksDb)
//! - `key` — 键编解码 (TsdbKey)
//! - `value` — 字段值编解码
//! - `tags` — 标签编解码
//! - `comparator` — 自定义比较器 (tags_hash + timestamp 排序)
//! - `merge` — MergeOperator (字段 union 合并)
//! - `compaction_filter` — CompactionFilter (TTL 过期过滤)
//! - `cleanup` — TTL 清理管理器 (三级策略)
//! - `snapshot` — 一致性快照读取
//! - `arrow_adapter` — Arrow RecordBatch 适配层
//! - `config` — 配置项

pub mod arrow_adapter;
pub mod cleanup;
pub mod compaction_filter;
pub mod comparator;
pub mod config;
pub mod cpu_features;
pub mod db;
pub mod error;
pub mod key;
pub mod merge;
pub mod snapshot;
pub mod tags;
pub mod value;

/// Arrow 适配器: DataPoint ↔ RecordBatch 转换
pub use arrow_adapter::ArrowAdapter;
/// TTL 清理管理器: 三级策略 (drop_cf / delete_range / CompactionFilter)
pub use cleanup::TtlManager;
/// CompactionFilter 工厂: TTL 过期数据自动过滤
pub use compaction_filter::TsdbTtlFilterFactory;
/// RocksDB 配置项
pub use config::RocksDbConfig;
/// 核心存储引擎
pub use db::TsdbRocksDb;
/// 错误类型与 Result 别名
pub use error::{Result, TsdbRocksDbError};
/// 键编解码与常量
pub use key::{TsdbKey, TAGS_HASH_SIZE, TIMESTAMP_SIZE};
