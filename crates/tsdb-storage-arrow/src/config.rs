use serde::{Deserialize, Serialize};

/// Arrow 存储引擎配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrowStorageConfig {
    /// 热数据保留天数, 默认 7 天
    pub hot_days: u64,
    /// 数据总保留天数, 默认 30 天
    pub retention_days: u64,
    /// 缓冲区最大行数, 超过则自动刷盘, 默认 100000
    pub max_buffer_rows: usize,
    /// 定时刷盘间隔 (毫秒), 默认 5000
    pub flush_interval_ms: u64,
    /// 单文件最大行数, 默认 1000000
    pub max_rows_per_file: usize,
    /// 行组大小, 默认 1000000
    pub row_group_size: usize,
    /// 是否启用 WAL, 默认 true
    pub wal_enabled: bool,
    /// WAL 同步间隔 (毫秒), 默认 1000
    pub wal_sync_interval_ms: u64,
    /// 内存限制 (字节), 默认 512MB
    pub memory_limit: usize,
}

impl Default for ArrowStorageConfig {
    fn default() -> Self {
        Self {
            hot_days: 7,
            retention_days: 30,
            max_buffer_rows: 100_000,
            flush_interval_ms: 5000,
            max_rows_per_file: 1_000_000,
            row_group_size: 1_000_000,
            wal_enabled: true,
            wal_sync_interval_ms: 1000,
            memory_limit: 512 * 1024 * 1024,
        }
    }
}
