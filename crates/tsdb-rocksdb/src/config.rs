/// RocksDB 引擎配置
///
/// 控制缓存、写入缓冲、TTL 等核心参数。
/// 所有时长参数均为秒, 日期相关逻辑在运行时动态计算。
#[derive(Debug, Clone)]
pub struct RocksDbConfig {
    /// Block Cache 大小 (字节), 默认 512MB
    pub cache_size: usize,
    /// SST Block 大小 (字节), 默认 4KB
    pub block_size: usize,
    /// 全局 Write Buffer 大小 (字节), 默认 256MB
    pub write_buffer_size: usize,
    /// 内存超限时是否 stall 写入, 默认 false
    pub allow_stall: bool,
    /// 单个 CF 的 Write Buffer 大小 (字节), 默认 64MB
    pub cf_write_buffer_size: usize,
    /// CF 的 LSM Level 1 大小基数 (字节), 默认 256MB
    pub cf_max_bytes_for_level_base: u64,
    /// 默认数据保留时长 (秒), 默认 7 天 = 604800
    pub default_retention_secs: u64,
    /// 默认 TTL 过期时长 (秒), 默认 30 天 = 2592000
    pub default_ttl_secs: u64,
}

impl Default for RocksDbConfig {
    fn default() -> Self {
        Self {
            cache_size: 512 * 1024 * 1024,
            block_size: 4096,
            write_buffer_size: 256 * 1024 * 1024,
            allow_stall: false,
            cf_write_buffer_size: 64 * 1024 * 1024,
            cf_max_bytes_for_level_base: 256 * 1024 * 1024,
            default_retention_secs: 7 * 24 * 3600,
            default_ttl_secs: 30 * 24 * 3600,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_cache_size() {
        let config = RocksDbConfig::default();
        assert_eq!(config.cache_size, 512 * 1024 * 1024);
    }

    #[test]
    fn test_default_config_block_size() {
        let config = RocksDbConfig::default();
        assert_eq!(config.block_size, 4096);
    }

    #[test]
    fn test_default_config_write_buffer_sizes() {
        let config = RocksDbConfig::default();
        assert!(config.write_buffer_size > 0);
        assert!(config.cf_write_buffer_size > 0);
        assert!(config.write_buffer_size >= config.cf_write_buffer_size);
    }

    #[test]
    fn test_default_config_ttl_vs_retention() {
        let config = RocksDbConfig::default();
        assert!(config.default_ttl_secs >= config.default_retention_secs);
    }

    #[test]
    fn test_default_config_positive_values() {
        let config = RocksDbConfig::default();
        assert!(config.cache_size > 0);
        assert!(config.block_size > 0);
        assert!(config.write_buffer_size > 0);
        assert!(config.cf_write_buffer_size > 0);
        assert!(config.cf_max_bytes_for_level_base > 0);
        assert!(config.default_retention_secs > 0);
        assert!(config.default_ttl_secs > 0);
    }

    #[test]
    fn test_config_clone() {
        let config = RocksDbConfig::default();
        let cloned = config.clone();
        assert_eq!(config.cache_size, cloned.cache_size);
        assert_eq!(config.default_ttl_secs, cloned.default_ttl_secs);
    }

    #[test]
    fn test_config_custom_values() {
        let config = RocksDbConfig {
            cache_size: 1024,
            block_size: 8192,
            write_buffer_size: 2048,
            allow_stall: true,
            cf_write_buffer_size: 1024,
            cf_max_bytes_for_level_base: 4096,
            default_retention_secs: 3600,
            default_ttl_secs: 7200,
        };
        assert_eq!(config.cache_size, 1024);
        assert!(config.allow_stall);
    }
}
