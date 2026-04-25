/// RocksDB 引擎配置
///
/// 控制缓存、写入缓冲、TTL、分层压缩等核心参数。
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
    /// 单个 CF 最大 Write Buffer 数量, 默认 4
    /// 增大可减少写入 stall, 但增加内存占用和读放大
    pub max_write_buffer_number: i32,
    /// CF 的 LSM Level 1 大小基数 (字节), 默认 256MB
    pub cf_max_bytes_for_level_base: u64,
    /// 默认数据保留时长 (秒), 默认 7 天 = 604800
    pub default_retention_secs: u64,
    /// 默认 TTL 过期时长 (秒), 默认 30 天 = 2592000
    pub default_ttl_secs: u64,
    /// L0-L2 压缩算法 (热点数据, 默认无压缩以降低写入延迟)
    pub compression_l0_l2: DBCompressionType,
    /// L3+ 压缩算法 (冷数据, 默认 Zstd 以最大化压缩比)
    pub compression_l3_plus: DBCompressionType,
    /// Bloom Filter 位数/Key, 默认 10 (约 1% 假阳性率)
    pub bloom_filter_bits_per_key: i32,
    /// 是否启用 Block-Based Bloom Filter (默认 false, 使用全过滤器)
    pub bloom_filter_block_based: bool,
    /// MemTable 前缀 Bloom Filter 比例, 默认 0.1
    pub memtable_prefix_bloom_ratio: f64,
    /// Level 0 触发 Compaction 的文件数, 默认 4
    pub level0_file_num_compaction_trigger: i32,
    /// Level 0 文件数慢写阈值, 默认 20
    pub level0_slowdown_writes_trigger: i32,
    /// Level 0 文件数停止写入阈值, 默认 36
    pub level0_stop_writes_trigger: i32,
    /// 是否为 Meta CF 启用优化 (默认 true)
    pub optimize_meta_cf: bool,
}

use rocksdb::DBCompressionType;

impl Default for RocksDbConfig {
    fn default() -> Self {
        Self {
            cache_size: 512 * 1024 * 1024,
            block_size: 4096,
            write_buffer_size: 256 * 1024 * 1024,
            allow_stall: false,
            cf_write_buffer_size: 64 * 1024 * 1024,
            max_write_buffer_number: 4,
            cf_max_bytes_for_level_base: 256 * 1024 * 1024,
            default_retention_secs: 7 * 24 * 3600,
            default_ttl_secs: 30 * 24 * 3600,
            compression_l0_l2: DBCompressionType::None,
            compression_l3_plus: DBCompressionType::Zstd,
            bloom_filter_bits_per_key: 10,
            bloom_filter_block_based: false,
            memtable_prefix_bloom_ratio: 0.1,
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            optimize_meta_cf: true,
        }
    }
}

impl RocksDbConfig {
    /// 从 INI 配置文件加载配置
    ///
    /// 支持的 section: [rocksdb], [rocksdb.compression], [rocksdb.bloom_filter], [rocksdb.level0]
    /// 未指定的字段使用默认值
    pub fn from_ini_file(path: impl AsRef<std::path::Path>) -> std::result::Result<Self, String> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|e| format!("failed to read config file: {}", e))?;
        Self::from_ini_str(&content)
    }

    /// 从 INI 格式字符串加载配置
    pub fn from_ini_str(content: &str) -> std::result::Result<Self, String> {
        let mut config = Self::default();
        let mut current_section = String::new();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if line.starts_with('[') && line.ends_with(']') {
                current_section = line[1..line.len() - 1].trim().to_string();
                continue;
            }
            if let Some((key, value)) = line.split_once('=') {
                let key = key.trim();
                let value = value.trim();
                match current_section.as_str() {
                    "rocksdb" => match key {
                        "cache_size" => {
                            config.cache_size =
                                value.parse().map_err(|e| format!("cache_size: {}", e))?
                        }
                        "block_size" => {
                            config.block_size =
                                value.parse().map_err(|e| format!("block_size: {}", e))?
                        }
                        "write_buffer_size" => {
                            config.write_buffer_size = value
                                .parse()
                                .map_err(|e| format!("write_buffer_size: {}", e))?
                        }
                        "allow_stall" => {
                            config.allow_stall =
                                value.parse().map_err(|e| format!("allow_stall: {}", e))?
                        }
                        "cf_write_buffer_size" => {
                            config.cf_write_buffer_size = value
                                .parse()
                                .map_err(|e| format!("cf_write_buffer_size: {}", e))?
                        }
                        "max_write_buffer_number" => {
                            config.max_write_buffer_number = value
                                .parse()
                                .map_err(|e| format!("max_write_buffer_number: {}", e))?
                        }
                        "cf_max_bytes_for_level_base" => {
                            config.cf_max_bytes_for_level_base = value
                                .parse()
                                .map_err(|e| format!("cf_max_bytes_for_level_base: {}", e))?
                        }
                        "default_retention_secs" => {
                            config.default_retention_secs = value
                                .parse()
                                .map_err(|e| format!("default_retention_secs: {}", e))?
                        }
                        "default_ttl_secs" => {
                            config.default_ttl_secs = value
                                .parse()
                                .map_err(|e| format!("default_ttl_secs: {}", e))?
                        }
                        _ => {}
                    },
                    "rocksdb.compression" => match key {
                        "l0_l2" => config.compression_l0_l2 = parse_compression(value)?,
                        "l3_plus" => config.compression_l3_plus = parse_compression(value)?,
                        _ => {}
                    },
                    "rocksdb.bloom_filter" => match key {
                        "bits_per_key" => {
                            config.bloom_filter_bits_per_key =
                                value.parse().map_err(|e| format!("bits_per_key: {}", e))?
                        }
                        "block_based" => {
                            config.bloom_filter_block_based =
                                value.parse().map_err(|e| format!("block_based: {}", e))?
                        }
                        "memtable_prefix_bloom_ratio" => {
                            config.memtable_prefix_bloom_ratio = value
                                .parse()
                                .map_err(|e| format!("memtable_prefix_bloom_ratio: {}", e))?
                        }
                        _ => {}
                    },
                    "rocksdb.level0" => match key {
                        "compaction_trigger" => {
                            config.level0_file_num_compaction_trigger = value
                                .parse()
                                .map_err(|e| format!("compaction_trigger: {}", e))?
                        }
                        "slowdown_trigger" => {
                            config.level0_slowdown_writes_trigger = value
                                .parse()
                                .map_err(|e| format!("slowdown_trigger: {}", e))?
                        }
                        "stop_trigger" => {
                            config.level0_stop_writes_trigger =
                                value.parse().map_err(|e| format!("stop_trigger: {}", e))?
                        }
                        _ => {}
                    },
                    "rocksdb.meta_cf" if key == "optimize" => {
                        config.optimize_meta_cf =
                            value.parse().map_err(|e| format!("optimize: {}", e))?;
                    }
                    _ => {}
                }
            }
        }

        Ok(config)
    }
}

fn parse_compression(value: &str) -> std::result::Result<DBCompressionType, String> {
    match value.to_lowercase().as_str() {
        "none" => Ok(DBCompressionType::None),
        "snappy" => Ok(DBCompressionType::Snappy),
        "zlib" => Ok(DBCompressionType::Zlib),
        "bz2" | "bzip2" => Ok(DBCompressionType::Bz2),
        "lz4" => Ok(DBCompressionType::Lz4),
        "lz4hc" => Ok(DBCompressionType::Lz4hc),
        "zstd" => Ok(DBCompressionType::Zstd),
        other => Err(format!("unknown compression type: {}", other)),
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
            max_write_buffer_number: 8,
            cf_max_bytes_for_level_base: 4096,
            default_retention_secs: 3600,
            default_ttl_secs: 7200,
            compression_l0_l2: DBCompressionType::Snappy,
            compression_l3_plus: DBCompressionType::Zstd,
            bloom_filter_bits_per_key: 20,
            bloom_filter_block_based: true,
            memtable_prefix_bloom_ratio: 0.15,
            level0_file_num_compaction_trigger: 8,
            level0_slowdown_writes_trigger: 40,
            level0_stop_writes_trigger: 60,
            optimize_meta_cf: false,
        };
        assert_eq!(config.cache_size, 1024);
        assert!(config.allow_stall);
        assert_eq!(config.max_write_buffer_number, 8);
        assert_eq!(config.bloom_filter_bits_per_key, 20);
    }

    #[test]
    fn test_tiered_compression_defaults() {
        let config = RocksDbConfig::default();
        assert_eq!(config.compression_l0_l2, DBCompressionType::None);
        assert_eq!(config.compression_l3_plus, DBCompressionType::Zstd);
    }

    #[test]
    fn test_bloom_filter_defaults() {
        let config = RocksDbConfig::default();
        assert_eq!(config.bloom_filter_bits_per_key, 10);
        assert!(!config.bloom_filter_block_based);
        assert!((config.memtable_prefix_bloom_ratio - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn test_level0_trigger_defaults() {
        let config = RocksDbConfig::default();
        assert_eq!(config.level0_file_num_compaction_trigger, 4);
        assert_eq!(config.level0_slowdown_writes_trigger, 20);
        assert_eq!(config.level0_stop_writes_trigger, 36);
    }

    #[test]
    fn test_from_ini_str_basic() {
        let ini = r#"
[rocksdb]
cache_size = 1073741824
cf_write_buffer_size = 67108864

[rocksdb.compression]
l0_l2 = snappy
l3_plus = zstd

[rocksdb.bloom_filter]
bits_per_key = 20
"#;
        let config = RocksDbConfig::from_ini_str(ini).unwrap();
        assert_eq!(config.cache_size, 1073741824);
        assert_eq!(config.cf_write_buffer_size, 67108864);
        assert_eq!(config.compression_l0_l2, DBCompressionType::Snappy);
        assert_eq!(config.compression_l3_plus, DBCompressionType::Zstd);
        assert_eq!(config.bloom_filter_bits_per_key, 20);
        assert_eq!(config.max_write_buffer_number, 4);
    }

    #[test]
    fn test_from_ini_str_write_heavy() {
        let ini = r#"
[rocksdb]
cache_size = 268435456
cf_write_buffer_size = 134217728
max_write_buffer_number = 6

[rocksdb.compression]
l0_l2 = none
l3_plus = zstd

[rocksdb.level0]
compaction_trigger = 8
slowdown_trigger = 40
stop_trigger = 60
"#;
        let config = RocksDbConfig::from_ini_str(ini).unwrap();
        assert_eq!(config.cf_write_buffer_size, 134217728);
        assert_eq!(config.max_write_buffer_number, 6);
        assert_eq!(config.compression_l0_l2, DBCompressionType::None);
        assert_eq!(config.level0_file_num_compaction_trigger, 8);
        assert_eq!(config.level0_slowdown_writes_trigger, 40);
    }

    #[test]
    fn test_from_ini_str_unknown_keys_ignored() {
        let ini = r#"
[storage]
engine = rocksdb

[rocksdb]
cache_size = 536870912
unknown_key = 42
"#;
        let config = RocksDbConfig::from_ini_str(ini).unwrap();
        assert_eq!(config.cache_size, 536870912);
    }

    #[test]
    fn test_from_ini_str_invalid_compression() {
        let ini = r#"
[rocksdb.compression]
l0_l2 = invalid
"#;
        let result = RocksDbConfig::from_ini_str(ini);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown compression type"));
    }

    #[test]
    fn test_from_ini_str_empty_uses_defaults() {
        let ini = "";
        let config = RocksDbConfig::from_ini_str(ini).unwrap();
        let default = RocksDbConfig::default();
        assert_eq!(config.cache_size, default.cache_size);
        assert_eq!(config.compression_l0_l2, default.compression_l0_l2);
    }

    #[test]
    fn test_from_ini_file_not_found() {
        let result = RocksDbConfig::from_ini_file("/nonexistent/path/config.ini");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_compression_all_types() {
        assert_eq!(parse_compression("none").unwrap(), DBCompressionType::None);
        assert_eq!(
            parse_compression("snappy").unwrap(),
            DBCompressionType::Snappy
        );
        assert_eq!(parse_compression("zstd").unwrap(), DBCompressionType::Zstd);
        assert_eq!(parse_compression("lz4").unwrap(), DBCompressionType::Lz4);
        assert_eq!(parse_compression("ZSTD").unwrap(), DBCompressionType::Zstd);
        assert_eq!(parse_compression("None").unwrap(), DBCompressionType::None);
    }
}
