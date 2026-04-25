use crate::key::TsdbKey;
use rocksdb::compaction_filter::CompactionFilter;
use rocksdb::compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory};
use rocksdb::CompactionDecision;
use std::ffi::{CStr, CString};

/// TTL 过期过滤器
///
/// 在 Compaction 过程中根据时间戳判断数据是否过期。
/// 过期判定: `timestamp < now - ttl` 的数据行将被移除。
///
/// 采用 Factory 模式创建，每次 Compaction 时获取当前时间，
/// 确保同一轮 Compaction 内使用一致的 "now" 基准。
pub struct TsdbTtlFilter {
    /// TTL 时长 (微秒)
    ttl_micros: i64,
    /// Compaction 开始时的当前时间 (微秒)
    now_micros: i64,
    /// 过滤器名称 (C 字符串, RocksDB 要求)
    name: CString,
}

impl TsdbTtlFilter {
    /// 创建 TTL 过滤器
    ///
    /// # 参数
    /// - `ttl_micros` - 生存时长 (微秒)
    /// - `now_micros` - 当前时间戳 (微秒), 用于计算过期截止点
    pub fn new(ttl_micros: i64, now_micros: i64) -> Self {
        Self {
            ttl_micros,
            now_micros,
            name: CString::new("tsdb_ttl_filter").unwrap(),
        }
    }
}

impl CompactionFilter for TsdbTtlFilter {
    /// 过滤单条 KV: 解码 key 中的时间戳, 与截止点比较
    ///
    /// - 时间戳 < cutoff → Remove (过期, 移除)
    /// - 时间戳 >= cutoff → Keep (未过期, 保留)
    /// - 解码失败 → Keep (安全降级, 不删除无法识别的数据)
    fn filter(&mut self, _level: u32, key: &[u8], _value: &[u8]) -> CompactionDecision {
        if let Ok(ts_key) = TsdbKey::decode(key) {
            let cutoff = self.now_micros - self.ttl_micros;
            if ts_key.timestamp < cutoff {
                return CompactionDecision::Remove;
            }
        }
        CompactionDecision::Keep
    }

    fn name(&self) -> &CStr {
        self.name.as_c_str()
    }
}

/// TTL 过滤器工厂
///
/// 为每个 CF 创建独立的 TTL 过滤器实例。
/// 每次 Compaction 触发时调用 `create`, 获取当前时间作为过滤基准。
pub struct TsdbTtlFilterFactory {
    /// TTL 时长 (微秒), 由秒级配置转换而来
    ttl_micros: i64,
    name: CString,
}

impl TsdbTtlFilterFactory {
    /// 创建工厂, 将秒级 TTL 转换为微秒
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            ttl_micros: ttl_secs as i64 * 1_000_000,
            name: CString::new("tsdb_ttl_filter_factory").unwrap(),
        }
    }
}

impl CompactionFilterFactory for TsdbTtlFilterFactory {
    type Filter = TsdbTtlFilter;

    /// 每次 Compaction 时创建新的过滤器, 捕获当前时间
    fn create(&mut self, _context: CompactionFilterContext) -> Self::Filter {
        let now_micros = chrono::Utc::now().timestamp_micros();
        TsdbTtlFilter::new(self.ttl_micros, now_micros)
    }

    fn name(&self) -> &CStr {
        self.name.as_c_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ttl_filter_keeps_recent_data() {
        let now = 1_000_000_000_000_000i64;
        let ttl_micros = 86_400_000_000i64;
        let mut filter = TsdbTtlFilter::new(ttl_micros, now);

        let key = TsdbKey::new(42, now - 1_000_000).encode();
        assert!(matches!(
            filter.filter(0, &key, &[]),
            CompactionDecision::Keep
        ));
    }

    #[test]
    fn test_ttl_filter_removes_expired_data() {
        let now = 1_000_000_000_000_000i64;
        let ttl_micros = 86_400_000_000i64;
        let mut filter = TsdbTtlFilter::new(ttl_micros, now);

        let key = TsdbKey::new(42, now - ttl_micros - 1).encode();
        assert!(matches!(
            filter.filter(0, &key, &[]),
            CompactionDecision::Remove
        ));
    }

    #[test]
    fn test_ttl_filter_boundary_exactly_at_cutoff() {
        let now = 1_000_000_000_000_000i64;
        let ttl_micros = 86_400_000_000i64;
        let mut filter = TsdbTtlFilter::new(ttl_micros, now);

        let cutoff = now - ttl_micros;
        let key = TsdbKey::new(42, cutoff).encode();
        assert!(matches!(
            filter.filter(0, &key, &[]),
            CompactionDecision::Keep
        ));

        let key_just_expired = TsdbKey::new(42, cutoff - 1).encode();
        assert!(matches!(
            filter.filter(0, &key_just_expired, &[]),
            CompactionDecision::Remove
        ));
    }

    #[test]
    fn test_ttl_filter_keeps_on_invalid_key() {
        let now = 1_000_000_000_000_000i64;
        let ttl_micros = 86_400_000_000i64;
        let mut filter = TsdbTtlFilter::new(ttl_micros, now);

        assert!(matches!(
            filter.filter(0, b"short", &[]),
            CompactionDecision::Keep
        ));
    }

    #[test]
    fn test_ttl_filter_factory_creates_filter() {
        let mut factory = TsdbTtlFilterFactory::new(86400);
        let context = CompactionFilterContext {
            is_full_compaction: true,
            is_manual_compaction: false,
        };
        let filter = factory.create(context);
        assert_eq!(filter.name().to_str().unwrap(), "tsdb_ttl_filter");
    }

    #[test]
    fn test_ttl_filter_factory_name() {
        let factory = TsdbTtlFilterFactory::new(86400);
        assert_eq!(factory.name().to_str().unwrap(), "tsdb_ttl_filter_factory");
    }
}
