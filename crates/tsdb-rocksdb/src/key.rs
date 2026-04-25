/// 标签哈希占用的字节数 (u64 大端序)
pub const TAGS_HASH_SIZE: usize = 8;
/// 时间戳占用的字节数 (i64 大端序)
pub const TIMESTAMP_SIZE: usize = 8;
/// 完整键长度: 标签哈希(8) + 时间戳(8) = 16 字节
pub const KEY_SIZE: usize = TAGS_HASH_SIZE + TIMESTAMP_SIZE;

use crate::error::TsdbRocksDbError;

/// 时序数据键结构
///
/// 编码格式 (16 字节, 大端序):
/// ```text
/// [tags_hash: u64 BE][timestamp: i64 BE]
///  ← 8 bytes       →← 8 bytes       →
/// ```
///
/// 排序语义: 先按 tags_hash 排序 (同一 series 聚集),
/// 再按 timestamp 排序 (时间递增), 天然适配前缀扫描.
#[derive(Debug, Clone)]
pub struct TsdbKey {
    /// 标签集合的哈希值, 用于将同一 series 的数据聚集存储
    pub tags_hash: u64,
    /// 微秒级时间戳
    pub timestamp: i64,
}

impl TsdbKey {
    /// 创建新的时序键
    pub fn new(tags_hash: u64, timestamp: i64) -> Self {
        Self {
            tags_hash,
            timestamp,
        }
    }

    /// 编码为 16 字节大端序字节数组
    ///
    /// 保证: encode(a) < encode(b) 当且仅当 a.tags_hash < b.tags_hash
    ///       或 (a.tags_hash == b.tags_hash && a.timestamp < b.timestamp)
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(KEY_SIZE);
        buf.extend_from_slice(&self.tags_hash.to_be_bytes());
        buf.extend_from_slice(&self.timestamp.to_be_bytes());
        buf
    }

    /// 从字节数组解码键
    ///
    /// # 错误
    /// 当数据长度不足 KEY_SIZE (16 字节) 时返回 InvalidKey 错误
    pub fn decode(data: &[u8]) -> crate::error::Result<Self> {
        if data.len() < KEY_SIZE {
            return Err(TsdbRocksDbError::InvalidKey(format!(
                "key too short: expected {} bytes, got {}",
                KEY_SIZE,
                data.len()
            )));
        }
        let tags_hash = u64::from_be_bytes(
            data[..TAGS_HASH_SIZE]
                .try_into()
                .map_err(|_| TsdbRocksDbError::InvalidKey("tags_hash parse".into()))?,
        );
        let timestamp = i64::from_be_bytes(
            data[TAGS_HASH_SIZE..KEY_SIZE]
                .try_into()
                .map_err(|_| TsdbRocksDbError::InvalidKey("timestamp parse".into()))?,
        );
        Ok(Self {
            tags_hash,
            timestamp,
        })
    }

    /// 编码标签哈希前缀 (8 字节), 用于 SliceTransform 前缀提取
    pub fn prefix_encode(tags_hash: u64) -> Vec<u8> {
        tags_hash.to_be_bytes().to_vec()
    }
}

/// 计算标签集合的哈希值
///
/// 使用 FxHash 对排序后的 key-value 对依次哈希,
/// 保证: 1) 确定性 (相同输入必定相同输出)
///       2) 顺序无关 (插入顺序不影响哈希结果)
///       3) 高性能 (FxHash 比 SipHash 快约 2x)
pub fn compute_tags_hash(tags: &tsdb_arrow::schema::Tags) -> u64 {
    use std::collections::BTreeMap;
    use std::hash::{Hash, Hasher};

    let mut sorted: BTreeMap<&str, &str> = BTreeMap::new();
    for (k, v) in tags.iter() {
        sorted.insert(k.as_str(), v.as_str());
    }
    let mut hasher = fxhash::FxHasher::default();
    for (k, v) in &sorted {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_encode_decode_roundtrip() {
        let key = TsdbKey::new(12345678901234567890, 1700000000000000);
        let encoded = key.encode();
        assert_eq!(encoded.len(), KEY_SIZE);
        let decoded = TsdbKey::decode(&encoded).unwrap();
        assert_eq!(decoded.tags_hash, key.tags_hash);
        assert_eq!(decoded.timestamp, key.timestamp);
    }

    #[test]
    fn test_key_encode_min_max() {
        let min_key = TsdbKey::new(u64::MIN, i64::MIN);
        let max_key = TsdbKey::new(u64::MAX, i64::MAX);

        let min_encoded = min_key.encode();
        let max_encoded = max_key.encode();

        assert!(min_encoded < max_encoded);
        assert_eq!(TsdbKey::decode(&min_encoded).unwrap().tags_hash, u64::MIN);
        assert_eq!(TsdbKey::decode(&max_encoded).unwrap().timestamp, i64::MAX);
    }

    #[test]
    fn test_key_sorting_order() {
        let key_a = TsdbKey::new(100, 1000);
        let key_b = TsdbKey::new(100, 2000);
        let key_c = TsdbKey::new(200, 500);

        let enc_a = key_a.encode();
        let enc_b = key_b.encode();
        let enc_c = key_c.encode();

        assert!(enc_a < enc_b, "same hash, earlier ts should sort first");
        assert!(enc_b < enc_c, "larger hash should sort after");
    }

    #[test]
    fn test_key_prefix_encode() {
        let prefix = TsdbKey::prefix_encode(42);
        assert_eq!(prefix.len(), TAGS_HASH_SIZE);
        assert_eq!(prefix, 42u64.to_be_bytes().to_vec());
    }

    #[test]
    fn test_key_too_short() {
        let result = TsdbKey::decode(&[1, 2, 3]);
        assert!(result.is_err());
    }

    #[test]
    fn test_key_exact_size() {
        let key = TsdbKey::new(0, 0);
        assert_eq!(key.encode().len(), KEY_SIZE);
    }

    #[test]
    fn test_compute_tags_hash_deterministic() {
        let mut tags = tsdb_arrow::schema::Tags::new();
        tags.insert("host".to_string(), "server01".to_string());
        tags.insert("region".to_string(), "us-west".to_string());

        let hash1 = compute_tags_hash(&tags);
        let hash2 = compute_tags_hash(&tags);
        assert_eq!(hash1, hash2, "hash must be deterministic");
    }

    #[test]
    fn test_compute_tags_hash_different_tags_different_hash() {
        let mut tags_a = tsdb_arrow::schema::Tags::new();
        tags_a.insert("host".to_string(), "server01".to_string());

        let mut tags_b = tsdb_arrow::schema::Tags::new();
        tags_b.insert("host".to_string(), "server02".to_string());

        let hash_a = compute_tags_hash(&tags_a);
        let hash_b = compute_tags_hash(&tags_b);
        assert_ne!(
            hash_a, hash_b,
            "different tags should have different hashes"
        );
    }

    #[test]
    fn test_compute_tags_hash_order_independent() {
        let mut tags_a = tsdb_arrow::schema::Tags::new();
        tags_a.insert("a".to_string(), "1".to_string());
        tags_a.insert("b".to_string(), "2".to_string());

        let mut tags_b = tsdb_arrow::schema::Tags::new();
        tags_b.insert("b".to_string(), "2".to_string());
        tags_b.insert("a".to_string(), "1".to_string());

        let hash_a = compute_tags_hash(&tags_a);
        let hash_b = compute_tags_hash(&tags_b);
        assert_eq!(hash_a, hash_b, "tag insertion order should not affect hash");
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn proptest_key_encode_decode_roundtrip(tags_hash in any::<u64>(), timestamp in any::<i64>()) {
            let key = TsdbKey::new(tags_hash, timestamp);
            let encoded = key.encode();
            let decoded = TsdbKey::decode(&encoded).unwrap();
            prop_assert_eq!(decoded.tags_hash, tags_hash);
            prop_assert_eq!(decoded.timestamp, timestamp);
        }

        #[test]
        fn proptest_key_ordering_invariant(
            hash_a in any::<u64>(), ts_a in any::<i64>(),
            hash_b in any::<u64>(), ts_b in any::<i64>()
        ) {
            let key_a = TsdbKey::new(hash_a, ts_a);
            let key_b = TsdbKey::new(hash_b, ts_b);
            let enc_a = key_a.encode();
            let enc_b = key_b.encode();

            let expected_order = match hash_a.cmp(&hash_b) {
                std::cmp::Ordering::Equal => ts_a.cmp(&ts_b),
                ord => ord,
            };

            let actual_order = enc_a.cmp(&enc_b);
            prop_assert_eq!(actual_order, expected_order);
        }

        #[test]
        fn proptest_prefix_encode_is_prefix(tags_hash in any::<u64>(), timestamp in any::<i64>()) {
            let prefix = TsdbKey::prefix_encode(tags_hash);
            let full = TsdbKey::new(tags_hash, timestamp).encode();
            prop_assert!(full.starts_with(&prefix));
            prop_assert_eq!(prefix.len(), TAGS_HASH_SIZE);
        }

        #[test]
        fn proptest_key_size_constant(tags_hash in any::<u64>(), timestamp in any::<i64>()) {
            let key = TsdbKey::new(tags_hash, timestamp);
            prop_assert_eq!(key.encode().len(), KEY_SIZE);
        }
    }
}
