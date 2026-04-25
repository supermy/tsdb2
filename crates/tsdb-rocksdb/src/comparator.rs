use crate::key::TAGS_HASH_SIZE;

/// 时序数据自定义比较器
///
/// 排序语义:
/// 1. 先按 `tags_hash` (前 8 字节, u64 大端序) 排序 → 同一 series 数据聚集
/// 2. 再按 `timestamp` (后 8 字节, i64 大端序) 排序 → 时间递增
///
/// 大端序编码保证字节序比较与数值比较一致, 无需额外转换。
/// 当键长度不足 TAGS_HASH_SIZE 时退化为字节序比较 (安全降级)。
pub fn tsdb_compare(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    if a.len() < TAGS_HASH_SIZE || b.len() < TAGS_HASH_SIZE {
        return a.cmp(b);
    }
    let a_hash = &a[..TAGS_HASH_SIZE];
    let b_hash = &b[..TAGS_HASH_SIZE];
    match a_hash.cmp(b_hash) {
        std::cmp::Ordering::Equal => {}
        ord => return ord,
    }

    let a_ts = &a[TAGS_HASH_SIZE..];
    let b_ts = &b[TAGS_HASH_SIZE..];
    a_ts.cmp(b_ts)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(hash: u64, ts: i64) -> Vec<u8> {
        let mut v = hash.to_be_bytes().to_vec();
        v.extend_from_slice(&ts.to_be_bytes());
        v
    }

    #[test]
    fn test_comparator_same_hash_different_timestamp() {
        let a = make_key(100, 1000);
        let b = make_key(100, 2000);
        assert_eq!(tsdb_compare(&a, &b), std::cmp::Ordering::Less);
        assert_eq!(tsdb_compare(&b, &a), std::cmp::Ordering::Greater);
    }

    #[test]
    fn test_comparator_different_hash() {
        let a = make_key(99, 9999);
        let b = make_key(100, 0);
        assert_eq!(tsdb_compare(&a, &b), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_comparator_equal_keys() {
        let a = make_key(42, 1234);
        let b = make_key(42, 1234);
        assert_eq!(tsdb_compare(&a, &b), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_comparator_min_max() {
        let min = make_key(u64::MIN, i64::MIN);
        let max = make_key(u64::MAX, i64::MAX);
        assert_eq!(tsdb_compare(&min, &max), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_comparator_sorts_correctly() {
        let keys = vec![
            make_key(1, 10),
            make_key(2, 5),
            make_key(1, 20),
            make_key(3, 0),
            make_key(1, 15),
            make_key(2, 3),
        ];
        let mut sorted = keys.clone();
        sorted.sort_by(|a, b| tsdb_compare(a, b));

        for i in 0..sorted.len() - 1 {
            assert!(
                tsdb_compare(&sorted[i], &sorted[i + 1]) != std::cmp::Ordering::Greater,
                "not sorted at index {}",
                i
            );
        }
    }

    #[test]
    fn test_comparator_transitivity() {
        use std::hash::Hasher;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        for _ in 0..10000 {
            std::hash::Hash::hash(&std::time::SystemTime::now(), &mut hasher);
            let seed = hasher.finish();

            let a = make_key(seed.wrapping_mul(13), (seed & 0xFFFF) as i64);
            let b = make_key(seed.wrapping_mul(17), ((seed >> 16) & 0xFFFF) as i64);
            let c = make_key(seed.wrapping_mul(23), ((seed >> 32) & 0xFFFF) as i64);

            let ab = tsdb_compare(&a, &b);
            let bc = tsdb_compare(&b, &c);

            if ab == std::cmp::Ordering::Less && bc == std::cmp::Ordering::Less {
                assert_eq!(
                    tsdb_compare(&a, &c),
                    std::cmp::Ordering::Less,
                    "transitivity violated"
                );
            }
        }
    }

    #[test]
    fn test_comparator_antisymmetry() {
        let cases = vec![
            (make_key(1, 10), make_key(1, 20)),
            (make_key(1, 10), make_key(2, 5)),
            (make_key(u64::MIN, i64::MIN), make_key(u64::MAX, i64::MAX)),
            (make_key(0, 0), make_key(0, 1)),
        ];

        for (a, b) in cases {
            let ab = tsdb_compare(&a, &b);
            let ba = tsdb_compare(&b, &a);
            assert_eq!(ab, ba.reverse(), "antisymmetry violated");
        }
    }

    #[test]
    fn test_comparator_reflexivity() {
        let cases = vec![
            make_key(0, 0),
            make_key(u64::MAX, i64::MAX),
            make_key(42, 1234),
        ];

        for key in cases {
            assert_eq!(
                tsdb_compare(&key, &key),
                std::cmp::Ordering::Equal,
                "reflexivity violated"
            );
        }
    }

    #[test]
    fn test_comparator_different_length_keys() {
        let full_key = make_key(42, 100);
        let short_key = vec![0u8; 4];

        let result = tsdb_compare(&short_key, &full_key);
        assert_ne!(
            result,
            std::cmp::Ordering::Equal,
            "different length keys should not be equal"
        );
    }
}
