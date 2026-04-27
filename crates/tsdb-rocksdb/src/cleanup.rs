use crate::db::TsdbRocksDb;
use crate::error::Result;
use crate::key::TsdbKey;
use rocksdb::{BoundColumnFamily, DB};
use std::sync::Arc;

/// 时序数据 CF 名称前缀
const TS_CF_PREFIX: &str = "ts_";

/// TTL 清理管理器
///
/// 提供三级清理策略:
/// 1. **天级**: 整个过期 CF → `drop_cf` (O(1), 秒级删除)
/// 2. **范围级**: CF 内过期范围 → `delete_range_cf` (O(1), 需 flush + compact)
/// 3. **行级**: CompactionFilter 过滤过期行 (O(N), 在 Compaction 时自动执行)
pub struct TtlManager;

impl TtlManager {
    /// 删除所有过期的时序数据 CF (天级清理)
    ///
    /// 遍历所有 `ts_{measurement}_{YYYYMMDD}` 格式的 CF,
    /// 将日期部分早于 `now - retention_days` 的 CF 整体删除。
    ///
    /// # 参数
    /// - `db` - 时序存储引擎实例
    /// - `now` - 当前日期
    /// - `retention_days` - 数据保留天数
    ///
    /// # 返回
    /// 被删除的 CF 名称列表
    pub fn drop_expired_cfs(
        db: &TsdbRocksDb,
        now: chrono::NaiveDate,
        retention_days: u64,
    ) -> Result<Vec<String>> {
        let cutoff = now - chrono::Duration::days(retention_days as i64);
        let cutoff_str = cutoff.format("%Y%m%d").to_string();

        let mut dropped = Vec::new();
        for cf_name in db.list_ts_cfs() {
            if let Some(date_str) = cf_name.strip_prefix(TS_CF_PREFIX) {
                let parts: Vec<&str> = date_str.splitn(2, '_').collect();
                if parts.len() == 2 {
                    let date_part = parts[1];
                    if date_part < cutoff_str.as_str() {
                        db.drop_cf(&cf_name)?;
                        dropped.push(cf_name);
                    }
                }
            }
        }
        Ok(dropped)
    }

    /// 在指定 CF 内删除时间范围内的数据 (范围级清理)
    ///
    /// 构造覆盖 `[0, start_micros)` 到 `[MAX, end_micros)` 的键范围,
    /// 使用 RocksDB 的 `delete_range_cf` 高效删除。
    ///
    /// 注意: 删除后需要 flush + compact 才能实际释放磁盘空间。
    ///
    /// # 参数
    /// - `db` - RocksDB 原始实例
    /// - `cf` - 目标 Column Family
    /// - `start_micros` - 删除范围起始时间戳 (微秒)
    /// - `end_micros` - 删除范围结束时间戳 (微秒)
    pub fn delete_range_in_cf(
        db: &DB,
        cf: &Arc<BoundColumnFamily<'_>>,
        start_micros: i64,
        end_micros: i64,
    ) -> Result<()> {
        let start_key = TsdbKey::new(0, start_micros).encode();
        let end_key = TsdbKey::new(0, end_micros + 1).encode();
        db.delete_range_cf(cf, &start_key, &end_key)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RocksDbConfig;
    use chrono::Datelike;
    use tsdb_arrow::schema::{FieldValue, Tags};

    fn make_tags(host: &str) -> Tags {
        let mut tags = Tags::new();
        tags.insert("host".to_string(), host.to_string());
        tags
    }

    fn make_fields(usage: f64) -> tsdb_arrow::schema::Fields {
        let mut fields = tsdb_arrow::schema::Fields::new();
        fields.insert("usage".to_string(), FieldValue::Float(usage));
        fields
    }

    fn ts_for_date(y: i32, m: u32, d: u32) -> i64 {
        chrono::NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros()
    }

    fn today_date() -> chrono::NaiveDate {
        chrono::Utc::now().date_naive()
    }

    #[test]
    fn test_drop_expired_cfs() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let fields = make_fields(1.0);

        let today = today_date();
        let old_date = today - chrono::Duration::days(7);
        let recent_date = today;

        db.put(
            "cpu",
            &tags,
            ts_for_date(old_date.year(), old_date.month(), old_date.day()),
            &fields,
        )
        .unwrap();
        db.put(
            "cpu",
            &tags,
            ts_for_date(recent_date.year(), recent_date.month(), recent_date.day()),
            &fields,
        )
        .unwrap();

        assert_eq!(db.list_ts_cfs().len(), 2);

        let _dropped = TtlManager::drop_expired_cfs(&db, today, 5).unwrap();

        let remaining = db.list_ts_cfs();
        assert!(remaining.len() <= 1);
    }

    #[test]
    fn test_drop_expired_cfs_keeps_recent() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let fields = make_fields(1.0);

        let today = today_date();
        db.put(
            "cpu",
            &tags,
            ts_for_date(today.year(), today.month(), today.day()),
            &fields,
        )
        .unwrap();

        let dropped = TtlManager::drop_expired_cfs(&db, today, 30).unwrap();

        assert!(dropped.is_empty());
        assert_eq!(db.list_ts_cfs().len(), 1);
    }
}
