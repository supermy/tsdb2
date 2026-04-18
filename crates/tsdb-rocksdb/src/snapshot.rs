use crate::error::Result;
use crate::key::TsdbKey;
use crate::tags::decode_tags;
use crate::value::decode_fields;
use rocksdb::{IteratorMode, ReadOptions, SnapshotWithThreadMode, DB};

/// 全局元数据 CF 名称
const SERIES_META_CF: &str = "_series_meta";

/// 一致性快照
///
/// 基于 RocksDB Snapshot 实现的一致性读取视图。
/// 快照创建后，后续的写入操作对快照不可见，保证读取的一致性。
///
/// # 生命周期
///
/// `'a` 绑定到底层 `DB` 实例，快照的生命周期不能超过数据库实例。
///
/// # 示例
///
/// ```ignore
/// let snapshot = db.snapshot();
/// // 此后所有通过 snapshot 的读取都基于创建时刻的数据视图
/// let result = snapshot.get(&cf, &key)?;
/// ```
pub struct TsdbSnapshot<'a> {
    snapshot: SnapshotWithThreadMode<'a, DB>,
}

impl<'a> TsdbSnapshot<'a> {
    /// 从 RocksDB Snapshot 创建一致性快照
    pub fn new(snapshot: SnapshotWithThreadMode<'a, DB>) -> Self {
        Self { snapshot }
    }

    /// 在快照内读取指定 CF 中的单个键
    ///
    /// # 参数
    /// - `cf` - Column Family 引用
    /// - `key` - 要读取的键
    ///
    /// # 返回
    /// - `Some(value)` - 键存在且在快照创建时已有
    /// - `None` - 键不存在或在快照创建后写入
    pub fn get(&self, cf: &impl rocksdb::AsColumnFamilyRef, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.snapshot.get_cf(cf, key)?)
    }

    /// 在快照内按时间范围读取数据点
    ///
    /// 读取逻辑:
    /// 1. 在数据 CF 中按 Key 范围迭代
    /// 2. 解码每个 KV 对为 TsdbKey + Fields
    /// 3. 通过 tags_hash 在 _series_meta CF 中查找标签
    /// 4. 组装为 DataPoint 返回
    ///
    /// # 参数
    /// - `db` - 底层 RocksDB 实例引用
    /// - `cf_name` - 数据 Column Family 名称
    /// - `start_micros` - 起始时间戳 (微秒)
    /// - `end_micros` - 结束时间戳 (微秒)
    /// - `measurement` - 度量名
    pub fn read_range(
        &self,
        db: &'a DB,
        cf_name: &str,
        start_micros: i64,
        end_micros: i64,
        measurement: &str,
    ) -> Result<Vec<tsdb_arrow::schema::DataPoint>> {
        let data_cf = match db.cf_handle(cf_name) {
            Some(cf) => cf,
            None => return Ok(Vec::new()),
        };

        let meta_cf = match db.cf_handle(SERIES_META_CF) {
            Some(cf) => cf,
            None => return Ok(Vec::new()),
        };

        let mut read_opts = ReadOptions::default();
        let start_key = TsdbKey::new(0, start_micros).encode();
        let end_key = TsdbKey::new(u64::MAX, end_micros).encode();
        read_opts.set_iterate_range(start_key..end_key);

        let iter = self
            .snapshot
            .iterator_cf_opt(&data_cf, read_opts, IteratorMode::Start);
        let mut results = Vec::new();

        for item in iter {
            let (raw_key, raw_value) = item?;
            let ts_key = TsdbKey::decode(&raw_key)?;
            if ts_key.timestamp < start_micros || ts_key.timestamp > end_micros {
                continue;
            }
            let fields = decode_fields(&raw_value)?;

            if let Some(tags_value) = self
                .snapshot
                .get_pinned_cf(&meta_cf, ts_key.tags_hash.to_be_bytes())?
            {
                let tags = decode_tags(&tags_value)?;
                results.push(tsdb_arrow::schema::DataPoint {
                    measurement: measurement.to_string(),
                    tags,
                    fields,
                    timestamp: ts_key.timestamp,
                });
            }
        }

        results.sort_by_key(|dp| dp.timestamp);
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RocksDbConfig;
    use crate::db::TsdbRocksDb;
    use crate::key::compute_tags_hash;
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

    fn now_ts() -> i64 {
        chrono::Utc::now()
            .date_naive()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros()
    }

    #[test]
    fn test_snapshot_get_after_write_invisible() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = now_ts();

        let fields1 = make_fields(1.0);
        db.put("cpu", &tags, base_ts, &fields1).unwrap();

        let today = chrono::Utc::now().format("%Y%m%d").to_string();
        let cf_name = format!("ts_cpu_{}", today);
        let data_cf = db.db().cf_handle(&cf_name).unwrap();

        let snapshot = db.snapshot();

        let fields2 = make_fields(2.0);
        db.put("cpu", &tags, base_ts + 1_000_000, &fields2).unwrap();

        let snap_result = snapshot
            .get(
                &data_cf,
                &TsdbKey::new(compute_tags_hash(&tags), base_ts + 1_000_000).encode(),
            )
            .unwrap();
        assert!(
            snap_result.is_none(),
            "snapshot should not see data written after creation"
        );

        let existing_result = snapshot
            .get(
                &data_cf,
                &TsdbKey::new(compute_tags_hash(&tags), base_ts).encode(),
            )
            .unwrap();
        assert!(
            existing_result.is_some(),
            "snapshot should see data written before creation"
        );
    }

    #[test]
    fn test_snapshot_read_range_consistency() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = now_ts();

        for i in 0..5i64 {
            let fields = make_fields(i as f64);
            db.put("cpu", &tags, base_ts + i * 1_000_000, &fields)
                .unwrap();
        }

        let db_result = db.read_range("cpu", base_ts, base_ts + 4_000_000).unwrap();
        assert_eq!(db_result.len(), 5, "should read all 5 points");
    }

    #[test]
    fn test_snapshot_multiple_independent() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = now_ts();

        let fields1 = make_fields(1.0);
        db.put("cpu", &tags, base_ts, &fields1).unwrap();

        let today = chrono::Utc::now().format("%Y%m%d").to_string();
        let cf_name = format!("ts_cpu_{}", today);
        let data_cf = db.db().cf_handle(&cf_name).unwrap();

        let snap1 = db.snapshot();

        let fields2 = make_fields(2.0);
        db.put("cpu", &tags, base_ts + 1_000_000, &fields2).unwrap();

        let snap2 = db.snapshot();

        let key2 = TsdbKey::new(compute_tags_hash(&tags), base_ts + 1_000_000).encode();
        assert!(snap1.get(&data_cf, &key2).unwrap().is_none());
        assert!(snap2.get(&data_cf, &key2).unwrap().is_some());
    }

    #[test]
    fn test_snapshot_cross_cf_read() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = now_ts();

        let fields = make_fields(1.0);
        db.put("cpu", &tags, base_ts, &fields).unwrap();
        db.put("memory", &tags, base_ts, &fields).unwrap();

        let today = chrono::Utc::now().format("%Y%m%d").to_string();

        let cpu_cf_name = format!("ts_cpu_{}", today);
        let mem_cf_name = format!("ts_memory_{}", today);

        if let Some(cpu_cf) = db.db().cf_handle(&cpu_cf_name) {
            let key = TsdbKey::new(compute_tags_hash(&tags), base_ts).encode();
            let result = db.snapshot().get(&cpu_cf, &key).unwrap();
            assert!(result.is_some());
        };

        if let Some(mem_cf) = db.db().cf_handle(&mem_cf_name) {
            let key = TsdbKey::new(compute_tags_hash(&tags), base_ts).encode();
            let result = db.snapshot().get(&mem_cf, &key).unwrap();
            assert!(result.is_some());
        };
    }
}
