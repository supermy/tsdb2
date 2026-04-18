use crate::compaction_filter::TsdbTtlFilterFactory;
use crate::comparator::tsdb_compare;
use crate::config::RocksDbConfig;
use crate::error::{Result, TsdbRocksDbError};
use crate::key::{compute_tags_hash, TsdbKey, TAGS_HASH_SIZE};
use crate::merge::{tsdb_full_merge, tsdb_partial_merge};
use crate::snapshot::TsdbSnapshot;
use crate::tags::{decode_tags, encode_tags};
use crate::value::{decode_fields, encode_fields};
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, Cache, ColumnFamilyDescriptor, DBCompressionType,
    IteratorMode, Options, ReadOptions, SliceTransform, WriteBatch, DB,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// 标签元数据 Column Family 名称
const SERIES_META_CF: &str = "_series_meta";
/// 时序数据 Column Family 名称前缀
const TS_CF_PREFIX: &str = "ts_";

/// RocksDB 时序存储引擎
///
/// 封装 RocksDB 实例，提供时序数据专用的读写接口。
/// 内部使用双 CF 架构消除标签冗余:
/// - `_series_meta`: 标签元数据 (每个 series 只存一次)
/// - `ts_{measurement}_{YYYYMMDD}`: 纯数据 (无标签冗余)
///
/// # 示例
/// ```ignore
/// let db = TsdbRocksDb::open("./data", RocksDbConfig::default())?;
/// db.put("cpu", &tags, timestamp, &fields)?;
/// let result = db.read_range("cpu", start, end)?;
/// ```
pub struct TsdbRocksDb {
    db: DB,
    config: RocksDbConfig,
    cache: Cache,
    base_dir: PathBuf,
}

impl TsdbRocksDb {
    /// 打开或创建时序存储引擎
    ///
    /// 如果指定路径不存在则自动创建。已存在的 CF 会根据名称
    /// 自动应用对应的 Options (meta / data)。
    pub fn open(path: impl AsRef<Path>, config: RocksDbConfig) -> Result<Self> {
        let base_dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_dir)?;

        let cache = Cache::new_lru_cache(config.cache_size);

        let mut db_opts = Self::db_options(&config);
        let block_opts = Self::block_options(&cache, &config);

        let existing_cfs = DB::list_cf(&Options::default(), &base_dir).unwrap_or_default();
        let mut cf_descriptors: Vec<ColumnFamilyDescriptor> =
            vec![ColumnFamilyDescriptor::new("default", Options::default())];

        for cf_name in &existing_cfs {
            if *cf_name == "default" {
                continue;
            } else if *cf_name == SERIES_META_CF {
                cf_descriptors.push(ColumnFamilyDescriptor::new(
                    SERIES_META_CF,
                    Self::meta_cf_options(),
                ));
            } else if cf_name.starts_with(TS_CF_PREFIX) {
                cf_descriptors.push(ColumnFamilyDescriptor::new(
                    cf_name,
                    Self::data_cf_options(&block_opts, &config),
                ));
            }
        }

        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let db = DB::open_cf_descriptors(&db_opts, &base_dir, cf_descriptors)?;

        Ok(Self {
            db,
            config,
            cache,
            base_dir,
        })
    }

    /// 数据库全局选项: 自定义比较器 + 无文件数限制
    fn db_options(_config: &RocksDbConfig) -> Options {
        let mut opts = Options::default();
        opts.set_comparator("tsdb_comparator", Box::new(tsdb_compare));
        opts.set_max_open_files(-1);
        opts
    }

    /// Block-Based Table 选项: LRU 缓存 + Bloom Filter + 元数据缓存
    fn block_options(cache: &Cache, _config: &RocksDbConfig) -> BlockBasedOptions {
        let mut opts = BlockBasedOptions::default();
        opts.set_block_cache(cache);
        opts.set_bloom_filter(10.0, false);
        opts.set_format_version(5);
        opts.set_cache_index_and_filter_blocks(true);
        opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        opts
    }

    /// 标签元数据 CF 选项: 仅 Zstd 压缩
    fn meta_cf_options() -> Options {
        let mut opts = Options::default();
        opts.set_compression_type(DBCompressionType::Zstd);
        opts
    }

    /// 时序数据 CF 选项: Zstd 压缩 + 前缀 Bloom + Merge + TTL Filter
    fn data_cf_options(block_opts: &BlockBasedOptions, config: &RocksDbConfig) -> Options {
        let mut opts = Options::default();
        opts.set_block_based_table_factory(block_opts);
        opts.set_compression_type(DBCompressionType::Zstd);
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(TAGS_HASH_SIZE));
        opts.set_memtable_prefix_bloom_ratio(0.1);
        opts.set_max_write_buffer_number(4);
        opts.set_write_buffer_size(config.cf_write_buffer_size);
        opts.set_max_bytes_for_level_base(config.cf_max_bytes_for_level_base);
        opts.set_merge_operator("tsdb_field_merge", tsdb_full_merge, tsdb_partial_merge);
        if config.default_ttl_secs > 0 {
            opts.set_compaction_filter_factory(TsdbTtlFilterFactory::new(config.default_ttl_secs));
        }
        opts
    }

    /// 写入一个数据点
    ///
    /// 自动完成以下操作:
    /// 1. 计算 tags_hash 并写入 `_series_meta` CF (幂等)
    /// 2. 编码 key = tags_hash(BE) + timestamp(BE) (16 bytes)
    /// 3. 编码 value = fields 二进制格式
    /// 4. 写入对应的日期分区 CF `ts_{measurement}_{YYYYMMDD}`
    pub fn put(
        &self,
        measurement: &str,
        tags: &tsdb_arrow::schema::Tags,
        timestamp: i64,
        fields: &tsdb_arrow::schema::Fields,
    ) -> Result<()> {
        let tags_hash = compute_tags_hash(tags);
        let key = TsdbKey::new(tags_hash, timestamp).encode();

        let meta_cf = self.ensure_meta_cf()?;
        self.db
            .put_cf(&meta_cf, tags_hash.to_be_bytes(), encode_tags(tags))?;

        let date = micros_to_date(timestamp)?;
        let cf_name = format!("{}{}_{}", TS_CF_PREFIX, measurement, date.format("%Y%m%d"));
        let data_cf = self.ensure_data_cf(&cf_name)?;
        let value = encode_fields(fields);
        self.db.put_cf(&data_cf, &key, &value)?;
        Ok(())
    }

    /// 合并字段到已有数据点 (union 语义, 同名字段后者覆盖)
    ///
    /// 如果该 key 不存在则等同于 put; 如果已存在则调用 MergeOperator 合并。
    pub fn merge(
        &self,
        measurement: &str,
        tags: &tsdb_arrow::schema::Tags,
        timestamp: i64,
        fields: &tsdb_arrow::schema::Fields,
    ) -> Result<()> {
        let tags_hash = compute_tags_hash(tags);
        let key = TsdbKey::new(tags_hash, timestamp).encode();

        let meta_cf = self.ensure_meta_cf()?;
        self.db
            .put_cf(&meta_cf, tags_hash.to_be_bytes(), encode_tags(tags))?;

        let date = micros_to_date(timestamp)?;
        let cf_name = format!("{}{}_{}", TS_CF_PREFIX, measurement, date.format("%Y%m%d"));
        let data_cf = self.ensure_data_cf(&cf_name)?;
        let value = encode_fields(fields);
        self.db.merge_cf(&data_cf, &key, &value)?;
        Ok(())
    }

    /// 批量写入数据点 (按 CF 分组, 每组一个 WriteBatch 原子提交)
    ///
    /// 比逐条 put 更高效: 减少 WAL fsync 次数, 降低写入放大。
    pub fn write_batch(&self, dps: &[tsdb_arrow::schema::DataPoint]) -> Result<()> {
        let mut by_cf: BTreeMap<String, Vec<&tsdb_arrow::schema::DataPoint>> = BTreeMap::new();
        for dp in dps {
            let date = micros_to_date(dp.timestamp).ok();
            if let Some(date) = date {
                let cf_name = format!(
                    "{}{}_{}",
                    TS_CF_PREFIX,
                    dp.measurement,
                    date.format("%Y%m%d")
                );
                by_cf.entry(cf_name).or_default().push(dp);
            }
        }

        let meta_cf = self.ensure_meta_cf()?;

        for (cf_name, group) in &by_cf {
            let data_cf = self.ensure_data_cf(cf_name)?;
            let mut batch = WriteBatch::default();

            for dp in group {
                let tags_hash = compute_tags_hash(&dp.tags);
                let key = TsdbKey::new(tags_hash, dp.timestamp).encode();
                let value = encode_fields(&dp.fields);
                batch.put_cf(&data_cf, &key, &value);
                batch.put_cf(&meta_cf, tags_hash.to_be_bytes(), encode_tags(&dp.tags));
            }

            self.db.write(batch)?;
        }

        Ok(())
    }

    /// 读取单个数据点 (零拷贝 get_pinned_cf)
    pub fn get(
        &self,
        measurement: &str,
        tags: &tsdb_arrow::schema::Tags,
        timestamp: i64,
    ) -> Result<Option<tsdb_arrow::schema::DataPoint>> {
        let tags_hash = compute_tags_hash(tags);
        let key = TsdbKey::new(tags_hash, timestamp).encode();

        let date = micros_to_date(timestamp)?;
        let cf_name = format!("{}{}_{}", TS_CF_PREFIX, measurement, date.format("%Y%m%d"));

        let data_cf = match self.db.cf_handle(&cf_name) {
            Some(cf) => cf,
            None => return Ok(None),
        };

        let value = self.db.get_pinned_cf(&data_cf, &key)?;
        match value {
            Some(v) => {
                let fields = decode_fields(&v)?;
                Ok(Some(tsdb_arrow::schema::DataPoint {
                    measurement: measurement.to_string(),
                    tags: tags.clone(),
                    fields,
                    timestamp,
                }))
            }
            None => Ok(None),
        }
    }

    /// 范围查询: 扫描指定时间范围内的所有数据点
    ///
    /// 自动遍历涉及的日期分区 CF, 合并结果并按时间排序。
    pub fn read_range(
        &self,
        measurement: &str,
        start_micros: i64,
        end_micros: i64,
    ) -> Result<Vec<tsdb_arrow::schema::DataPoint>> {
        let start_date = micros_to_date(start_micros)?;
        let end_date = micros_to_date(end_micros)?;

        let mut results = Vec::new();
        let mut current_date = start_date;

        while current_date <= end_date {
            let cf_name = format!(
                "{}{}_{}",
                TS_CF_PREFIX,
                measurement,
                current_date.format("%Y%m%d")
            );
            if let Some(data_cf) = self.db.cf_handle(&cf_name) {
                let iter = self.db.iterator_cf(&data_cf, IteratorMode::Start);
                for item in iter {
                    let (raw_key, raw_value) = item?;
                    let ts_key = TsdbKey::decode(&raw_key)?;
                    if ts_key.timestamp < start_micros || ts_key.timestamp > end_micros {
                        continue;
                    }
                    let fields = decode_fields(&raw_value)?;

                    if let Some(meta_cf) = self.db.cf_handle(SERIES_META_CF) {
                        if let Some(tags_value) = self
                            .db
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
                }
            }
            current_date += chrono::Duration::days(1);
        }

        results.sort_by_key(|dp| dp.timestamp);
        Ok(results)
    }

    /// 前缀扫描: 查询指定标签集合在时间范围内的数据点
    ///
    /// 利用 SliceTransform 前缀索引, 只扫描 tags_hash 匹配的行,
    /// 比全范围 read_range 更高效 (跳过不相关 series)。
    pub fn prefix_scan(
        &self,
        measurement: &str,
        tags: &tsdb_arrow::schema::Tags,
        start_micros: i64,
        end_micros: i64,
    ) -> Result<Vec<tsdb_arrow::schema::DataPoint>> {
        let tags_hash = compute_tags_hash(tags);

        let start_date = micros_to_date(start_micros)?;
        let end_date = micros_to_date(end_micros)?;

        let mut results = Vec::new();
        let mut current_date = start_date;

        while current_date <= end_date {
            let cf_name = format!(
                "{}{}_{}",
                TS_CF_PREFIX,
                measurement,
                current_date.format("%Y%m%d")
            );
            if let Some(data_cf) = self.db.cf_handle(&cf_name) {
                let mut read_opts = ReadOptions::default();
                let start_key = TsdbKey::new(tags_hash, start_micros).encode();
                let end_key = TsdbKey::new(tags_hash, end_micros + 1).encode();
                read_opts.set_iterate_range(start_key..end_key);

                let iter = self
                    .db
                    .iterator_cf_opt(&data_cf, read_opts, IteratorMode::Start);
                for item in iter {
                    let (raw_key, raw_value) = item?;
                    let ts_key = TsdbKey::decode(&raw_key)?;
                    if ts_key.tags_hash != tags_hash {
                        continue;
                    }
                    let fields = decode_fields(&raw_value)?;
                    results.push(tsdb_arrow::schema::DataPoint {
                        measurement: measurement.to_string(),
                        tags: tags.clone(),
                        fields,
                        timestamp: ts_key.timestamp,
                    });
                }
            }
            current_date += chrono::Duration::days(1);
        }

        results.sort_by_key(|dp| dp.timestamp);
        Ok(results)
    }

    /// 创建一致性快照 (创建后新写入不可见)
    pub fn snapshot(&self) -> TsdbSnapshot<'_> {
        TsdbSnapshot::new(self.db.snapshot())
    }

    /// 删除指定的 Column Family (用于过期数据清理)
    pub fn drop_cf(&self, cf_name: &str) -> Result<()> {
        self.db.drop_cf(cf_name)?;
        Ok(())
    }

    /// 手动触发指定 CF 的 Compaction
    pub fn compact_cf(&self, cf_name: &str) -> Result<()> {
        if let Some(cf) = self.db.cf_handle(cf_name) {
            self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        }
        Ok(())
    }

    /// 列出所有时序数据 CF (名称以 "ts_" 开头)
    pub fn list_ts_cfs(&self) -> Vec<String> {
        let cfs: Vec<String> = DB::list_cf(&Options::default(), &self.base_dir)
            .unwrap_or_default()
            .into_iter()
            .filter(|name| name.starts_with(TS_CF_PREFIX))
            .collect();
        cfs
    }

    /// 获取数据库统计信息 (字符串格式)
    pub fn stats(&self) -> String {
        self.db
            .property_value(rocksdb::properties::STATS)
            .ok()
            .flatten()
            .unwrap_or_default()
    }

    /// 获取指定 CF 的统计信息
    pub fn cf_stats(&self, cf_name: &str) -> Option<String> {
        let cf = self.db.cf_handle(cf_name)?;
        self.db
            .property_value_cf(&cf, rocksdb::properties::STATS)
            .ok()
            .flatten()
    }

    /// 确保标签元数据 CF 存在, 不存在则创建
    fn ensure_meta_cf(&self) -> Result<Arc<BoundColumnFamily<'_>>> {
        if let Some(cf) = self.db.cf_handle(SERIES_META_CF) {
            return Ok(cf);
        }
        self.db
            .create_cf(SERIES_META_CF, &Self::meta_cf_options())?;
        Ok(self.db.cf_handle(SERIES_META_CF).unwrap())
    }

    /// 确保时序数据 CF 存在, 不存在则创建 (含竞态安全处理)
    fn ensure_data_cf(&self, cf_name: &str) -> Result<Arc<BoundColumnFamily<'_>>> {
        if let Some(cf) = self.db.cf_handle(cf_name) {
            return Ok(cf);
        }
        let block_opts = Self::block_options(&self.cache, &self.config);
        match self
            .db
            .create_cf(cf_name, &Self::data_cf_options(&block_opts, &self.config))
        {
            Ok(_) => {}
            Err(e) => {
                if let Some(cf) = self.db.cf_handle(cf_name) {
                    return Ok(cf);
                }
                return Err(e.into());
            }
        }
        Ok(self.db.cf_handle(cf_name).unwrap())
    }

    /// 获取数据目录路径
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// 获取底层 RocksDB 实例引用
    pub fn db(&self) -> &DB {
        &self.db
    }
}

/// 微秒时间戳转日期 (用于确定 CF 分区)
fn micros_to_date(micros: i64) -> Result<chrono::NaiveDate> {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|dt| dt.date_naive())
        .ok_or_else(|| TsdbRocksDbError::InvalidKey(format!("invalid timestamp: {}", micros)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tsdb_arrow::schema::{FieldValue, Tags};

    /// 构造单标签 Tags
    fn make_tags(host: &str) -> Tags {
        let mut tags = Tags::new();
        tags.insert("host".to_string(), host.to_string());
        tags
    }

    /// 构造单字段 Fields
    fn make_fields(usage: f64) -> tsdb_arrow::schema::Fields {
        let mut fields = tsdb_arrow::schema::Fields::new();
        fields.insert("usage".to_string(), FieldValue::Float(usage));
        fields
    }

    /// 获取当天零点的微秒时间戳 (避免硬编码日期)
    fn now_ts() -> i64 {
        chrono::Utc::now()
            .date_naive()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros()
    }

    /// 获取 N 天前的微秒时间戳
    fn days_ago_ts(days: u32) -> i64 {
        let date = chrono::Utc::now().date_naive() - chrono::Duration::days(days as i64);
        date.and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros()
    }

    #[test]
    fn test_db_open_create() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        assert_eq!(db.list_ts_cfs().len(), 0);
    }

    #[test]
    fn test_db_put_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let fields = make_fields(2.71);
        let ts = now_ts();

        db.put("cpu", &tags, ts, &fields).unwrap();

        let result = db.get("cpu", &tags, ts).unwrap();
        assert!(result.is_some());
        let got = result.unwrap();
        assert_eq!(got.measurement, "cpu");
        assert_eq!(got.timestamp, ts);
        assert_eq!(got.tags.get("host").unwrap(), "server01");
        assert_eq!(*got.fields.get("usage").unwrap(), FieldValue::Float(2.71));
    }

    #[test]
    fn test_db_get_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let result = db.get("cpu", &tags, 9999999999999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_db_read_range_empty() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let result = db.read_range("cpu", 1000, 2000).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_db_write_and_read_range() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = now_ts();

        for i in 0..10i64 {
            let mut fields = tsdb_arrow::schema::Fields::new();
            fields.insert("usage".to_string(), FieldValue::Float(i as f64));
            db.put("cpu", &tags, base_ts + i * 1_000_000, &fields)
                .unwrap();
        }

        let result = db.read_range("cpu", base_ts, base_ts + 9_000_000).unwrap();
        assert_eq!(result.len(), 10);
        for (i, dp) in result.iter().enumerate() {
            assert_eq!(
                *dp.fields.get("usage").unwrap(),
                FieldValue::Float(i as f64)
            );
        }
    }

    #[test]
    fn test_db_auto_create_cf() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let fields = make_fields(1.0);

        db.put("metric_a", &tags, now_ts(), &fields).unwrap();

        let cfs = db.list_ts_cfs();
        assert_eq!(cfs.len(), 1);
        assert!(cfs[0].starts_with("ts_metric_a_"));
    }

    #[test]
    fn test_db_recover_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let ts = now_ts();

        {
            let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
            let tags = make_tags("server01");
            let fields = make_fields(42.0);
            db.put("cpu", &tags, ts, &fields).unwrap();
        }

        {
            let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
            let tags = make_tags("server01");
            let result = db.get("cpu", &tags, ts).unwrap();
            assert!(result.is_some());
            assert_eq!(
                *result.unwrap().fields.get("usage").unwrap(),
                FieldValue::Float(42.0)
            );
        }
    }

    #[test]
    fn test_db_different_measurements_different_cfs() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let fields = make_fields(1.0);
        let ts = now_ts();

        db.put("cpu", &tags, ts, &fields).unwrap();
        db.put("memory", &tags, ts, &fields).unwrap();

        let cfs = db.list_ts_cfs();
        assert_eq!(cfs.len(), 2);
    }

    #[test]
    fn test_db_same_series_different_dates() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let fields = make_fields(1.0);

        db.put("cpu", &tags, now_ts(), &fields).unwrap();
        db.put("cpu", &tags, days_ago_ts(1), &fields).unwrap();

        let cfs = db.list_ts_cfs();
        assert_eq!(cfs.len(), 2);
    }

    #[test]
    fn test_db_tags_deduplication_in_meta_cf() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = now_ts();

        for i in 0..100i64 {
            let mut fields = tsdb_arrow::schema::Fields::new();
            fields.insert("usage".to_string(), FieldValue::Float(i as f64));
            db.put("cpu", &tags, base_ts + i * 1_000_000, &fields)
                .unwrap();
        }

        let meta_cf = db.db.cf_handle(SERIES_META_CF).unwrap();
        let mut count = 0;
        let iter = db.db.iterator_cf(&meta_cf, IteratorMode::Start);
        for item in iter {
            let _ = item.unwrap();
            count += 1;
        }
        assert_eq!(count, 1, "same tags should only have one entry in meta CF");
    }

    #[test]
    fn test_db_multiple_series() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags_a = make_tags("server01");
        let tags_b = make_tags("server02");
        let fields = make_fields(1.0);
        let ts = now_ts();

        db.put("cpu", &tags_a, ts, &fields).unwrap();
        db.put("cpu", &tags_b, ts, &fields).unwrap();

        let result = db.read_range("cpu", ts, ts).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_db_merge_union_fields() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let ts = now_ts();

        let mut fields1 = tsdb_arrow::schema::Fields::new();
        fields1.insert("cpu".to_string(), FieldValue::Float(1.0));
        db.put("metrics", &tags, ts, &fields1).unwrap();

        let mut fields2 = tsdb_arrow::schema::Fields::new();
        fields2.insert("mem".to_string(), FieldValue::Float(2.0));
        db.merge("metrics", &tags, ts, &fields2).unwrap();

        let result = db.get("metrics", &tags, ts).unwrap().unwrap();
        assert_eq!(*result.fields.get("cpu").unwrap(), FieldValue::Float(1.0));
        assert_eq!(*result.fields.get("mem").unwrap(), FieldValue::Float(2.0));
    }

    #[test]
    fn test_db_merge_overwrite_field() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let ts = now_ts();

        let mut fields1 = tsdb_arrow::schema::Fields::new();
        fields1.insert("cpu".to_string(), FieldValue::Float(1.0));
        db.put("metrics", &tags, ts, &fields1).unwrap();

        let mut fields2 = tsdb_arrow::schema::Fields::new();
        fields2.insert("cpu".to_string(), FieldValue::Float(99.0));
        db.merge("metrics", &tags, ts, &fields2).unwrap();

        let result = db.get("metrics", &tags, ts).unwrap().unwrap();
        assert_eq!(*result.fields.get("cpu").unwrap(), FieldValue::Float(99.0));
    }

    #[test]
    fn test_db_write_batch() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let base_ts = now_ts();
        let mut dps = Vec::new();
        for i in 0..100i64 {
            let mut tags = Tags::new();
            tags.insert("host".to_string(), format!("server{:02}", i % 5));
            let mut fields = tsdb_arrow::schema::Fields::new();
            fields.insert("usage".to_string(), FieldValue::Float(i as f64));
            dps.push(tsdb_arrow::schema::DataPoint {
                measurement: "cpu".to_string(),
                tags,
                fields,
                timestamp: base_ts + i * 1_000_000,
            });
        }

        db.write_batch(&dps).unwrap();

        let result = db.read_range("cpu", base_ts, base_ts + 99_000_000).unwrap();
        assert_eq!(result.len(), 100);
    }

    #[test]
    fn test_db_write_batch_cross_day() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let ts_day1 = now_ts();
        let ts_day2 = days_ago_ts(1);

        let tags = make_tags("server01");
        let mut fields = tsdb_arrow::schema::Fields::new();
        fields.insert("v".to_string(), FieldValue::Float(1.0));

        let dps = vec![
            tsdb_arrow::schema::DataPoint {
                measurement: "cpu".to_string(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: ts_day1,
            },
            tsdb_arrow::schema::DataPoint {
                measurement: "cpu".to_string(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: ts_day2,
            },
        ];

        db.write_batch(&dps).unwrap();

        let cfs = db.list_ts_cfs();
        assert_eq!(cfs.len(), 2);

        let r1 = db.get("cpu", &tags, ts_day1).unwrap();
        assert!(r1.is_some());
        let r2 = db.get("cpu", &tags, ts_day2).unwrap();
        assert!(r2.is_some());
    }

    #[test]
    fn test_db_prefix_scan() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags_a = make_tags("server01");
        let tags_b = make_tags("server02");
        let base_ts = now_ts();

        for i in 0..5i64 {
            let mut fields = tsdb_arrow::schema::Fields::new();
            fields.insert("usage".to_string(), FieldValue::Float(i as f64));
            db.put("cpu", &tags_a, base_ts + i * 1_000_000, &fields)
                .unwrap();
        }
        for i in 0..3i64 {
            let mut fields = tsdb_arrow::schema::Fields::new();
            fields.insert("usage".to_string(), FieldValue::Float(i as f64 * 10.0));
            db.put("cpu", &tags_b, base_ts + i * 1_000_000, &fields)
                .unwrap();
        }

        let result_a = db
            .prefix_scan("cpu", &tags_a, base_ts, base_ts + 4_000_000)
            .unwrap();
        assert_eq!(result_a.len(), 5);

        let result_b = db
            .prefix_scan("cpu", &tags_b, base_ts, base_ts + 2_000_000)
            .unwrap();
        assert_eq!(result_b.len(), 3);

        assert_eq!(
            *result_a[0].fields.get("usage").unwrap(),
            FieldValue::Float(0.0)
        );
        assert_eq!(
            *result_b[0].fields.get("usage").unwrap(),
            FieldValue::Float(0.0)
        );
    }

    #[test]
    fn test_db_snapshot_consistency() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = now_ts();

        let mut fields1 = tsdb_arrow::schema::Fields::new();
        fields1.insert("usage".to_string(), FieldValue::Float(1.0));
        db.put("cpu", &tags, base_ts, &fields1).unwrap();

        let snapshot = db.snapshot();

        let mut fields2 = tsdb_arrow::schema::Fields::new();
        fields2.insert("usage".to_string(), FieldValue::Float(2.0));
        db.put("cpu", &tags, base_ts + 1_000_000, &fields2).unwrap();

        let today = chrono::Utc::now().format("%Y%m%d").to_string();
        let cf_name = format!("ts_cpu_{}", today);
        let snap_result = snapshot
            .get(
                &db.db().cf_handle(&cf_name).unwrap(),
                &TsdbKey::new(compute_tags_hash(&tags), base_ts + 1_000_000).encode(),
            )
            .unwrap();
        assert!(
            snap_result.is_none(),
            "snapshot should not see data written after snapshot creation"
        );
    }

    #[test]
    fn test_db_drop_cf() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let fields = make_fields(1.0);

        db.put("cpu", &tags, now_ts(), &fields).unwrap();
        db.put("memory", &tags, now_ts(), &fields).unwrap();

        assert_eq!(db.list_ts_cfs().len(), 2);

        let cfs = db.list_ts_cfs();
        let memory_cf = cfs.iter().find(|c| c.contains("memory")).unwrap().clone();
        db.drop_cf(&memory_cf).unwrap();

        assert_eq!(db.list_ts_cfs().len(), 1);
    }

    #[test]
    fn test_db_compact_cf() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let base_ts = now_ts();

        for i in 0..100i64 {
            let mut fields = tsdb_arrow::schema::Fields::new();
            fields.insert("usage".to_string(), FieldValue::Float(i as f64));
            db.put("cpu", &tags, base_ts + i * 1_000_000, &fields)
                .unwrap();
        }

        let cfs = db.list_ts_cfs();
        let cpu_cf = cfs.iter().find(|c| c.contains("cpu")).unwrap().clone();
        db.compact_cf(&cpu_cf).unwrap();

        let result = db.read_range("cpu", base_ts, base_ts + 99_000_000).unwrap();
        assert_eq!(result.len(), 100);
    }

    #[test]
    fn test_db_cf_stats() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let fields = make_fields(1.0);
        db.put("cpu", &tags, now_ts(), &fields).unwrap();

        let cfs = db.list_ts_cfs();
        let cpu_cf = cfs.iter().find(|c| c.contains("cpu")).unwrap().clone();
        let stats = db.cf_stats(&cpu_cf);
        assert!(stats.is_some());
    }
}
