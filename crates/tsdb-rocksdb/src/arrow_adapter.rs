use crate::db::TsdbRocksDb;
use crate::error::Result;
use arrow::datatypes::SchemaRef;
use tsdb_arrow::converter::datapoints_to_record_batch;
use tsdb_arrow::schema::DataPoint;

/// Arrow 适配器
///
/// 将 RocksDB 读取的 DataPoint 列表转换为 Apache Arrow RecordBatch,
/// 供 DataFusion 查询引擎或外部系统消费。
pub struct ArrowAdapter;

impl ArrowAdapter {
    /// 范围读取并转换为 Arrow RecordBatch
    ///
    /// 读取指定 measurement 在时间范围 `[start_micros, end_micros]` 内的所有数据点,
    /// 然后按照 `schema` 的列定义转换为 RecordBatch。
    ///
    /// # 参数
    /// - `db` - 时序存储引擎实例
    /// - `measurement` - 指标名 (如 "cpu")
    /// - `start_micros` - 起始时间戳 (微秒)
    /// - `end_micros` - 结束时间戳 (微秒)
    /// - `schema` - 目标 Arrow Schema
    pub fn read_range_arrow(
        db: &TsdbRocksDb,
        measurement: &str,
        start_micros: i64,
        end_micros: i64,
        schema: SchemaRef,
    ) -> Result<arrow::record_batch::RecordBatch> {
        let datapoints = db.read_range(measurement, start_micros, end_micros)?;
        let batch = datapoints_to_record_batch(&datapoints, schema)?;
        Ok(batch)
    }

    /// 前缀扫描并转换为 Arrow RecordBatch
    ///
    /// 读取指定 measurement + tags 组合 (同一 series) 在时间范围内的数据点,
    /// 利用 RocksDB 的前缀迭代器高效扫描。
    ///
    /// # 参数
    /// - `db` - 时序存储引擎实例
    /// - `measurement` - 指标名
    /// - `tags` - 标签键值对 (用于前缀匹配)
    /// - `start_micros` - 起始时间戳 (微秒)
    /// - `end_micros` - 结束时间戳 (微秒)
    /// - `schema` - 目标 Arrow Schema
    pub fn prefix_scan_arrow(
        db: &TsdbRocksDb,
        measurement: &str,
        tags: &tsdb_arrow::schema::Tags,
        start_micros: i64,
        end_micros: i64,
        schema: SchemaRef,
    ) -> Result<arrow::record_batch::RecordBatch> {
        let datapoints = db.prefix_scan(measurement, tags, start_micros, end_micros)?;
        let batch = datapoints_to_record_batch(&datapoints, schema)?;
        Ok(batch)
    }

    /// 生成空的 Arrow RecordBatch
    ///
    /// 当查询结果为空时, 返回符合 schema 的空 RecordBatch,
    /// 避免下游消费者处理 None 的复杂度。
    pub fn read_range_arrow_empty(schema: SchemaRef) -> Result<arrow::record_batch::RecordBatch> {
        let empty: Vec<DataPoint> = Vec::new();
        let batch = datapoints_to_record_batch(&empty, schema)?;
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RocksDbConfig;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use chrono::Datelike;
    use std::sync::Arc;
    use tsdb_arrow::schema::{FieldValue, Tags};

    fn make_tags(host: &str) -> Tags {
        let mut tags = Tags::new();
        tags.insert("host".to_string(), host.to_string());
        tags
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

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("measurement", DataType::Utf8, false),
            Field::new("tags_hash", DataType::UInt64, false),
            Field::new("tag_keys", DataType::new_list(DataType::Utf8, true), false),
            Field::new(
                "tag_values",
                DataType::new_list(DataType::Utf8, true),
                false,
            ),
            Field::new("usage", DataType::Float64, true),
        ]))
    }

    #[test]
    fn test_arrow_adapter_read_range() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags = make_tags("server01");
        let today = today_date();
        let base_ts = ts_for_date(today.year(), today.month(), today.day());

        for i in 0..5i64 {
            let mut fields = tsdb_arrow::schema::Fields::new();
            fields.insert("usage".to_string(), FieldValue::Float(i as f64));
            db.put("cpu", &tags, base_ts + i * 1_000_000, &fields)
                .unwrap();
        }

        let schema = test_schema();
        let batch =
            ArrowAdapter::read_range_arrow(&db, "cpu", base_ts, base_ts + 4_000_000, schema)
                .unwrap();

        assert_eq!(batch.num_rows(), 5);
    }

    #[test]
    fn test_arrow_adapter_prefix_scan() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let tags_a = make_tags("server01");
        let tags_b = make_tags("server02");
        let today = today_date();
        let base_ts = ts_for_date(today.year(), today.month(), today.day());

        for i in 0..3i64 {
            let mut fields = tsdb_arrow::schema::Fields::new();
            fields.insert("usage".to_string(), FieldValue::Float(i as f64));
            db.put("cpu", &tags_a, base_ts + i * 1_000_000, &fields)
                .unwrap();
        }
        for i in 0..2i64 {
            let mut fields = tsdb_arrow::schema::Fields::new();
            fields.insert("usage".to_string(), FieldValue::Float(i as f64 * 10.0));
            db.put("cpu", &tags_b, base_ts + i * 1_000_000, &fields)
                .unwrap();
        }

        let schema = test_schema();
        let batch = ArrowAdapter::prefix_scan_arrow(
            &db,
            "cpu",
            &tags_a,
            base_ts,
            base_ts + 2_000_000,
            schema,
        )
        .unwrap();

        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_arrow_adapter_empty_result() {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();

        let schema = test_schema();
        let batch = ArrowAdapter::read_range_arrow(&db, "cpu", 0, 1000, schema).unwrap();

        assert_eq!(batch.num_rows(), 0);
    }
}
