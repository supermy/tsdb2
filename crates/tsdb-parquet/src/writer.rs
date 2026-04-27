use crate::error::{Result, TsdbParquetError};
use crate::partition::{micros_to_date, PartitionManager};
use arrow::array::{UInt32Array, UInt64Array, TimestampMicrosecondArray};
use arrow::compute::take;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::format::SortingColumn;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tsdb_arrow::converter::datapoints_to_record_batch;
use tsdb_arrow::schema::{DataPoint, FieldValue};

/// 写入缓冲区配置
#[derive(Debug, Clone)]
pub struct WriteBufferConfig {
    /// 每批次最大行数, 默认 1024
    pub max_rows_per_batch: usize,
    /// 缓冲区最大行数, 超过则自动刷盘, 默认 100000
    pub max_buffer_rows: usize,
    /// 定时刷盘间隔 (毫秒), 默认 5000
    pub flush_interval_ms: u64,
}

impl Default for WriteBufferConfig {
    fn default() -> Self {
        Self {
            max_rows_per_batch: 1024,
            max_buffer_rows: 100_000,
            flush_interval_ms: 5000,
        }
    }
}

/// 单个日期分区的内存缓冲区
struct PartitionBuffer {
    /// 已缓冲的 RecordBatch
    batches: Vec<RecordBatch>,
    /// 已缓冲的行数
    row_count: usize,
}

/// Parquet 写入器
///
/// 将 DataPoint 按日期分区缓冲, 达到阈值或手动调用 flush 时写入 Parquet 文件。
/// 写入流程:
/// 1. DataPoint → RecordBatch (通过 tsdb-arrow 转换)
/// 2. 按日期分区缓冲
/// 3. 缓冲区满或 flush 时, 选择 hot/cold 编码写入 Parquet 文件
pub struct TsdbParquetWriter {
    partition_manager: Arc<PartitionManager>,
    config: WriteBufferConfig,
    /// 日期分区 → 缓冲区
    buffers: BTreeMap<NaiveDate, PartitionBuffer>,
    /// 文件计数器 (原子递增, 用于生成文件名)
    file_counter: AtomicU64,
    /// 缓存的 Schema (由第一个 DataPoint 推断)
    schema: Option<arrow::datatypes::SchemaRef>,
}

impl TsdbParquetWriter {
    /// 创建写入器
    pub fn new(partition_manager: Arc<PartitionManager>, config: WriteBufferConfig) -> Self {
        Self {
            partition_manager,
            config,
            buffers: BTreeMap::new(),
            file_counter: AtomicU64::new(0),
            schema: None,
        }
    }

    /// 写入单个数据点
    ///
    /// 返回 true 表示触发了自动刷盘 (缓冲区满)
    pub fn write(&mut self, dp: &DataPoint) -> Result<bool> {
        let date = micros_to_date(dp.timestamp);
        let schema = self.get_or_create_schema(dp)?;

        let batch = datapoints_to_record_batch(std::slice::from_ref(dp), schema)?;

        let buffer = self.buffers.entry(date).or_insert_with(|| PartitionBuffer {
            batches: Vec::new(),
            row_count: 0,
        });

        buffer.batches.push(batch);
        buffer.row_count += 1;

        if buffer.row_count >= self.config.max_buffer_rows {
            let buffer = self.buffers.remove(&date).unwrap();
            self.flush_buffer(date, buffer)?;
            return Ok(true);
        }

        Ok(false)
    }

    /// 批量写入数据点
    ///
    /// 按日期分组, 逐组转换为 RecordBatch 并缓冲
    pub fn write_batch(&mut self, datapoints: &[DataPoint]) -> Result<()> {
        if datapoints.is_empty() {
            return Ok(());
        }

        let mut by_date: BTreeMap<NaiveDate, Vec<&DataPoint>> = BTreeMap::new();
        for dp in datapoints {
            let date = micros_to_date(dp.timestamp);
            by_date.entry(date).or_default().push(dp);
        }

        for (date, dps) in by_date {
            let schema = self.get_or_create_schema(dps[0])?;
            let dp_owned: Vec<DataPoint> = dps.into_iter().cloned().collect();
            let batch = datapoints_to_record_batch(&dp_owned, schema)?;

            let buffer = self.buffers.entry(date).or_insert_with(|| PartitionBuffer {
                batches: Vec::new(),
                row_count: 0,
            });

            buffer.batches.push(batch);
            buffer.row_count += dp_owned.len();
        }

        Ok(())
    }

    /// 刷新所有缓冲区, 返回写入的文件路径列表
    pub fn flush_all(&mut self) -> Result<Vec<PathBuf>> {
        let keys: Vec<NaiveDate> = self.buffers.keys().copied().collect();
        let mut paths = Vec::new();

        for date in keys {
            if let Some(buffer) = self.buffers.remove(&date) {
                let written = self.flush_buffer(date, buffer)?;
                paths.extend(written);
            }
        }

        Ok(paths)
    }

    /// 将单个分区的缓冲区写入 Parquet 文件
    ///
    /// 行数 ≤ 10000 使用 hot 编码 (SNAPPY), 否则使用 cold 编码 (ZSTD)
    fn flush_buffer(&self, date: NaiveDate, buffer: PartitionBuffer) -> Result<Vec<PathBuf>> {
        if buffer.batches.is_empty() {
            return Ok(Vec::new());
        }

        let dir = self.partition_manager.ensure_partition(date)?;

        let file_id = self.file_counter.fetch_add(1, Ordering::Relaxed);
        let file_name = format!("part-{:08}.parquet", file_id);
        let file_path = dir.join(&file_name);

        let schema = buffer.batches[0].schema();
        let file = File::create(&file_path).map_err(|e| {
            TsdbParquetError::Io(std::io::Error::other(format!(
                "failed to create parquet file: {}",
                e
            )))
        })?;

        let sorted_batches: Vec<RecordBatch> = buffer
            .batches
            .into_iter()
            .map(|b| sort_batch_by_tags_hash_timestamp(&b))
            .collect::<Result<Vec<_>>>()?;

        let writer_props = build_indexed_writer_props(&schema, buffer.row_count);

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_props))
            .map_err(TsdbParquetError::Parquet)?;

        for batch in &sorted_batches {
            writer.write(batch).map_err(TsdbParquetError::Parquet)?;
        }

        writer.close().map_err(TsdbParquetError::Parquet)?;

        tracing::info!(
            "flushed partition {}: {} rows to {} (sorted, indexed)",
            date,
            buffer.row_count,
            file_name
        );

        Ok(vec![file_path])
    }

    /// 从第一个 DataPoint 推断 Schema, 后续复用
    fn get_or_create_schema(&mut self, dp: &DataPoint) -> Result<arrow::datatypes::SchemaRef> {
        if self.schema.is_none() {
            let mut builder = tsdb_arrow::schema::TsdbSchemaBuilder::new(&dp.measurement).compact();

            for key in dp.tags.keys() {
                builder = builder.with_tag_key(key);
            }

            for (name, value) in &dp.fields {
                match value {
                    FieldValue::Float(_) => builder = builder.with_float_field(name),
                    FieldValue::Integer(_) => builder = builder.with_int_field(name),
                    FieldValue::String(_) => builder = builder.with_string_field(name),
                    FieldValue::Boolean(_) => builder = builder.with_bool_field(name),
                }
            }

            self.schema = Some(builder.build());
        }

        Ok(self.schema.clone().unwrap())
    }

    /// 获取缓冲区中的分区数量
    pub fn buffer_count(&self) -> usize {
        self.buffers.len()
    }

    /// 获取缓冲区中的总行数
    pub fn buffer_row_count(&self) -> usize {
        self.buffers.values().map(|b| b.row_count).sum()
    }
}

fn sort_batch_by_tags_hash_timestamp(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let tags_hash_idx = schema.index_of("tags_hash").map_err(|_| {
        TsdbParquetError::Conversion("tags_hash column not found for sorting".into())
    })?;
    let ts_idx = schema.index_of("timestamp").map_err(|_| {
        TsdbParquetError::Conversion("timestamp column not found for sorting".into())
    })?;

    let tags_hash_col = batch
        .column(tags_hash_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            TsdbParquetError::Conversion("tags_hash column is not UInt64".into())
        })?;
    let ts_col = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            TsdbParquetError::Conversion("timestamp column is not TimestampMicrosecond".into())
        })?;

    let mut indices: Vec<usize> = (0..batch.num_rows()).collect();
    indices.sort_by(|&a, &b| {
        tags_hash_col
            .value(a)
            .cmp(&tags_hash_col.value(b))
            .then_with(|| ts_col.value(a).cmp(&ts_col.value(b)))
    });

    let index_array = UInt32Array::from_iter(indices.iter().map(|&i| i as u32));
    let sorted_cols: Vec<std::sync::Arc<dyn arrow::array::Array>> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(col_idx, _)| {
            take(batch.column(col_idx), &index_array, None)
                .map_err(|e| TsdbParquetError::Conversion(e.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(RecordBatch::try_new(schema, sorted_cols)
        .map_err(|e| TsdbParquetError::Conversion(e.to_string()))?)
}

fn build_indexed_writer_props(schema: &arrow::datatypes::SchemaRef, row_count: usize) -> WriterProperties {
    let compression = if row_count <= 10000 {
        parquet::basic::Compression::SNAPPY
    } else {
        parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default())
    };

    let max_row_group_size = if row_count <= 10000 {
        1024 * 1024
    } else {
        64 * 1024
    };

    let mut builder = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(max_row_group_size)
        .set_statistics_enabled(EnabledStatistics::Page);

    if let Ok(tags_hash_idx) = schema.index_of("tags_hash") {
        if let Ok(ts_idx) = schema.index_of("timestamp") {
            builder = builder.set_sorting_columns(Some(vec![
                SortingColumn::new(tags_hash_idx as i32, false, false),
                SortingColumn::new(ts_idx as i32, false, false),
            ]));
        }
    }

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::PartitionConfig;
    use tempfile::TempDir;
    use tsdb_arrow::schema::FieldValue;

    fn make_test_datapoints(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_tag("region", "us-west")
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.01))
                    .with_field("count", FieldValue::Integer(i as i64 * 10))
            })
            .collect()
    }

    #[test]
    fn test_writer_basic() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default());

        let dps = make_test_datapoints(10);
        for dp in &dps {
            writer.write(dp).unwrap();
        }

        assert_eq!(writer.buffer_row_count(), 10);

        let paths = writer.flush_all().unwrap();
        assert!(!paths.is_empty());

        for path in &paths {
            assert!(path.exists());
            assert!(path.extension().unwrap() == "parquet");
        }
    }

    #[test]
    fn test_writer_batch() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default());

        let dps = make_test_datapoints(100);
        writer.write_batch(&dps).unwrap();

        let paths = writer.flush_all().unwrap();
        assert!(!paths.is_empty());
    }

    #[test]
    fn test_writer_auto_flush() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let config = WriteBufferConfig {
            max_buffer_rows: 5,
            ..Default::default()
        };
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), config);

        let dps = make_test_datapoints(10);
        for dp in &dps {
            writer.write(dp).unwrap();
        }

        assert!(writer.buffer_row_count() < 10);
    }
}
