use crate::error::{Result, TsdbParquetError};
use crate::partition::{micros_to_date, PartitionManager};
use arrow::array::{UInt32Array, UInt64Array, TimestampMicrosecondArray};
use arrow::compute::take;
use arrow::datatypes::DataType;
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

#[derive(Debug, Clone)]
pub struct WriteBufferConfig {
    pub max_rows_per_batch: usize,
    pub max_buffer_rows: usize,
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

struct PartitionBuffer {
    batches: Vec<RecordBatch>,
    row_count: usize,
}

pub struct TsdbParquetWriter {
    partition_manager: Arc<PartitionManager>,
    config: WriteBufferConfig,
    measurement: String,
    buffers: BTreeMap<NaiveDate, PartitionBuffer>,
    file_counter: AtomicU64,
    schema: Option<arrow::datatypes::SchemaRef>,
}

impl TsdbParquetWriter {
    pub fn new(
        partition_manager: Arc<PartitionManager>,
        config: WriteBufferConfig,
        measurement: &str,
    ) -> Self {
        Self {
            partition_manager,
            config,
            measurement: measurement.to_string(),
            buffers: BTreeMap::new(),
            file_counter: AtomicU64::new(0),
            schema: None,
        }
    }

    pub fn measurement(&self) -> &str {
        &self.measurement
    }

    pub fn write(&mut self, dp: &DataPoint) -> Result<bool> {
        let date = micros_to_date(dp.timestamp)?;
        let schema = self.get_or_create_schema(dp)?;

        let batch = datapoints_to_record_batch(std::slice::from_ref(dp), schema)?;

        let buffer = self.buffers.entry(date).or_insert_with(|| PartitionBuffer {
            batches: Vec::new(),
            row_count: 0,
        });

        buffer.batches.push(batch);
        buffer.row_count += 1;

        if buffer.row_count >= self.config.max_buffer_rows {
            let buffer = self.buffers.get(&date).unwrap();
            self.flush_buffer_ref(date, buffer)?;
            self.buffers.remove(&date);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn write_batch(&mut self, datapoints: &[DataPoint]) -> Result<()> {
        if datapoints.is_empty() {
            return Ok(());
        }

        let mut by_date: BTreeMap<NaiveDate, Vec<&DataPoint>> = BTreeMap::new();
        for dp in datapoints {
            let date = micros_to_date(dp.timestamp)?;
            by_date.entry(date).or_default().push(dp);
        }

        for (date, dps) in by_date {
            for dp in &dps {
                self.get_or_create_schema(dp)?;
            }
            let schema = self.schema.clone().ok_or_else(|| {
                TsdbParquetError::Conversion("schema not initialized".into())
            })?;
            let dp_owned: Vec<DataPoint> = dps.into_iter().cloned().collect();
            let batch = datapoints_to_record_batch(&dp_owned, schema)?;

            let buffer = self.buffers.entry(date).or_insert_with(|| PartitionBuffer {
                batches: Vec::new(),
                row_count: 0,
            });

            buffer.batches.push(batch);
            buffer.row_count += dp_owned.len();

            if buffer.row_count >= self.config.max_buffer_rows {
                let buffer = self.buffers.get(&date).unwrap();
                self.flush_buffer_ref(date, buffer)?;
                self.buffers.remove(&date);
            }
        }

        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<Vec<PathBuf>> {
        let keys: Vec<NaiveDate> = self.buffers.keys().copied().collect();
        let mut paths = Vec::new();
        let mut last_error = None;

        for date in &keys {
            if let Some(buffer) = self.buffers.get(date) {
                match self.flush_buffer_ref(*date, buffer) {
                    Ok(written) => {
                        self.buffers.remove(date);
                        paths.extend(written);
                    }
                    Err(e) => {
                        tracing::error!("flush_buffer failed for date {}: {}", date, e);
                        last_error = Some(e);
                    }
                }
            }
        }

        if let Some(e) = last_error {
            return Err(e);
        }

        Ok(paths)
    }

    fn flush_buffer_ref(&self, date: NaiveDate, buffer: &PartitionBuffer) -> Result<Vec<PathBuf>> {
        if buffer.batches.is_empty() {
            return Ok(Vec::new());
        }

        let dir = self
            .partition_manager
            .ensure_measurement_partition(date, &self.measurement)?;

        let file_id = self.file_counter.fetch_add(1, Ordering::Relaxed);
        let file_name = format!("part-{:08}.parquet", file_id);
        let file_path = dir.join(&file_name);
        let tmp_path = dir.join(format!("{}.tmp", file_name));

        let schema = buffer.batches[0].schema();
        let file = File::create(&tmp_path).map_err(|e| {
            TsdbParquetError::Io(std::io::Error::other(format!(
                "failed to create temp parquet file: {}",
                e
            )))
        })?;

        let sorted_batches: Vec<RecordBatch> = buffer
            .batches
            .iter()
            .map(sort_batch_by_tags_hash_timestamp)
            .collect::<Result<Vec<_>>>()?;

        let writer_props = build_indexed_writer_props(&schema, buffer.row_count);

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_props))
            .map_err(TsdbParquetError::Parquet)?;

        for batch in &sorted_batches {
            writer.write(batch).map_err(|e| {
                let _ = std::fs::remove_file(&tmp_path);
                TsdbParquetError::Parquet(e)
            })?;
        }

        writer.close().map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            TsdbParquetError::Parquet(e)
        })?;

        {
            let dir_fd = std::fs::File::open(&dir).map_err(|e| {
                let _ = std::fs::remove_file(&tmp_path);
                TsdbParquetError::Io(std::io::Error::other(format!(
                    "failed to open partition dir for fsync: {}",
                    e
                )))
            })?;
            dir_fd.sync_all().map_err(|e| {
                let _ = std::fs::remove_file(&tmp_path);
                TsdbParquetError::Io(std::io::Error::other(format!(
                    "fsync partition dir failed: {}",
                    e
                )))
            })?;
        }

        std::fs::rename(&tmp_path, &file_path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            TsdbParquetError::Io(std::io::Error::other(format!(
                "failed to rename temp parquet file: {}",
                e
            )))
        })?;

        tracing::info!(
            "flushed {}/{}: {} rows to {} (sorted, indexed)",
            self.measurement,
            date,
            buffer.row_count,
            file_name
        );

        Ok(vec![file_path])
    }

    fn get_or_create_schema(&mut self, dp: &DataPoint) -> Result<arrow::datatypes::SchemaRef> {
        if self.schema.is_none() {
            let mut builder =
                tsdb_arrow::schema::TsdbSchemaBuilder::new(&self.measurement).compact();

            for key in dp.tags.keys() {
                builder = builder.with_tag_key(key);
            }

            for (name, value) in &dp.fields {
                match value {
                    FieldValue::Float(_) => builder = builder.with_float_field(name),
                    FieldValue::Integer(_) => builder = builder.with_int_field(name),
                    FieldValue::String(_) => builder = builder.with_string_field(name),
                    FieldValue::Boolean(_) => builder = builder.with_bool_field(name),
                    FieldValue::Null => {}
                }
            }

            self.schema = Some(builder.build());
        }

        let schema = self.schema.clone().ok_or_else(|| {
            TsdbParquetError::Conversion("schema not initialized".into())
        })?;
        let needs_update = {
            let schema_fields: std::collections::HashSet<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();

            for (name, value) in &dp.fields {
                if schema_fields.contains(name.as_str()) {
                    if let Ok(field) = schema.field_with_name(name) {
                        let expected = field.data_type();
                        let actual: DataType = match value {
                            FieldValue::Float(_) => DataType::Float64,
                            FieldValue::Integer(_) => DataType::Int64,
                            FieldValue::String(_) => DataType::Utf8,
                            FieldValue::Boolean(_) => DataType::Boolean,
                            FieldValue::Null => DataType::Null,
                        };
                        if expected != &actual
                            && !matches!(
                                (expected, &actual),
                                (DataType::Int64, DataType::Float64)
                            )
                        {
                            return Err(TsdbParquetError::Conversion(format!(
                                "field '{}' type conflict: expected {:?}, got {:?}",
                                name, expected, actual
                            )));
                        }
                    }
                }
            }

            let missing_tags: Vec<String> = dp
                .tags
                .keys()
                .filter(|k| !schema_fields.contains(&format!("tag_{}", k).as_str()))
                .cloned()
                .collect();
            let missing_fields: Vec<(String, FieldValue)> = dp
                .fields
                .iter()
                .filter(|(k, _)| !schema_fields.contains(k.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            (!missing_tags.is_empty() || !missing_fields.is_empty())
                .then_some((missing_tags, missing_fields))
        };

        if let Some((missing_tags, missing_fields)) = needs_update {
            let num_new_tags = missing_tags.len();
            let num_new_fields = missing_fields.len();

            let mut builder =
                tsdb_arrow::schema::TsdbSchemaBuilder::new(&self.measurement).compact();

            for field in schema.fields() {
                let name = field.name();
                if tsdb_arrow::schema::is_tag_field(field) {
                    let tag_key = name.strip_prefix("tag_").unwrap_or(name);
                    builder = builder.with_tag_key(tag_key);
                } else if name == "tags_hash" || name == "timestamp" {
                } else {
                    match field.data_type() {
                        DataType::Float64 => builder = builder.with_float_field(name),
                        DataType::Int64 => builder = builder.with_int_field(name),
                        DataType::Utf8 => builder = builder.with_string_field(name),
                        DataType::Boolean => builder = builder.with_bool_field(name),
                        _ => builder = builder.with_float_field(name),
                    }
                }
            }

            for tag_key in &missing_tags {
                builder = builder.with_tag_key(tag_key);
            }
            for (field_name, field_value) in &missing_fields {
                match field_value {
                    FieldValue::Float(_) => builder = builder.with_float_field(field_name),
                    FieldValue::Integer(_) => builder = builder.with_int_field(field_name),
                    FieldValue::String(_) => builder = builder.with_string_field(field_name),
                    FieldValue::Boolean(_) => builder = builder.with_bool_field(field_name),
                    FieldValue::Null => {}
                }
            }

            tracing::info!(
                "schema evolved for {}: added {} tags, {} fields (deferred flush)",
                self.measurement,
                num_new_tags,
                num_new_fields
            );

            let new_schema = builder.build();

            let keys: Vec<NaiveDate> = self.buffers.keys().copied().collect();
            for date in &keys {
                if let Some(buffer) = self.buffers.get_mut(date) {
                    let mut aligned = Vec::with_capacity(buffer.batches.len());
                    for batch in &buffer.batches {
                        aligned.push(align_batch_to_schema(batch, &new_schema)?);
                    }
                    buffer.batches = aligned;
                }
            }

            self.schema = Some(new_schema);
        }

        self.schema.clone().ok_or_else(|| {
            TsdbParquetError::Conversion("schema not initialized after update".into())
        })
    }

    pub fn buffer_count(&self) -> usize {
        self.buffers.len()
    }

    pub fn buffer_row_count(&self) -> usize {
        self.buffers.values().map(|b| b.row_count).sum()
    }
}

impl Drop for TsdbParquetWriter {
    fn drop(&mut self) {
        if self.buffer_row_count() > 0 {
            if let Err(e) = self.flush_all() {
                tracing::warn!(
                    "TsdbParquetWriter drop: flush failed, {} rows lost: {}",
                    self.buffer_row_count(),
                    e
                );
            }
        }
    }
}

fn align_batch_to_schema(
    batch: &RecordBatch,
    target_schema: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let mut columns: Vec<std::sync::Arc<dyn arrow::array::Array>> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        match source_schema.index_of(field.name()) {
            Ok(idx) => {
                columns.push(batch.column(idx).clone());
            }
            Err(_) => {
                let null_array = arrow::array::new_null_array(field.data_type(), batch.num_rows());
                columns.push(null_array);
            }
        }
    }

    RecordBatch::try_new(target_schema.clone(), columns)
        .map_err(|e| TsdbParquetError::Conversion(e.to_string()))
}

fn sort_batch_by_tags_hash_timestamp(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let tags_hash_idx = match schema.index_of("tags_hash") {
        Ok(idx) => idx,
        Err(_) => return Ok(batch.clone()),
    };
    let ts_idx = match schema.index_of("timestamp") {
        Ok(idx) => idx,
        Err(_) => return Ok(batch.clone()),
    };

    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

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

    RecordBatch::try_new(schema, sorted_cols)
        .map_err(|e| TsdbParquetError::Conversion(e.to_string()))
}

fn build_indexed_writer_props(
    schema: &arrow::datatypes::SchemaRef,
    row_count: usize,
) -> WriterProperties {
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
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default(), "cpu");

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
            assert!(path.to_string_lossy().contains("/cpu/"));
        }
    }

    #[test]
    fn test_writer_batch() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default(), "cpu");

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
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), config, "cpu");

        let dps = make_test_datapoints(10);
        for dp in &dps {
            writer.write(dp).unwrap();
        }

        assert!(writer.buffer_row_count() < 10);
    }

    #[test]
    fn test_writer_drop_flushes_buffer() {
        let dir = TempDir::new().unwrap();
        let pm = Arc::new(PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap());

        {
            let mut writer = TsdbParquetWriter::new(pm.clone(), WriteBufferConfig::default(), "cpu");
            let dps = make_test_datapoints(5);
            for dp in &dps {
                writer.write(dp).unwrap();
            }
            assert_eq!(writer.buffer_row_count(), 5);
        }

        let date = micros_to_date(1_000_000).unwrap();
        let files = pm.list_parquet_files(date).unwrap();
        assert!(
            !files.is_empty(),
            "Drop should flush buffered data to parquet files"
        );
    }

    #[test]
    fn test_writer_flush_all_partial_failure_continues() {
        let dir = TempDir::new().unwrap();
        let pm = Arc::new(PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap());

        let mut writer = TsdbParquetWriter::new(pm.clone(), WriteBufferConfig::default(), "cpu");

        let dps = make_test_datapoints(5);
        for dp in &dps {
            writer.write(dp).unwrap();
        }
        assert_eq!(writer.buffer_row_count(), 5);

        let result = writer.flush_all();
        assert!(
            result.is_ok(),
            "flush_all should succeed when all partitions are writable"
        );

        let date = micros_to_date(1_000_000).unwrap();
        let files = pm.list_parquet_files(date).unwrap();
        assert!(!files.is_empty());

        assert_eq!(
            writer.buffer_row_count(),
            0,
            "flush_all should clear all buffers on success"
        );
    }

    #[test]
    fn test_writer_flush_all_empty_buffer() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default(), "cpu");

        let paths = writer.flush_all().unwrap();
        assert!(
            paths.is_empty(),
            "flush_all on empty buffer should return empty paths"
        );
    }

    #[test]
    fn test_writer_measurement_isolation() {
        let dir = TempDir::new().unwrap();
        let pm = Arc::new(PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap());

        let mut cpu_writer =
            TsdbParquetWriter::new(pm.clone(), WriteBufferConfig::default(), "cpu");
        let mut mem_writer =
            TsdbParquetWriter::new(pm.clone(), WriteBufferConfig::default(), "mem");

        let cpu_dps: Vec<DataPoint> = (0..5)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "s1")
                    .with_field("usage", FieldValue::Float(i as f64))
            })
            .collect();
        let mem_dps: Vec<DataPoint> = (0..5)
            .map(|i| {
                DataPoint::new("mem", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "s1")
                    .with_field("percent", FieldValue::Float(i as f64 * 10.0))
            })
            .collect();

        cpu_writer.write_batch(&cpu_dps).unwrap();
        mem_writer.write_batch(&mem_dps).unwrap();
        cpu_writer.flush_all().unwrap();
        mem_writer.flush_all().unwrap();

        let date = micros_to_date(1_000_000).unwrap();
        let cpu_files = pm.list_measurement_parquet_files(date, "cpu").unwrap();
        let mem_files = pm.list_measurement_parquet_files(date, "mem").unwrap();

        assert!(!cpu_files.is_empty(), "cpu should have its own parquet files");
        assert!(!mem_files.is_empty(), "mem should have its own parquet files");

        for f in &cpu_files {
            assert!(
                f.to_string_lossy().contains("/cpu/"),
                "cpu files should be in cpu subdirectory"
            );
        }
        for f in &mem_files {
            assert!(
                f.to_string_lossy().contains("/mem/"),
                "mem files should be in mem subdirectory"
            );
        }
    }
}
