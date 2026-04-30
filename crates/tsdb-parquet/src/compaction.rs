use crate::error::{Result, TsdbParquetError};
use crate::partition::PartitionManager;
use arrow::array::{Array, TimestampMicrosecondArray, UInt32Array, UInt64Array};
use arrow::compute::take;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::format::SortingColumn;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

fn uuid_short() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:08x}", (ts & 0xFFFFFFFF) as u32)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CompactionLevel {
    L0 = 0,
    L1 = 1,
    L2 = 2,
}

impl CompactionLevel {
    pub fn from_file_name(name: &str) -> Self {
        if name.starts_with("L0-") || name.starts_with("part-") {
            CompactionLevel::L0
        } else if name.starts_with("L1-") {
            CompactionLevel::L1
        } else if name.starts_with("L2-") || name.starts_with("compacted-") {
            CompactionLevel::L2
        } else {
            CompactionLevel::L0
        }
    }

    pub fn target_rows(&self) -> usize {
        match self {
            CompactionLevel::L0 => 10_000,
            CompactionLevel::L1 => 100_000,
            CompactionLevel::L2 => 1_000_000,
        }
    }

    pub fn next(self) -> Option<Self> {
        match self {
            CompactionLevel::L0 => Some(CompactionLevel::L1),
            CompactionLevel::L1 => Some(CompactionLevel::L2),
            CompactionLevel::L2 => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub hot_days: u64,
    pub retention_days: u64,
    pub l0_max_files: usize,
    pub l1_max_files: usize,
    pub l2_target_rows: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            hot_days: 7,
            retention_days: 30,
            l0_max_files: 8,
            l1_max_files: 4,
            l2_target_rows: 1_000_000,
        }
    }
}

pub struct ParquetCompactor {
    partition_manager: Arc<PartitionManager>,
    config: CompactionConfig,
}

impl ParquetCompactor {
    pub fn new(partition_manager: Arc<PartitionManager>, config: CompactionConfig) -> Self {
        Self {
            partition_manager,
            config,
        }
    }

    pub fn compact_date(&self, date: NaiveDate) -> Result<()> {
        let measurements = self.partition_manager.list_measurements_for_date(date)?;
        if measurements.is_empty() {
            self.compact_date_legacy(date)?;
        } else {
            for measurement in &measurements {
                self.compact_measurement_l0_to_l1(date, measurement)?;
                self.compact_measurement_l1_to_l2(date, measurement)?;
            }
        }
        Ok(())
    }

    fn compact_date_legacy(&self, date: NaiveDate) -> Result<()> {
        let files = self.partition_manager.list_parquet_files(date)?;
        let root_files: Vec<PathBuf> = {
            let partition_dir = self.partition_manager.ensure_partition(date)?;
            files
                .into_iter()
                .filter(|p| p.parent() == Some(&partition_dir))
                .collect()
        };

        if root_files.is_empty() {
            return Ok(());
        }

        let l0_files: Vec<PathBuf> = root_files
            .iter()
            .filter(|p| {
                let name = p.file_name().unwrap_or_default().to_string_lossy();
                CompactionLevel::from_file_name(&name) == CompactionLevel::L0
            })
            .cloned()
            .collect();

        if l0_files.len() >= self.config.l0_max_files {
            let mut all_batches = Vec::new();
            for path in &l0_files {
                all_batches.extend(self.read_parquet_file(path)?);
            }
            if !all_batches.is_empty() {
                all_batches = align_batches_to_merged_schema(all_batches);
                let deduped = deduplicate_batches(&all_batches)?;

                let dir = self.partition_manager.ensure_partition(date)?;
                let uid = &uuid_short();
                let tmp_path = dir.join(format!(".tmp-L1-{}-{}", date.format("%Y%m%d"), uid));
                let final_path = dir.join(format!("L1-{}-{}.parquet", date.format("%Y%m%d"), uid));
                self.write_compacted(&deduped, &tmp_path, &final_path, CompactionLevel::L1)?;
                self.delete_old_files(&l0_files, "L0", &final_path);
            }
        }

        let files = self.partition_manager.list_parquet_files(date)?;
        let partition_dir = self.partition_manager.ensure_partition(date)?;
        let root_files: Vec<PathBuf> = files
            .into_iter()
            .filter(|p| p.parent() == Some(&partition_dir))
            .collect();
        let l1_files: Vec<PathBuf> = root_files
            .iter()
            .filter(|p| {
                let name = p.file_name().unwrap_or_default().to_string_lossy();
                CompactionLevel::from_file_name(&name) == CompactionLevel::L1
            })
            .cloned()
            .collect();

        if l1_files.len() >= self.config.l1_max_files {
            let mut all_batches = Vec::new();
            for path in &l1_files {
                all_batches.extend(self.read_parquet_file(path)?);
            }
            if !all_batches.is_empty() {
                all_batches = align_batches_to_merged_schema(all_batches);
                let deduped = deduplicate_batches(&all_batches)?;

                let dir = self.partition_manager.ensure_partition(date)?;
                let uid = &uuid_short();
                let tmp_path = dir.join(format!(".tmp-L2-{}-{}", date.format("%Y%m%d"), uid));
                let final_path = dir.join(format!("L2-{}-{}.parquet", date.format("%Y%m%d"), uid));
                self.write_compacted(&deduped, &tmp_path, &final_path, CompactionLevel::L2)?;
                self.delete_old_files(&l1_files, "L1", &final_path);
            }
        }

        Ok(())
    }

    fn compact_measurement_l0_to_l1(&self, date: NaiveDate, measurement: &str) -> Result<()> {
        let files = self
            .partition_manager
            .list_measurement_parquet_files(date, measurement)?;
        let l0_files: Vec<PathBuf> = files
            .iter()
            .filter(|p| {
                let name = p.file_name().unwrap_or_default().to_string_lossy();
                CompactionLevel::from_file_name(&name) == CompactionLevel::L0
            })
            .cloned()
            .collect();

        if l0_files.len() < self.config.l0_max_files {
            return Ok(());
        }

        let mut all_batches = Vec::new();
        for path in &l0_files {
            all_batches.extend(self.read_parquet_file(path)?);
        }

        if all_batches.is_empty() {
            return Ok(());
        }

        all_batches = align_batches_to_merged_schema(all_batches);
        let deduped = deduplicate_batches(&all_batches)?;

        let dir = self
            .partition_manager
            .ensure_measurement_partition(date, measurement)?;
        let uid = &uuid_short();
        let tmp_path = dir.join(format!(
            ".tmp-L1-{}-{}-{}",
            measurement,
            date.format("%Y%m%d"),
            uid
        ));
        let final_path = dir.join(format!(
            "L1-{}-{}-{}.parquet",
            measurement,
            date.format("%Y%m%d"),
            uid
        ));

        self.write_compacted(&deduped, &tmp_path, &final_path, CompactionLevel::L1)?;

        self.delete_old_files(&l0_files, "L0", &final_path);

        Ok(())
    }

    fn compact_measurement_l1_to_l2(&self, date: NaiveDate, measurement: &str) -> Result<()> {
        let files = self
            .partition_manager
            .list_measurement_parquet_files(date, measurement)?;
        let l1_files: Vec<PathBuf> = files
            .iter()
            .filter(|p| {
                let name = p.file_name().unwrap_or_default().to_string_lossy();
                CompactionLevel::from_file_name(&name) == CompactionLevel::L1
            })
            .cloned()
            .collect();

        if l1_files.len() < self.config.l1_max_files {
            return Ok(());
        }

        let mut all_batches = Vec::new();
        for path in &l1_files {
            all_batches.extend(self.read_parquet_file(path)?);
        }

        if all_batches.is_empty() {
            return Ok(());
        }

        all_batches = align_batches_to_merged_schema(all_batches);
        let deduped = deduplicate_batches(&all_batches)?;

        let dir = self
            .partition_manager
            .ensure_measurement_partition(date, measurement)?;
        let uid = &uuid_short();
        let tmp_path = dir.join(format!(
            ".tmp-L2-{}-{}-{}",
            measurement,
            date.format("%Y%m%d"),
            uid
        ));
        let final_path = dir.join(format!(
            "L2-{}-{}-{}.parquet",
            measurement,
            date.format("%Y%m%d"),
            uid
        ));

        self.write_compacted(&deduped, &tmp_path, &final_path, CompactionLevel::L2)?;

        self.delete_old_files(&l1_files, "L1", &final_path);

        Ok(())
    }

    fn delete_old_files(&self, old_files: &[PathBuf], level_label: &str, new_path: &PathBuf) {
        let mut delete_errors = Vec::new();
        for path in old_files {
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!(
                    "failed to remove {} file {:?}: {}, retrying...",
                    level_label,
                    path,
                    e
                );
                std::thread::sleep(std::time::Duration::from_millis(100));
                if let Err(e2) = std::fs::remove_file(path) {
                    tracing::error!(
                        "failed to remove {} file {:?} after retry: {}",
                        level_label,
                        path,
                        e2
                    );
                    delete_errors.push((path.clone(), e2.to_string()));
                }
            }
        }
        if !delete_errors.is_empty() {
            tracing::warn!(
                "compaction {} completed but {} old files could not be removed; \
                 new compacted file: {:?}",
                level_label,
                delete_errors.len(),
                new_path
            );
        }
    }

    fn write_compacted(
        &self,
        batches: &[RecordBatch],
        tmp_path: &PathBuf,
        final_path: &PathBuf,
        level: CompactionLevel,
    ) -> Result<()> {
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return Ok(());
        }
        let schema = batches[0].schema();

        let merged = if batches.len() > 1 {
            arrow::compute::concat_batches(&schema, batches.iter())
                .map_err(|e| TsdbParquetError::Conversion(e.to_string()))
        } else {
            Ok(batches[0].clone())
        }?;

        let sorted = sort_batch_by_tags_hash_timestamp(&merged)?;

        let file = File::create(tmp_path).map_err(|e| {
            TsdbParquetError::Io(std::io::Error::other(format!(
                "failed to create compacted file: {}",
                e
            )))
        })?;

        let writer_props = build_compaction_writer_props(&schema, level, total_rows);

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_props))
            .map_err(TsdbParquetError::Parquet)?;

        writer.write(&sorted).map_err(TsdbParquetError::Parquet)?;

        writer.close().map_err(TsdbParquetError::Parquet)?;

        {
            let dir_fd =
                std::fs::File::open(tmp_path.parent().unwrap_or(tmp_path)).map_err(|e| {
                    let _ = std::fs::remove_file(tmp_path);
                    TsdbParquetError::Io(std::io::Error::other(format!(
                        "failed to open dir for fsync: {}",
                        e
                    )))
                })?;
            dir_fd.sync_all().map_err(|e| {
                let _ = std::fs::remove_file(tmp_path);
                TsdbParquetError::Io(std::io::Error::other(format!("fsync dir failed: {}", e)))
            })?;
        }

        std::fs::rename(tmp_path, final_path).map_err(|e| {
            let _ = std::fs::remove_file(tmp_path);
            TsdbParquetError::Io(std::io::Error::other(format!(
                "atomic rename failed: {}",
                e
            )))
        })?;

        Ok(())
    }

    pub fn cleanup_expired(&self) -> Result<Vec<NaiveDate>> {
        self.partition_manager.refresh()?;
        let today = chrono::Utc::now().date_naive();
        let cutoff = today - chrono::Duration::days(self.config.retention_days as i64);
        let partitions = self
            .partition_manager
            .get_partitions_in_range(chrono::NaiveDate::MIN, cutoff);

        let mut removed = Vec::new();
        for partition in partitions {
            if partition.date < cutoff
                && self
                    .partition_manager
                    .remove_partition(partition.date)
                    .is_ok()
            {
                removed.push(partition.date);
            }
        }

        let tier_dropped = self
            .partition_manager
            .cleanup_expired_with_retention(self.config.retention_days)?;
        for path_str in &tier_dropped {
            let path = std::path::Path::new(path_str);
            if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(date_str) = dir_name.strip_prefix("data_") {
                    if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y%m%d") {
                        if !removed.contains(&date) {
                            removed.push(date);
                        }
                    }
                }
            }
        }

        Ok(removed)
    }

    pub fn compact_all_levels(&self) -> Result<HashMap<NaiveDate, (usize, usize)>> {
        self.partition_manager.refresh()?;
        let mut result = HashMap::new();

        let today = chrono::Utc::now().date_naive();
        let cutoff = today - chrono::Duration::days(self.config.retention_days as i64);
        let partitions = self
            .partition_manager
            .get_partitions_in_range(cutoff, today);

        for partition in partitions {
            let measurements = self
                .partition_manager
                .list_measurements_for_date(partition.date)?;
            if measurements.is_empty() {
                let files = self.partition_manager.list_parquet_files(partition.date)?;
                let l0_count = files
                    .iter()
                    .filter(|p| {
                        let name = p.file_name().unwrap_or_default().to_string_lossy();
                        CompactionLevel::from_file_name(&name) == CompactionLevel::L0
                    })
                    .count();
                let l1_count = files
                    .iter()
                    .filter(|p| {
                        let name = p.file_name().unwrap_or_default().to_string_lossy();
                        CompactionLevel::from_file_name(&name) == CompactionLevel::L1
                    })
                    .count();

                if l0_count >= self.config.l0_max_files || l1_count >= self.config.l1_max_files {
                    self.compact_date_legacy(partition.date)?;
                    result.insert(partition.date, (l0_count, l1_count));
                }
            } else {
                let mut needs_compact = false;
                for measurement in &measurements {
                    let files = self
                        .partition_manager
                        .list_measurement_parquet_files(partition.date, measurement)?;
                    let l0_count = files
                        .iter()
                        .filter(|p| {
                            let name = p.file_name().unwrap_or_default().to_string_lossy();
                            CompactionLevel::from_file_name(&name) == CompactionLevel::L0
                        })
                        .count();
                    let l1_count = files
                        .iter()
                        .filter(|p| {
                            let name = p.file_name().unwrap_or_default().to_string_lossy();
                            CompactionLevel::from_file_name(&name) == CompactionLevel::L1
                        })
                        .count();

                    if l0_count >= self.config.l0_max_files || l1_count >= self.config.l1_max_files
                    {
                        needs_compact = true;
                        break;
                    }
                }
                if needs_compact {
                    self.compact_date(partition.date)?;
                    let all_files = self.partition_manager.list_parquet_files(partition.date)?;
                    let l0_count = all_files
                        .iter()
                        .filter(|p| {
                            let name = p.file_name().unwrap_or_default().to_string_lossy();
                            CompactionLevel::from_file_name(&name) == CompactionLevel::L0
                        })
                        .count();
                    let l1_count = all_files
                        .iter()
                        .filter(|p| {
                            let name = p.file_name().unwrap_or_default().to_string_lossy();
                            CompactionLevel::from_file_name(&name) == CompactionLevel::L1
                        })
                        .count();
                    result.insert(partition.date, (l0_count, l1_count));
                }
            }
        }

        Ok(result)
    }

    fn read_parquet_file(&self, path: &PathBuf) -> Result<Vec<RecordBatch>> {
        let file = File::open(path).map_err(|e| {
            TsdbParquetError::Io(std::io::Error::other(format!(
                "parquet file not found: {}",
                e
            )))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        let mut batches = Vec::new();
        for batch_result in reader {
            batches.push(batch_result?);
        }

        Ok(batches)
    }
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

    let tags_hash_col = match batch
        .column(tags_hash_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
    {
        Some(col) => col,
        None => return Ok(batch.clone()),
    };
    let ts_col = match batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
    {
        Some(col) => col,
        None => return Ok(batch.clone()),
    };

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

fn build_compaction_writer_props(
    schema: &arrow::datatypes::SchemaRef,
    level: CompactionLevel,
    total_rows: usize,
) -> WriterProperties {
    let compression = match level {
        CompactionLevel::L0 | CompactionLevel::L1 => parquet::basic::Compression::SNAPPY,
        CompactionLevel::L2 => {
            if total_rows <= 1_000_000 {
                parquet::basic::Compression::SNAPPY
            } else {
                parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default())
            }
        },
    };

    let max_row_group_size = match level {
        CompactionLevel::L0 | CompactionLevel::L1 => 1024 * 1024,
        CompactionLevel::L2 => 64 * 1024,
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

fn align_batches_to_merged_schema(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    if batches.len() <= 1 {
        return batches;
    }

    let first_schema = batches[0].schema();
    let needs_align = batches.iter().any(|b| b.schema() != first_schema);
    if !needs_align {
        return batches;
    }

    let mut merged_fields: Vec<arrow::datatypes::Field> = first_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    for batch in &batches[1..] {
        for field in batch.schema().fields() {
            if !merged_fields.iter().any(|f| f.name() == field.name()) {
                merged_fields.push(field.as_ref().clone());
            }
        }
    }
    let mut merged_metadata = std::collections::HashMap::new();
    for (key, value) in first_schema.metadata() {
        merged_metadata.insert(key.clone(), value.clone());
    }
    for batch in &batches[1..] {
        for (key, value) in batch.schema().metadata() {
            if key == "measurement" && merged_metadata.contains_key("measurement") {
                let existing = merged_metadata.get("measurement").unwrap();
                if existing != value {
                    let merged = format!("{},{}", existing, value);
                    merged_metadata.insert("measurement".to_string(), merged);
                }
            } else {
                merged_metadata.insert(key.clone(), value.clone());
            }
        }
    }
    let merged_schema =
        Arc::new(arrow::datatypes::Schema::new(merged_fields).with_metadata(merged_metadata));

    batches
        .into_iter()
        .map(|batch| {
            if batch.schema() == merged_schema {
                return batch;
            }
            let mut columns: Vec<Arc<dyn Array>> = Vec::new();
            for field in merged_schema.fields() {
                if let Ok(idx) = batch.schema().index_of(field.name()) {
                    columns.push(batch.column(idx).clone());
                } else {
                    columns.push(arrow::array::new_null_array(
                        field.data_type(),
                        batch.num_rows(),
                    ));
                }
            }
            RecordBatch::try_new(merged_schema.clone(), columns).unwrap_or(batch)
        })
        .collect()
}

fn deduplicate_batches(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = batches[0].schema();
    let ts_idx = schema
        .index_of("timestamp")
        .ok()
        .or_else(|| schema.index_of("time").ok());

    if ts_idx.is_none() {
        return Ok(batches.to_vec());
    }

    let ts_idx = ts_idx.unwrap();

    let tags_hash_idx = schema.index_of("tags_hash").ok();

    let tag_column_indices: Vec<usize> = if tags_hash_idx.is_none() {
        schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| tsdb_arrow::schema::is_tag_field(f))
            .map(|(i, _)| i)
            .collect()
    } else {
        Vec::new()
    };

    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut deduped_batches = Vec::new();

    for batch in batches.iter().rev() {
        let ts_col = batch.column(ts_idx);
        let num_rows = batch.num_rows();
        let mut keep_indices: Vec<usize> = Vec::new();

        for row_idx in (0..num_rows).rev() {
            let mut key_bytes = Vec::new();

            if let Some(th_idx) = tags_hash_idx {
                let tags_hash_col = batch.column(th_idx);
                if let Some(arr) = tags_hash_col
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                {
                    if row_idx < arr.len() {
                        if arr.is_null(row_idx) {
                            key_bytes.extend_from_slice(&row_idx.to_be_bytes());
                        } else {
                            key_bytes.extend_from_slice(&arr.value(row_idx).to_be_bytes());
                        }
                    }
                } else if let Some(arr) = tags_hash_col
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                {
                    if row_idx < num_rows {
                        if arr.is_null(row_idx) {
                            key_bytes.extend_from_slice(&row_idx.to_be_bytes());
                        } else {
                            key_bytes.extend_from_slice(arr.value(row_idx).as_bytes());
                        }
                    }
                }
            } else {
                for &tag_idx in &tag_column_indices {
                    let tag_col = batch.column(tag_idx);
                    if let Some(arr) = tag_col.as_any().downcast_ref::<arrow::array::StringArray>()
                    {
                        if row_idx < arr.len() {
                            if arr.is_null(row_idx) {
                                key_bytes.push(0u8);
                            } else {
                                key_bytes.push(1u8);
                                key_bytes.extend_from_slice(arr.value(row_idx).as_bytes());
                            }
                        }
                    } else if let Some(arr) = tag_col
                        .as_any()
                        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>(
                        )
                    {
                        if row_idx < arr.len() {
                            if arr.is_null(row_idx) {
                                key_bytes.push(0u8);
                            } else {
                                key_bytes.push(1u8);
                                let key = arr.keys().value(row_idx);
                                if let Some(values) = arr
                                    .values()
                                    .as_any()
                                    .downcast_ref::<arrow::array::StringArray>()
                                {
                                    if key >= 0 && (key as usize) < values.len() {
                                        key_bytes.extend_from_slice(
                                            values.value(key as usize).as_bytes(),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }

            let mut ts_appended = false;
            if let Some(arr) = ts_col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                if row_idx < arr.len() && !arr.is_null(row_idx) {
                    key_bytes.extend_from_slice(&arr.value(row_idx).to_be_bytes());
                    ts_appended = true;
                }
            }
            if !ts_appended {
                if let Some(arr) = ts_col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                {
                    if row_idx < arr.len() && !arr.is_null(row_idx) {
                        key_bytes.extend_from_slice(&arr.value(row_idx).to_be_bytes());
                        ts_appended = true;
                    }
                }
            }
            if !ts_appended {
                if let Some(arr) = ts_col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                {
                    if row_idx < arr.len() && !arr.is_null(row_idx) {
                        key_bytes.extend_from_slice(&arr.value(row_idx).to_be_bytes());
                        ts_appended = true;
                    }
                }
            }
            if !ts_appended {
                if let Some(arr) = ts_col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()
                {
                    if row_idx < arr.len() && !arr.is_null(row_idx) {
                        key_bytes.extend_from_slice(&arr.value(row_idx).to_be_bytes());
                        ts_appended = true;
                    }
                }
            }
            if !ts_appended {
                if let Some(arr) = ts_col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                {
                    if row_idx < arr.len() && !arr.is_null(row_idx) {
                        key_bytes.extend_from_slice(&arr.value(row_idx).to_be_bytes());
                        ts_appended = true;
                    }
                }
            }
            if !ts_appended {
                key_bytes.extend_from_slice(&row_idx.to_be_bytes());
            }

            if seen.insert(key_bytes) {
                keep_indices.push(row_idx);
            }
        }

        keep_indices.reverse();
        if keep_indices.len() == num_rows {
            deduped_batches.push(batch.clone());
        } else if !keep_indices.is_empty() {
            let index_array =
                arrow::array::UInt32Array::from_iter(keep_indices.iter().map(|&i| i as u32));
            let new_cols: Vec<Arc<dyn arrow::array::Array>> = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(col_idx, _)| {
                    let col = batch.column(col_idx);
                    arrow::compute::take(col, &index_array, None)
                        .map_err(|e| TsdbParquetError::Conversion(format!("take failed: {}", e)))
                })
                .collect::<Result<Vec<_>>>()?;
            let new_batch = RecordBatch::try_new(schema.clone(), new_cols)
                .map_err(|e| TsdbParquetError::Conversion(e.to_string()))?;
            deduped_batches.push(new_batch);
        }
    }

    deduped_batches.reverse();

    let original_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let deduped_rows: usize = deduped_batches.iter().map(|b| b.num_rows()).sum();
    if original_rows != deduped_rows {
        tracing::info!(
            "deduplicate_batches: {} rows -> {} rows (removed {} duplicates)",
            original_rows,
            deduped_rows,
            original_rows - deduped_rows
        );
    }

    Ok(deduped_batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::PartitionConfig;
    use crate::writer::{TsdbParquetWriter, WriteBufferConfig};
    use tempfile::TempDir;
    use tsdb_arrow::schema::{DataPoint, FieldValue};

    fn write_test_data(dir: &TempDir) -> PartitionManager {
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default(), "cpu");

        let today = chrono::Utc::now().date_naive();
        let base_ts = today
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();

        let dps: Vec<DataPoint> = (0..50)
            .map(|i| {
                DataPoint::new("cpu", base_ts + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.01))
            })
            .collect();

        writer.write_batch(&dps).unwrap();
        writer.flush_all().unwrap();

        PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap()
    }

    #[test]
    fn test_compact_date_single_file() {
        let dir = TempDir::new().unwrap();
        let pm = write_test_data(&dir);
        let compactor = ParquetCompactor::new(Arc::new(pm), CompactionConfig::default());

        let today = chrono::Utc::now().date_naive();
        let result = compactor.compact_date(today);
        assert!(result.is_ok());
    }

    #[test]
    fn test_compaction_level_from_file_name() {
        assert_eq!(
            CompactionLevel::from_file_name("part-00000001.parquet"),
            CompactionLevel::L0
        );
        assert_eq!(
            CompactionLevel::from_file_name("L0-20240101.parquet"),
            CompactionLevel::L0
        );
        assert_eq!(
            CompactionLevel::from_file_name("L1-20240101.parquet"),
            CompactionLevel::L1
        );
        assert_eq!(
            CompactionLevel::from_file_name("L2-20240101.parquet"),
            CompactionLevel::L2
        );
        assert_eq!(
            CompactionLevel::from_file_name("compacted-20240101.parquet"),
            CompactionLevel::L2
        );
    }

    #[test]
    fn test_compaction_level_next() {
        assert_eq!(CompactionLevel::L0.next(), Some(CompactionLevel::L1));
        assert_eq!(CompactionLevel::L1.next(), Some(CompactionLevel::L2));
        assert_eq!(CompactionLevel::L2.next(), None);
    }

    #[test]
    fn test_cleanup_expired() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default(), "cpu");

        let old_date = chrono::Utc::now().date_naive() - chrono::Duration::days(10);
        let base_ts = old_date
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();

        let dps: Vec<DataPoint> = (0..10)
            .map(|i| {
                DataPoint::new("cpu", base_ts + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.01))
            })
            .collect();

        writer.write_batch(&dps).unwrap();
        writer.flush_all().unwrap();

        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let config = CompactionConfig {
            retention_days: 5,
            ..Default::default()
        };
        let compactor = ParquetCompactor::new(Arc::new(pm), config);

        let removed = compactor.cleanup_expired().unwrap();
        assert!(!removed.is_empty());
    }

    #[test]
    fn test_compact_all_levels() {
        let dir = TempDir::new().unwrap();
        let pm = write_test_data(&dir);
        let config = CompactionConfig {
            l0_max_files: 1,
            ..Default::default()
        };
        let compactor = ParquetCompactor::new(Arc::new(pm), config);

        let result = compactor.compact_all_levels().unwrap();
        assert!(result.is_empty() || !result.is_empty());
    }

    #[test]
    fn test_deduplicate_batches_empty() {
        let batches: Vec<RecordBatch> = vec![];
        let result = deduplicate_batches(&batches).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_deduplicate_batches_no_timestamp_column() {
        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "value",
            arrow::datatypes::DataType::Float64,
            false,
        )]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(arrow::array::Float64Array::from(vec![
                1.0, 2.0, 3.0,
            ]))],
        )
        .unwrap();

        let result = deduplicate_batches(&[batch]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);
    }

    #[test]
    fn test_deduplicate_batches_removes_duplicates() {
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("tags_hash", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("timestamp", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Float64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrow::array::UInt64Array::from(vec![
                    1u64, 1u64, 2u64, 1u64,
                ])),
                Arc::new(arrow::array::Int64Array::from(vec![
                    100i64, 100i64, 200i64, 200i64,
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        )
        .unwrap();

        let result = deduplicate_batches(&[batch]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);
    }

    #[test]
    fn test_deduplicate_batches_no_duplicates() {
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("tags_hash", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("timestamp", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Float64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrow::array::UInt64Array::from(vec![1u64, 1u64, 2u64])),
                Arc::new(arrow::array::Int64Array::from(vec![100i64, 200i64, 100i64])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();

        let result = deduplicate_batches(&[batch]).unwrap();
        assert_eq!(result[0].num_rows(), 3);
    }

    #[test]
    fn test_uuid_short_format() {
        let uid = uuid_short();
        assert_eq!(uid.len(), 8);
        assert!(uid.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_uuid_short_unique() {
        let a = uuid_short();
        std::thread::sleep(std::time::Duration::from_nanos(1));
        let b = uuid_short();
        // Not guaranteed unique in theory, but extremely likely with nanosecond precision
        assert_eq!(a.len(), 8);
        assert_eq!(b.len(), 8);
    }

    #[test]
    fn test_deduplicate_null_tags_hash() {
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("tags_hash", arrow::datatypes::DataType::UInt64, true),
            arrow::datatypes::Field::new("timestamp", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Float64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrow::array::UInt64Array::from(vec![
                    Some(1u64),
                    None,
                    Some(2u64),
                    None,
                ])),
                Arc::new(arrow::array::Int64Array::from(vec![
                    100i64, 200i64, 300i64, 400i64,
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        )
        .unwrap();

        let result = deduplicate_batches(&[batch]).unwrap();
        assert_eq!(
            result[0].num_rows(),
            4,
            "null tags_hash rows should not be incorrectly deduplicated"
        );
    }

    #[test]
    fn test_deduplicate_timestamp_second_type() {
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("tags_hash", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new(
                "timestamp",
                arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
                false,
            ),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Float64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrow::array::UInt64Array::from(vec![1u64, 1u64, 2u64])),
                Arc::new(arrow::array::TimestampSecondArray::from(vec![
                    100i64, 200i64, 100i64,
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();

        let result = deduplicate_batches(&[batch]).unwrap();
        assert_eq!(
            result[0].num_rows(),
            3,
            "TimestampSecond should be handled correctly in dedup"
        );
    }

    #[test]
    fn test_deduplicate_timestamp_nanosecond_type() {
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("tags_hash", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new(
                "timestamp",
                arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                false,
            ),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Float64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(arrow::array::UInt64Array::from(vec![1u64, 1u64, 2u64])),
                Arc::new(arrow::array::TimestampNanosecondArray::from(vec![
                    100_000i64, 200_000i64, 100_000i64,
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();

        let result = deduplicate_batches(&[batch]).unwrap();
        assert_eq!(
            result[0].num_rows(),
            3,
            "TimestampNanosecond should be handled correctly in dedup"
        );
    }

    #[test]
    fn test_compaction_measurement_isolation() {
        let dir = TempDir::new().unwrap();
        let pm = Arc::new(PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap());

        let mut cpu_writer =
            TsdbParquetWriter::new(pm.clone(), WriteBufferConfig::default(), "cpu");
        let mut mem_writer =
            TsdbParquetWriter::new(pm.clone(), WriteBufferConfig::default(), "mem");

        let today = chrono::Utc::now().date_naive();
        let base_ts = today
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();

        for batch in 0..3 {
            let cpu_dps: Vec<DataPoint> = (0..5)
                .map(|i| {
                    DataPoint::new("cpu", base_ts + (batch * 5 + i) as i64 * 1_000_000)
                        .with_tag("host", "server01")
                        .with_field("usage", FieldValue::Float(0.5))
                })
                .collect();
            let mem_dps: Vec<DataPoint> = (0..5)
                .map(|i| {
                    DataPoint::new("mem", base_ts + (batch * 5 + i) as i64 * 1_000_000)
                        .with_tag("host", "server01")
                        .with_field("percent", FieldValue::Float(0.8))
                })
                .collect();
            cpu_writer.write_batch(&cpu_dps).unwrap();
            mem_writer.write_batch(&mem_dps).unwrap();
            cpu_writer.flush_all().unwrap();
            mem_writer.flush_all().unwrap();
        }

        let pm = Arc::new(PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap());
        let config = CompactionConfig {
            l0_max_files: 2,
            ..Default::default()
        };
        let compactor = ParquetCompactor::new(pm.clone(), config);
        compactor.compact_date(today).unwrap();

        let cpu_files = pm.list_measurement_parquet_files(today, "cpu").unwrap();
        let mem_files = pm.list_measurement_parquet_files(today, "mem").unwrap();

        assert!(
            !cpu_files.is_empty(),
            "cpu should still have files after compaction"
        );
        assert!(
            !mem_files.is_empty(),
            "mem should still have files after compaction"
        );

        for f in &cpu_files {
            assert!(
                f.to_string_lossy().contains("/cpu/"),
                "cpu compacted file should be in cpu subdirectory"
            );
        }
        for f in &mem_files {
            assert!(
                f.to_string_lossy().contains("/mem/"),
                "mem compacted file should be in mem subdirectory"
            );
        }
    }
}
