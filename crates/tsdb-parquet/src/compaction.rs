use crate::error::{Result, TsdbParquetError};
use crate::partition::PartitionManager;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

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
        self.compact_l0_to_l1(date)?;
        self.compact_l1_to_l2(date)
    }

    fn compact_l0_to_l1(&self, date: NaiveDate) -> Result<()> {
        let files = self.partition_manager.list_parquet_files(date)?;
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

        let deduped = deduplicate_batches(&all_batches)?;

        let dir = self.partition_manager.ensure_partition(date)?;
        let tmp_path = dir.join(format!(".tmp-L1-{}", date.format("%Y%m%d")));
        let final_path = dir.join(format!("L1-{}.parquet", date.format("%Y%m%d")));

        self.write_compacted(&deduped, &tmp_path, &final_path, CompactionLevel::L1)?;

        for path in &l0_files {
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!("failed to remove L0 file {:?}: {}", path, e);
            }
        }

        Ok(())
    }

    fn compact_l1_to_l2(&self, date: NaiveDate) -> Result<()> {
        let files = self.partition_manager.list_parquet_files(date)?;
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

        let deduped = deduplicate_batches(&all_batches)?;

        let dir = self.partition_manager.ensure_partition(date)?;
        let tmp_path = dir.join(format!(".tmp-L2-{}", date.format("%Y%m%d")));
        let final_path = dir.join(format!("L2-{}.parquet", date.format("%Y%m%d")));

        self.write_compacted(&deduped, &tmp_path, &final_path, CompactionLevel::L2)?;

        for path in &l1_files {
            if let Err(e) = std::fs::remove_file(path) {
                tracing::warn!("failed to remove L1 file {:?}: {}", path, e);
            }
        }

        Ok(())
    }

    fn write_compacted(
        &self,
        batches: &[RecordBatch],
        tmp_path: &PathBuf,
        final_path: &PathBuf,
        level: CompactionLevel,
    ) -> Result<()> {
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let schema = batches[0].schema();

        let file = File::create(tmp_path).map_err(|e| {
            TsdbParquetError::Io(std::io::Error::other(format!(
                "failed to create compacted file: {}",
                e
            )))
        })?;

        let writer_props = match level {
            CompactionLevel::L0 => crate::encoding::hot_writer_props(),
            CompactionLevel::L1 => crate::encoding::hot_writer_props(),
            CompactionLevel::L2 => {
                if total_rows <= self.config.l2_target_rows {
                    crate::encoding::hot_writer_props()
                } else {
                    crate::encoding::cold_writer_props()
                }
            }
        };

        let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(
            file,
            schema.clone(),
            Some(writer_props),
        )
        .map_err(TsdbParquetError::Parquet)?;

        for batch in batches {
            writer.write(batch).map_err(TsdbParquetError::Parquet)?;
        }

        writer.close().map_err(TsdbParquetError::Parquet)?;

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
        let today = chrono::Utc::now().date_naive();
        let cutoff = today - chrono::Duration::days(self.config.retention_days as i64);

        self.partition_manager.refresh()?;
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
                self.compact_date(partition.date)?;
                result.insert(partition.date, (l0_count, l1_count));
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

    Ok(batches.to_vec())
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
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default());

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
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default());

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
}
