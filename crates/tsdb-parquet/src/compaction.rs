use std::sync::Arc;
use crate::error::{Result, TsdbParquetError};
use crate::partition::PartitionManager;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::PathBuf;

/// Parquet Compaction 配置
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// 热数据保留天数, 默认 7 天
    pub hot_days: u64,
    /// 总数据保留天数, 默认 30 天
    pub retention_days: u64,
    /// 合并后目标文件行数阈值
    pub target_rows: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            hot_days: 7,
            retention_days: 30,
            target_rows: 100_000,
        }
    }
}

/// Parquet Compactor
///
/// 负责合并小文件、清理过期分区、转换冷热数据编码。
///
/// 合并策略:
/// 1. 扫描指定日期分区内的所有 Parquet 文件
/// 2. 读取所有 RecordBatch 并合并
/// 3. 按行数阈值重新写入 (行数 ≤ target_rows 使用 hot 编码, 否则使用 cold 编码)
/// 4. 删除原始小文件
pub struct ParquetCompactor {
    partition_manager: Arc<PartitionManager>,
    config: CompactionConfig,
}

impl ParquetCompactor {
    /// 创建 Compactor
    pub fn new(partition_manager: Arc<PartitionManager>, config: CompactionConfig) -> Self {
        Self {
            partition_manager,
            config,
        }
    }

    /// 合并指定日期分区内的所有 Parquet 文件
    ///
    /// 1. 读取分区内所有 RecordBatch
    /// 2. 合并为单个大文件
    /// 3. 根据行数选择 hot/cold 编码
    /// 4. 替换原始文件
    pub fn compact_date(&self, date: NaiveDate) -> Result<()> {
        let files = self.partition_manager.list_parquet_files(date)?;

        if files.len() <= 1 {
            return Ok(());
        }

        let mut all_batches = Vec::new();
        for path in &files {
            let batches = self.read_parquet_file(path)?;
            all_batches.extend(batches);
        }

        if all_batches.is_empty() {
            return Ok(());
        }

        let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();

        let dir = self.partition_manager.ensure_partition(date)?;
        let tmp_path = dir.join(format!(".tmp-compacted-{}.parquet", date.format("%Y%m%d")));
        let final_path = dir.join(format!("compacted-{}.parquet", date.format("%Y%m%d")));

        let schema = all_batches[0].schema();
        let file = File::create(&tmp_path).map_err(|e| {
            TsdbParquetError::Io(std::io::Error::other(format!(
                "failed to create compacted file: {}",
                e
            )))
        })?;

        let writer_props = if total_rows <= self.config.target_rows {
            crate::encoding::hot_writer_props()
        } else {
            crate::encoding::cold_writer_props()
        };

        let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(
            file,
            schema.clone(),
            Some(writer_props),
        )
        .map_err(TsdbParquetError::Parquet)?;

        for batch in &all_batches {
            writer.write(batch).map_err(TsdbParquetError::Parquet)?;
        }

        writer.close().map_err(TsdbParquetError::Parquet)?;

        std::fs::rename(&tmp_path, &final_path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp_path);
            TsdbParquetError::Io(std::io::Error::other(format!(
                "atomic rename failed: {}",
                e
            )))
        })?;

        for path in &files {
            if path != &final_path {
                if let Err(e) = std::fs::remove_file(path) {
                    tracing::warn!("failed to remove file {:?}: {}", path, e);
                }
            }
        }

        Ok(())
    }

    /// 清理过期分区
    ///
    /// 删除早于 `now - retention_days` 的分区目录
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

    /// 读取单个 Parquet 文件的所有 RecordBatch
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
}
