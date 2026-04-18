use crate::buffer::{AsyncWriteBuffer, WriteBuffer};
use crate::config::ArrowStorageConfig;
use crate::error::{Result, TsdbStorageArrowError};
use std::path::{Path, PathBuf};
use tsdb_arrow::schema::{DataPoint, Tags};
use tsdb_datafusion::engine::DataFusionQueryEngine;
use tsdb_parquet::compaction::{CompactionConfig, ParquetCompactor};
use tsdb_parquet::partition::{PartitionConfig, PartitionManager};
use tsdb_parquet::reader::TsdbParquetReader;
use tsdb_parquet::wal::{TsdbWAL, WALEntryType};
use tsdb_parquet::writer::{TsdbParquetWriter, WriteBufferConfig};

/// Arrow 存储引擎
///
/// 组合 Parquet 存储层、WAL、异步写入缓冲区和 DataFusion 查询引擎,
/// 提供完整的时序数据读写和 SQL 查询能力。
///
/// 架构:
/// ```text
/// [写入] → AsyncWriteBuffer → TsdbParquetWriter → Parquet 文件
///         ↘ WAL (可选)
/// [读取] → TsdbParquetReader → Parquet 文件
/// [查询] → DataFusionQueryEngine → SQL → RecordBatch
/// [维护] → ParquetCompactor (合并/清理)
/// ```
pub struct ArrowStorageEngine {
    partition_manager: PartitionManager,
    _reader: TsdbParquetReader,
    compactor: ParquetCompactor,
    wal: Option<TsdbWAL>,
    async_writer: AsyncWriteBuffer,
    query_engine: DataFusionQueryEngine,
    _config: ArrowStorageConfig,
    base_dir: PathBuf,
}

impl ArrowStorageEngine {
    /// 打开存储引擎
    ///
    /// 初始化分区管理器、读取器、Compactor、WAL、异步写入器和查询引擎。
    pub fn open(path: impl AsRef<Path>, config: ArrowStorageConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let partition_config = PartitionConfig {
            hot_days: config.hot_days,
            retention_days: config.retention_days,
            row_group_size: config.row_group_size,
            ..Default::default()
        };

        let partition_manager = PartitionManager::new(&path, partition_config)?;
        let reader =
            TsdbParquetReader::new(PartitionManager::new(&path, PartitionConfig::default())?);
        let compactor = ParquetCompactor::new(
            PartitionManager::new(&path, PartitionConfig::default())?,
            CompactionConfig::default(),
        );

        let wal = if config.wal_enabled {
            let wal_dir = path.join("wal");
            let wal_path = wal_dir.join("wal-000001.log");
            Some(TsdbWAL::create(&wal_path)?)
        } else {
            None
        };

        let writer_pm = PartitionManager::new(&path, PartitionConfig::default())?;
        let writer_config = WriteBufferConfig {
            max_buffer_rows: config.max_buffer_rows,
            flush_interval_ms: config.flush_interval_ms,
            ..Default::default()
        };
        let writer = TsdbParquetWriter::new(writer_pm, writer_config);

        let buffer = std::sync::Mutex::new(WriteBuffer::new(config.max_buffer_rows));
        let async_writer = AsyncWriteBuffer::new(buffer, writer, config.flush_interval_ms);

        let query_engine = DataFusionQueryEngine::new(&path);

        Ok(Self {
            partition_manager,
            _reader: reader,
            compactor,
            wal,
            async_writer,
            query_engine,
            _config: config,
            base_dir: path,
        })
    }

    /// 写入单个数据点 (先写 WAL, 再写缓冲区)
    pub fn write(&self, dp: &DataPoint) -> Result<()> {
        if let Some(ref wal) = self.wal {
            let payload = serde_json::to_vec(dp)
                .map_err(|e| TsdbStorageArrowError::Storage(format!("serialize error: {}", e)))?;
            wal.append(WALEntryType::Insert, &payload)?;
        }
        self.async_writer.write(dp)?;
        Ok(())
    }

    /// 批量写入数据点
    pub fn write_batch(&self, datapoints: &[DataPoint]) -> Result<()> {
        if let Some(ref wal) = self.wal {
            for dp in datapoints {
                let payload = serde_json::to_vec(dp).map_err(|e| {
                    TsdbStorageArrowError::Storage(format!("serialize error: {}", e))
                })?;
                wal.append(WALEntryType::Insert, &payload)?;
            }
        }
        self.async_writer.write_batch(datapoints)?;
        Ok(())
    }

    /// 范围读取数据点 (先刷盘, 再读取)
    pub fn read_range(
        &self,
        measurement: &str,
        tags: &Tags,
        start_micros: i64,
        end_micros: i64,
    ) -> Result<Vec<DataPoint>> {
        self.async_writer.flush()?;

        let pm = PartitionManager::new(&self.base_dir, PartitionConfig::default())?;
        let reader = TsdbParquetReader::new(pm);
        let points = reader.read_range(measurement, tags, start_micros, end_micros)?;
        Ok(points)
    }

    /// 读取单个数据点
    pub fn get_point(
        &self,
        measurement: &str,
        tags: &Tags,
        timestamp: i64,
    ) -> Result<Option<DataPoint>> {
        self.async_writer.flush()?;

        let pm = PartitionManager::new(&self.base_dir, PartitionConfig::default())?;
        let reader = TsdbParquetReader::new(pm);
        let point = reader.get_point(measurement, tags, timestamp)?;
        Ok(point)
    }

    /// 范围读取 Arrow RecordBatch
    pub fn read_range_arrow(
        &self,
        measurement: &str,
        start_micros: i64,
        end_micros: i64,
        projection: Option<&[String]>,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        self.async_writer.flush()?;

        let pm = PartitionManager::new(&self.base_dir, PartitionConfig::default())?;
        let reader = TsdbParquetReader::new(pm);
        let batches = reader.read_range_arrow(measurement, start_micros, end_micros, projection)?;
        Ok(batches)
    }

    /// 执行 SQL 查询
    pub async fn execute_sql(
        &self,
        sql: &str,
        measurement: &str,
        datapoints: &[DataPoint],
    ) -> Result<tsdb_datafusion::engine::QueryResult> {
        self.async_writer.flush()?;

        self.query_engine
            .register_from_datapoints(measurement, datapoints)?;

        let result = self.query_engine.execute(sql).await?;
        Ok(result)
    }

    /// 刷盘 (缓冲区 + WAL)
    pub fn flush(&self) -> Result<()> {
        self.async_writer.flush()?;
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// 清理过期分区
    pub fn cleanup(&self) -> Result<Vec<String>> {
        let dropped = self.partition_manager.cleanup_expired()?;
        Ok(dropped)
    }

    /// 执行 Compaction (合并今天的分区)
    pub fn compact(&self) -> Result<()> {
        let today = chrono::Utc::now().date_naive();
        self.compactor.compact_date(today)?;
        Ok(())
    }
}

impl Drop for ArrowStorageEngine {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tsdb_arrow::schema::FieldValue;

    fn make_test_datapoints(count: usize) -> Vec<DataPoint> {
        (0..count)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_tag("region", "us-west")
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.01))
                    .with_field("count", FieldValue::Integer(i as i64 * 10))
            })
            .collect()
    }

    #[test]
    fn test_engine_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let config = ArrowStorageConfig {
            wal_enabled: false,
            max_buffer_rows: 10,
            flush_interval_ms: 100,
            ..Default::default()
        };

        let engine = ArrowStorageEngine::open(dir.path(), config).unwrap();

        let dps = make_test_datapoints(20);
        engine.write_batch(&dps).unwrap();
        engine.flush().unwrap();

        let tags = Tags::new();
        let result = engine
            .read_range("cpu", &tags, 1_000_000_000, 1_000_020_000_000)
            .unwrap();

        assert!(!result.is_empty());
        for dp in &result {
            assert_eq!(dp.measurement, "cpu");
        }
    }

    #[test]
    fn test_engine_single_point() {
        let dir = tempfile::tempdir().unwrap();
        let config = ArrowStorageConfig {
            wal_enabled: false,
            max_buffer_rows: 10,
            flush_interval_ms: 100,
            ..Default::default()
        };

        let engine = ArrowStorageEngine::open(dir.path(), config).unwrap();

        let dp = DataPoint::new("cpu", 1_000_000)
            .with_tag("host", "s1")
            .with_field("usage", FieldValue::Float(0.5));

        engine.write(&dp).unwrap();
        engine.flush().unwrap();

        let tags = Tags::new();
        let result = engine.get_point("cpu", &tags, 1_000_000).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_engine_with_wal() {
        let dir = tempfile::tempdir().unwrap();
        let config = ArrowStorageConfig {
            wal_enabled: true,
            max_buffer_rows: 10,
            flush_interval_ms: 100,
            ..Default::default()
        };

        let engine = ArrowStorageEngine::open(dir.path(), config).unwrap();

        let dps = make_test_datapoints(5);
        engine.write_batch(&dps).unwrap();
        engine.flush().unwrap();

        let tags = Tags::new();
        let result = engine
            .read_range("cpu", &tags, 1_000_000_000, 1_000_005_000_000)
            .unwrap();
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn test_engine_sql_query() {
        let dir = tempfile::tempdir().unwrap();
        let config = ArrowStorageConfig {
            wal_enabled: false,
            max_buffer_rows: 10,
            flush_interval_ms: 100,
            ..Default::default()
        };

        let engine = ArrowStorageEngine::open(dir.path(), config).unwrap();

        let dps: Vec<DataPoint> = (0..50)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000)
                    .with_tag("host", "server01")
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.01))
            })
            .collect();

        engine.write_batch(&dps).unwrap();
        engine.flush().unwrap();

        let result = engine
            .execute_sql("SELECT AVG(usage) as avg_usage FROM cpu", "cpu", &dps)
            .await
            .unwrap();
        assert!(!result.rows.is_empty());
    }
}
