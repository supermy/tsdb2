use crate::buffer::{AsyncWriteBuffer, WriteBuffer};
use crate::config::ArrowStorageConfig;
use crate::error::{Result, TsdbStorageArrowError};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
    partition_manager: Arc<PartitionManager>,
    reader: TsdbParquetReader,
    compactor: ParquetCompactor,
    wal: Option<TsdbWAL>,
    async_writer: AsyncWriteBuffer,
    query_engine: DataFusionQueryEngine,
    _config: ArrowStorageConfig,
    _base_dir: PathBuf,
    known_measurements: std::sync::Mutex<HashSet<String>>,
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

        let partition_manager = Arc::new(PartitionManager::new(&path, partition_config)?);
        let reader = TsdbParquetReader::new(partition_manager.clone());
        let compactor =
            ParquetCompactor::new(partition_manager.clone(), CompactionConfig::default());

        let wal = if config.wal_enabled {
            let wal_dir = path.join("wal");
            let wal_path = wal_dir.join("wal-000001.log");
            Some(TsdbWAL::create(&wal_path)?)
        } else {
            None
        };

        let writer_config = WriteBufferConfig {
            max_buffer_rows: config.max_buffer_rows,
            flush_interval_ms: config.flush_interval_ms,
            ..Default::default()
        };
        let writer = TsdbParquetWriter::new(partition_manager.clone(), writer_config);

        let buffer = std::sync::Mutex::new(WriteBuffer::new(config.max_buffer_rows));
        let async_writer = AsyncWriteBuffer::new(buffer, writer, config.flush_interval_ms);

        let query_engine = DataFusionQueryEngine::new(&path);

        Ok(Self {
            partition_manager,
            reader,
            compactor,
            wal,
            async_writer,
            query_engine,
            _config: config,
            _base_dir: path,
            known_measurements: std::sync::Mutex::new(HashSet::new()),
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
        self.known_measurements
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(dp.measurement.clone());
        Ok(())
    }

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
        let mut measurements = self.known_measurements.lock().unwrap_or_else(|e| e.into_inner());
        for dp in datapoints {
            measurements.insert(dp.measurement.clone());
        }
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
        let points = self
            .reader
            .read_range(measurement, tags, start_micros, end_micros)?;
        Ok(points)
    }

    pub fn get_point(
        &self,
        measurement: &str,
        tags: &Tags,
        timestamp: i64,
    ) -> Result<Option<DataPoint>> {
        self.async_writer.flush()?;
        let point = self.reader.get_point(measurement, tags, timestamp)?;
        Ok(point)
    }

    pub fn read_range_arrow(
        &self,
        measurement: &str,
        start_micros: i64,
        end_micros: i64,
        projection: Option<&[String]>,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        self.async_writer.flush()?;
        let batches =
            self.reader
                .read_range_arrow(measurement, start_micros, end_micros, projection)?;
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

impl tsdb_arrow::StorageEngine for ArrowStorageEngine {
    fn write(&self, dp: &DataPoint) -> tsdb_arrow::engine::EngineResult<()> {
        ArrowStorageEngine::write(self, dp)
            .map_err(|e| tsdb_arrow::error::TsdbArrowError::Storage(e.to_string()))
    }

    fn write_batch(&self, datapoints: &[DataPoint]) -> tsdb_arrow::engine::EngineResult<()> {
        ArrowStorageEngine::write_batch(self, datapoints)
            .map_err(|e| tsdb_arrow::error::TsdbArrowError::Storage(e.to_string()))
    }

    fn read_range(
        &self,
        measurement: &str,
        tags: &Tags,
        start: i64,
        end: i64,
    ) -> tsdb_arrow::engine::EngineResult<Vec<DataPoint>> {
        ArrowStorageEngine::read_range(self, measurement, tags, start, end)
            .map_err(|e| tsdb_arrow::error::TsdbArrowError::Storage(e.to_string()))
    }

    fn get_point(
        &self,
        measurement: &str,
        tags: &Tags,
        timestamp: i64,
    ) -> tsdb_arrow::engine::EngineResult<Option<DataPoint>> {
        ArrowStorageEngine::get_point(self, measurement, tags, timestamp)
            .map_err(|e| tsdb_arrow::error::TsdbArrowError::Storage(e.to_string()))
    }

    fn list_measurements(&self) -> Vec<String> {
        let measurements = self.known_measurements.lock().unwrap_or_else(|e| e.into_inner());
        let mut list: Vec<String> = measurements.iter().cloned().collect();
        list.sort();
        list
    }

    fn flush(&self) -> tsdb_arrow::engine::EngineResult<()> {
        ArrowStorageEngine::flush(self)
            .map_err(|e| tsdb_arrow::error::TsdbArrowError::Storage(e.to_string()))
    }

    fn read_range_arrow(
        &self,
        measurement: &str,
        start: i64,
        end: i64,
        projection: Option<&[String]>,
    ) -> tsdb_arrow::engine::EngineResult<Vec<RecordBatch>> {
        ArrowStorageEngine::read_range_arrow(self, measurement, start, end, projection)
            .map_err(|e| tsdb_arrow::error::TsdbArrowError::Storage(e.to_string()))
    }

    fn measurement_schema(&self, measurement: &str) -> Option<SchemaRef> {
        let now = chrono::Utc::now().timestamp_micros();
        let start = now - 365 * 86_400_000_000i64;
        let datapoints =
            ArrowStorageEngine::read_range(self, measurement, &Tags::new(), start, now)
                .ok()
                .filter(|dps| !dps.is_empty());
        let datapoints = match datapoints {
            Some(dps) => dps,
            None => {
                let future = now + 86_400_000_000i64;
                ArrowStorageEngine::read_range(self, measurement, &Tags::new(), now, future)
                    .ok()
                    .filter(|dps| !dps.is_empty())?
            }
        };
        Some(tsdb_arrow::schema::compact_tsdb_schema_from_datapoints(&datapoints))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
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

    fn make_config(wal: bool) -> ArrowStorageConfig {
        ArrowStorageConfig {
            wal_enabled: wal,
            max_buffer_rows: 10,
            flush_interval_ms: 100,
            ..Default::default()
        }
    }

    #[test]
    fn test_engine_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let engine = ArrowStorageEngine::open(dir.path(), make_config(false)).unwrap();

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
        let engine = ArrowStorageEngine::open(dir.path(), make_config(false)).unwrap();

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
        let engine = ArrowStorageEngine::open(dir.path(), make_config(true)).unwrap();

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
        let engine = ArrowStorageEngine::open(dir.path(), make_config(false)).unwrap();

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

    #[test]
    fn test_engine_wal_recovery_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().to_path_buf();

        let dps = make_test_datapoints(15);
        {
            let engine = ArrowStorageEngine::open(&wal_dir, make_config(true)).unwrap();
            engine.write_batch(&dps).unwrap();
            engine.flush().unwrap();
        }

        let engine2 = ArrowStorageEngine::open(&wal_dir, make_config(true)).unwrap();
        let tags = Tags::new();
        let result = engine2
            .read_range("cpu", &tags, 1_000_000_000, 1_000_015_000_000)
            .unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn test_engine_compaction_after_multiple_writes() {
        let dir = tempfile::tempdir().unwrap();
        let engine = ArrowStorageEngine::open(dir.path(), make_config(false)).unwrap();

        for batch in 0..3 {
            let dps: Vec<DataPoint> = (0..15)
                .map(|i| {
                    DataPoint::new("cpu", 1_000_000_000 + (batch * 15 + i) as i64 * 1_000_000)
                        .with_tag("host", "server01")
                        .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.01))
                })
                .collect();
            engine.write_batch(&dps).unwrap();
            engine.flush().unwrap();
        }

        let result = engine.compact();
        assert!(result.is_ok());
    }

    #[test]
    fn test_engine_concurrent_writes() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(ArrowStorageEngine::open(dir.path(), make_config(true)).unwrap());

        let mut handles = Vec::new();
        for t in 0..4 {
            let eng = engine.clone();
            handles.push(thread::spawn(move || {
                let dps: Vec<DataPoint> = (0..10)
                    .map(|i| {
                        DataPoint::new("cpu", 1_000_000_000 + (t * 10 + i) as i64 * 1_000_000)
                            .with_tag("host", format!("host-{}", t))
                            .with_field("usage", FieldValue::Float(t as f64 + i as f64 * 0.1))
                    })
                    .collect();
                eng.write_batch(&dps).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        engine.flush().unwrap();
        let tags = Tags::new();
        let result = engine
            .read_range("cpu", &tags, 1_000_000_000, 1_000_040_000_000)
            .unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn test_engine_read_empty_range() {
        let dir = tempfile::tempdir().unwrap();
        let engine = ArrowStorageEngine::open(dir.path(), make_config(false)).unwrap();

        let tags = Tags::new();
        let result = engine
            .read_range("cpu", &tags, 9_999_000_000, 9_999_001_000)
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_engine_flush_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let engine = ArrowStorageEngine::open(dir.path(), make_config(true)).unwrap();

        engine.flush().unwrap();
        engine.flush().unwrap();
        engine.flush().unwrap();
    }

    #[test]
    fn test_engine_wal_sync_after_write() {
        let dir = tempfile::tempdir().unwrap();
        let engine = ArrowStorageEngine::open(dir.path(), make_config(true)).unwrap();

        let dp = DataPoint::new("cpu", 1_000_000)
            .with_tag("host", "s1")
            .with_field("usage", FieldValue::Float(0.5));

        engine.write(&dp).unwrap();
        engine.flush().unwrap();

        let wal_path = dir.path().join("wal").join("wal-000001.log");
        assert!(wal_path.exists());
        let entries = TsdbWAL::recover(&wal_path).unwrap();
        assert!(!entries.is_empty());
    }

    #[test]
    fn test_engine_multiple_measurements() {
        let dir = tempfile::tempdir().unwrap();
        let engine = ArrowStorageEngine::open(dir.path(), make_config(false)).unwrap();

        let cpu_dps: Vec<DataPoint> = (0..5)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000)
                    .with_tag("host", "s1")
                    .with_field("usage", FieldValue::Float(0.5))
            })
            .collect();

        let mem_dps: Vec<DataPoint> = (0..5)
            .map(|i| {
                DataPoint::new("mem", 1_000_000 + i as i64 * 1_000)
                    .with_tag("host", "s1")
                    .with_field("used", FieldValue::Float(1024.0))
            })
            .collect();

        engine.write_batch(&cpu_dps).unwrap();
        engine.write_batch(&mem_dps).unwrap();
        engine.flush().unwrap();

        let tags = Tags::new();
        let cpu_result = engine.read_range("cpu", &tags, 0, 2_000_000).unwrap();
        let mem_result = engine.read_range("mem", &tags, 0, 2_000_000).unwrap();
        assert!(!cpu_result.is_empty());
        assert!(!mem_result.is_empty());
    }
}
