use crate::buffer::{AsyncWriteBuffer, WriteBuffer};
use crate::config::ArrowStorageConfig;
use crate::error::{Result, TsdbStorageArrowError};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tsdb_arrow::schema::{DataPoint, Tags};
use tsdb_datafusion::engine::DataFusionQueryEngine;
use tsdb_parquet::compaction::{CompactionConfig, ParquetCompactor};
use tsdb_parquet::partition::{PartitionConfig, PartitionManager};
use tsdb_parquet::reader::TsdbParquetReader;
use tsdb_parquet::wal::{TsdbWAL, WALEntryType};
use tsdb_parquet::writer::{TsdbParquetWriter, WriteBufferConfig};

pub struct ArrowStorageEngine {
    partition_manager: Arc<PartitionManager>,
    reader: TsdbParquetReader,
    compactor: ParquetCompactor,
    wal: Option<TsdbWAL>,
    async_writers: std::sync::Mutex<HashMap<String, AsyncWriteBuffer>>,
    writer_config: WriteBufferConfig,
    query_engine: DataFusionQueryEngine,
    _config: ArrowStorageConfig,
    _base_dir: PathBuf,
    known_measurements: std::sync::Mutex<HashSet<String>>,
    write_seq: std::sync::atomic::AtomicU64,
    last_flush_seq: std::sync::atomic::AtomicU64,
    last_flush_time: std::sync::Mutex<std::time::Instant>,
}

impl ArrowStorageEngine {
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
        let compactor = ParquetCompactor::new(
            partition_manager.clone(),
            CompactionConfig {
                hot_days: config.hot_days,
                retention_days: config.retention_days,
                ..CompactionConfig::default()
            },
        );

        let mut wal_replay_count = 0usize;
        let mut wal_replay_datapoints: Vec<DataPoint> = Vec::new();
        let wal = if config.wal_enabled {
            let wal_dir = path.join("wal");
            std::fs::create_dir_all(&wal_dir).map_err(|e| {
                TsdbStorageArrowError::Storage(format!("failed to create WAL dir: {}", e))
            })?;
            let wal_path = wal_dir.join("wal-000001.log");

            if wal_path.exists() {
                match TsdbWAL::recover(&wal_path) {
                    Ok(entries) => {
                        tracing::info!("WAL recovery: found {} entries", entries.len());
                        for entry in &entries {
                            if entry.entry_type == WALEntryType::Insert {
                                match serde_json::from_slice::<DataPoint>(&entry.payload) {
                                    Ok(dp) => wal_replay_datapoints.push(dp),
                                    Err(e) => {
                                        tracing::warn!("WAL replay: skipping corrupt entry: {}", e);
                                    },
                                }
                            }
                        }
                        wal_replay_count = entries.len();
                    },
                    Err(e) => {
                        tracing::warn!("WAL recovery failed: {}", e);
                    },
                }
            }

            Some(TsdbWAL::create(&wal_path)?)
        } else {
            None
        };

        let writer_config = WriteBufferConfig {
            max_buffer_rows: config.max_buffer_rows,
            flush_interval_ms: config.flush_interval_ms,
            ..Default::default()
        };

        let query_engine = DataFusionQueryEngine::new(&path);

        let mut known_measurements = HashSet::new();
        {
            let pm_ref = &partition_manager;
            pm_ref.refresh().ok();
            let today = chrono::Utc::now().date_naive();
            let start = today - chrono::Duration::days(config.retention_days as i64);
            let partitions =
                pm_ref.get_partitions_in_range(start, today + chrono::Duration::days(1));
            for partition in &partitions {
                if let Ok(sub_dirs) = std::fs::read_dir(&partition.dir) {
                    for sub_entry in sub_dirs.flatten() {
                        let path = sub_entry.path();
                        if path.is_dir() {
                            let m_name = sub_entry.file_name().to_string_lossy().to_string();
                            known_measurements.insert(m_name);
                        } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                            if let Ok(file) = std::fs::File::open(&path) {
                                if let Ok(builder) =
                                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                                {
                                    let arrow_schema = builder.schema();
                                    if let Some(mf) = arrow_schema.metadata().get("measurement") {
                                        known_measurements.insert(mf.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let engine = Self {
            partition_manager,
            reader,
            compactor,
            wal,
            async_writers: std::sync::Mutex::new(HashMap::new()),
            writer_config,
            query_engine,
            _config: config,
            _base_dir: path,
            known_measurements: std::sync::Mutex::new(known_measurements),
            write_seq: std::sync::atomic::AtomicU64::new(0),
            last_flush_seq: std::sync::atomic::AtomicU64::new(0),
            last_flush_time: std::sync::Mutex::new(std::time::Instant::now()),
        };

        if !wal_replay_datapoints.is_empty() {
            tracing::info!(
                "WAL replay: re-writing {} datapoints to storage",
                wal_replay_datapoints.len()
            );
            for chunk in wal_replay_datapoints.chunks(1000) {
                if let Err(e) = engine.write_batch_bypass_wal(chunk) {
                    tracing::warn!("WAL replay write_batch failed: {}", e);
                }
            }
            if let Err(e) = engine.flush() {
                tracing::warn!("WAL replay flush failed: {}", e);
            }
            tracing::info!(
                "WAL recovery complete, {} entries replayed",
                wal_replay_count
            );
        } else if wal_replay_count > 0 {
            tracing::info!(
                "WAL recovery complete, {} entries found (0 valid datapoints to replay)",
                wal_replay_count
            );
        }

        if engine.wal.as_ref().is_some_and(|_w| wal_replay_count > 0) {
            if let Some(ref w) = engine.wal {
                if let Err(e) = w.truncate() {
                    tracing::warn!("WAL truncate after recovery failed: {}", e);
                }
            }
        }

        Ok(engine)
    }

    fn write_to_buffer(&self, measurement: &str, dp: &DataPoint) -> Result<()> {
        let mut writers = self.async_writers.lock().unwrap_or_else(|e| e.into_inner());
        if !writers.contains_key(measurement) {
            let writer = TsdbParquetWriter::new(
                self.partition_manager.clone(),
                self.writer_config.clone(),
                measurement,
            );
            let buffer =
                std::sync::Mutex::new(WriteBuffer::new(self.writer_config.max_buffer_rows));
            let async_writer =
                AsyncWriteBuffer::new(buffer, writer, self.writer_config.flush_interval_ms);
            writers.insert(measurement.to_string(), async_writer);
        }
        if let Some(writer) = writers.get(measurement) {
            writer.write(dp)?;
        }
        Ok(())
    }

    fn write_batch_to_buffer(&self, measurement: &str, datapoints: &[DataPoint]) -> Result<()> {
        let mut writers = self.async_writers.lock().unwrap_or_else(|e| e.into_inner());
        if !writers.contains_key(measurement) {
            let writer = TsdbParquetWriter::new(
                self.partition_manager.clone(),
                self.writer_config.clone(),
                measurement,
            );
            let buffer =
                std::sync::Mutex::new(WriteBuffer::new(self.writer_config.max_buffer_rows));
            let async_writer =
                AsyncWriteBuffer::new(buffer, writer, self.writer_config.flush_interval_ms);
            writers.insert(measurement.to_string(), async_writer);
        }
        if let Some(writer) = writers.get(measurement) {
            writer.write_batch(datapoints)?;
        }
        Ok(())
    }

    pub fn write(&self, dp: &DataPoint) -> Result<()> {
        if let Err(e) = dp.validate() {
            tracing::warn!("invalid datapoint rejected: {}", e);
            return Err(TsdbStorageArrowError::Storage(format!(
                "invalid datapoint: {}",
                e
            )));
        }
        if let Some(ref wal) = self.wal {
            let payload = serde_json::to_vec(dp)
                .map_err(|e| TsdbStorageArrowError::Storage(format!("serialize error: {}", e)))?;
            wal.append(WALEntryType::Insert, &payload)?;
        }
        self.write_to_buffer(&dp.measurement, dp)?;
        self.write_seq
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        self.known_measurements
            .lock()
            .unwrap_or_else(|e| {
                tracing::warn!("Mutex poisoned in write, recovering: {}", e);
                e.into_inner()
            })
            .insert(dp.measurement.clone());
        Ok(())
    }

    pub fn write_batch(&self, datapoints: &[DataPoint]) -> Result<()> {
        for dp in datapoints {
            if let Err(e) = dp.validate() {
                tracing::warn!("invalid datapoint in batch rejected: {}", e);
                return Err(TsdbStorageArrowError::Storage(format!(
                    "invalid datapoint in batch: {}",
                    e
                )));
            }
        }

        if let Some(ref wal) = self.wal {
            for dp in datapoints {
                let payload = serde_json::to_vec(dp).map_err(|e| {
                    TsdbStorageArrowError::Storage(format!("serialize error: {}", e))
                })?;
                wal.append(WALEntryType::Insert, &payload)?;
            }
        }

        let mut by_measurement: HashMap<&str, Vec<&DataPoint>> = HashMap::new();
        for dp in datapoints {
            by_measurement.entry(&dp.measurement).or_default().push(dp);
        }

        for (measurement, dps) in by_measurement {
            let owned: Vec<DataPoint> = dps.into_iter().cloned().collect();
            self.write_batch_to_buffer(measurement, &owned)?;
        }

        self.write_seq.fetch_add(
            datapoints.len() as u64,
            std::sync::atomic::Ordering::Release,
        );

        let mut measurements = self.known_measurements.lock().unwrap_or_else(|e| {
            tracing::warn!("Mutex poisoned in write_batch, recovering: {}", e);
            e.into_inner()
        });
        for dp in datapoints {
            measurements.insert(dp.measurement.clone());
        }
        Ok(())
    }

    fn write_batch_bypass_wal(&self, datapoints: &[DataPoint]) -> Result<()> {
        let mut by_measurement: HashMap<&str, Vec<&DataPoint>> = HashMap::new();
        for dp in datapoints {
            by_measurement.entry(&dp.measurement).or_default().push(dp);
        }

        for (measurement, dps) in by_measurement {
            let owned: Vec<DataPoint> = dps.into_iter().cloned().collect();
            self.write_batch_to_buffer(measurement, &owned)?;
        }

        self.write_seq.fetch_add(
            datapoints.len() as u64,
            std::sync::atomic::Ordering::Release,
        );

        let mut measurements = self.known_measurements.lock().unwrap_or_else(|e| {
            tracing::warn!(
                "Mutex poisoned in write_batch_bypass_wal, recovering: {}",
                e
            );
            e.into_inner()
        });
        for dp in datapoints {
            measurements.insert(dp.measurement.clone());
        }
        Ok(())
    }

    pub fn read_range(
        &self,
        measurement: &str,
        tags: &Tags,
        start_micros: i64,
        end_micros: i64,
    ) -> Result<Vec<DataPoint>> {
        self.ensure_data_visible()?;
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
        self.ensure_data_visible()?;
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
        self.ensure_data_visible()?;
        let batches =
            self.reader
                .read_range_arrow(measurement, start_micros, end_micros, projection)?;
        Ok(batches)
    }

    pub async fn execute_sql(
        &self,
        sql: &str,
        measurement: &str,
        datapoints: &[DataPoint],
    ) -> Result<tsdb_datafusion::engine::QueryResult> {
        self.ensure_data_visible()?;

        self.query_engine
            .register_from_datapoints(measurement, datapoints)?;

        let result = self.query_engine.execute(sql).await?;
        Ok(result)
    }

    pub fn flush(&self) -> Result<()> {
        let writers = self.async_writers.lock().unwrap_or_else(|e| e.into_inner());

        let mut all_flushed = true;
        for (measurement, writer) in writers.iter() {
            if let Err(e) = writer.flush() {
                tracing::warn!("failed to flush writer for {}: {}", measurement, e);
                all_flushed = false;
            }
        }

        if let Some(ref wal) = self.wal {
            wal.sync()?;
            if all_flushed {
                wal.truncate()?;
            }
        }

        if all_flushed {
            let current = self.write_seq.load(std::sync::atomic::Ordering::Acquire);
            self.last_flush_seq
                .store(current, std::sync::atomic::Ordering::Release);
            *self
                .last_flush_time
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = std::time::Instant::now();
        }

        if !all_flushed {
            return Err(TsdbStorageArrowError::Storage(
                "one or more writers failed to flush".to_string(),
            ));
        }

        Ok(())
    }

    fn ensure_data_visible(&self) -> Result<()> {
        let current = self.write_seq.load(std::sync::atomic::Ordering::Acquire);
        let flushed = self
            .last_flush_seq
            .load(std::sync::atomic::Ordering::Acquire);
        if current > flushed {
            let should_flush = {
                let last = self
                    .last_flush_time
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                last.elapsed() >= std::time::Duration::from_secs(1)
            };
            if should_flush {
                self.flush()?;
            }
        }
        Ok(())
    }

    pub fn cleanup(&self) -> Result<Vec<String>> {
        self.flush()?;
        let dropped = self.partition_manager.cleanup_expired()?;

        let active_measurements = self.partition_manager.list_measurements();
        let writer_keys: Vec<String> = {
            let writers = self.async_writers.lock().unwrap_or_else(|e| e.into_inner());
            writers.keys().cloned().collect()
        };
        let stale: Vec<String> = writer_keys
            .iter()
            .filter(|m| !active_measurements.contains(m))
            .cloned()
            .collect();

        if !stale.is_empty() {
            let mut writers = self.async_writers.lock().unwrap_or_else(|e| e.into_inner());
            for m in &stale {
                if let Some(mut writer) = writers.remove(m) {
                    if let Err(e) = writer.stop() {
                        tracing::warn!("failed to stop stale writer for {}: {}", m, e);
                    }
                }
            }
        }

        if !dropped.is_empty() {
            let mut known = self
                .known_measurements
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for m in &stale {
                known.remove(m);
            }
        }

        Ok(dropped)
    }

    pub fn compact(&self) -> Result<()> {
        self.compactor.compact_all_levels()?;
        Ok(())
    }

    pub fn refresh_partitions(&self) -> Result<()> {
        self.partition_manager.refresh()?;
        let discovered = self.partition_manager.list_measurements();
        let mut known = self.known_measurements.lock().unwrap_or_else(|e| {
            tracing::warn!("Mutex poisoned in refresh_partitions, recovering: {}", e);
            e.into_inner()
        });
        for m in discovered {
            known.insert(m);
        }
        Ok(())
    }
}

impl Drop for ArrowStorageEngine {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            tracing::warn!("failed to flush during drop: {}", e);
        }
        {
            let mut writers = self.async_writers.lock().unwrap_or_else(|e| e.into_inner());
            for (_measurement, writer) in writers.iter_mut() {
                if let Err(e) = writer.stop() {
                    tracing::warn!("failed to stop async writer during drop: {}", e);
                }
            }
        }
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
        let measurements = self.known_measurements.lock().unwrap_or_else(|e| {
            tracing::warn!("Mutex poisoned in list_measurements, recovering: {}", e);
            e.into_inner()
        });
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
            },
        };
        Some(tsdb_arrow::schema::compact_tsdb_schema_from_datapoints(
            &datapoints,
        ))
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

        let wal_path = dir.path().join("wal").join("wal-000001.log");
        assert!(wal_path.exists());
        let entries_before = TsdbWAL::recover(&wal_path).unwrap();
        assert!(!entries_before.is_empty());

        engine.flush().unwrap();

        let entries_after = TsdbWAL::recover(&wal_path).unwrap();
        assert!(
            entries_after.is_empty(),
            "WAL should be truncated after flush"
        );
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

        for dp in &cpu_result {
            assert_eq!(dp.measurement, "cpu");
        }
        for dp in &mem_result {
            assert_eq!(dp.measurement, "mem");
        }
    }

    #[test]
    fn test_engine_measurement_isolation_directories() {
        let dir = tempfile::tempdir().unwrap();
        let engine = ArrowStorageEngine::open(dir.path(), make_config(false)).unwrap();

        let cpu_dps: Vec<DataPoint> = (0..3)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000)
                    .with_tag("host", "s1")
                    .with_field("usage", FieldValue::Float(0.5))
            })
            .collect();

        let mem_dps: Vec<DataPoint> = (0..3)
            .map(|i| {
                DataPoint::new("mem", 1_000_000 + i as i64 * 1_000)
                    .with_tag("host", "s1")
                    .with_field("used", FieldValue::Float(1024.0))
            })
            .collect();

        engine.write_batch(&cpu_dps).unwrap();
        engine.write_batch(&mem_dps).unwrap();
        engine.flush().unwrap();

        let date = tsdb_parquet::partition::micros_to_date(1_000_000).unwrap();
        let cpu_files = engine
            .partition_manager
            .list_measurement_parquet_files(date, "cpu")
            .unwrap();
        let mem_files = engine
            .partition_manager
            .list_measurement_parquet_files(date, "mem")
            .unwrap();

        assert!(
            !cpu_files.is_empty(),
            "cpu should have files in its subdirectory"
        );
        assert!(
            !mem_files.is_empty(),
            "mem should have files in its subdirectory"
        );

        for f in &cpu_files {
            assert!(f.to_string_lossy().contains("/cpu/"));
        }
        for f in &mem_files {
            assert!(f.to_string_lossy().contains("/mem/"));
        }
    }

    #[test]
    fn test_engine_mixed_batch_write() {
        let dir = tempfile::tempdir().unwrap();
        let engine = ArrowStorageEngine::open(dir.path(), make_config(false)).unwrap();

        let mut mixed_dps = Vec::new();
        for i in 0..5 {
            mixed_dps.push(
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000)
                    .with_tag("host", "s1")
                    .with_field("usage", FieldValue::Float(i as f64)),
            );
            mixed_dps.push(
                DataPoint::new("mem", 1_000_000 + i as i64 * 1_000)
                    .with_tag("host", "s1")
                    .with_field("percent", FieldValue::Float(i as f64 * 10.0)),
            );
        }

        engine.write_batch(&mixed_dps).unwrap();
        engine.flush().unwrap();

        let tags = Tags::new();
        let cpu_result = engine.read_range("cpu", &tags, 0, 2_000_000).unwrap();
        let mem_result = engine.read_range("mem", &tags, 0, 2_000_000).unwrap();

        assert_eq!(cpu_result.len(), 5, "cpu should have 5 data points");
        assert_eq!(mem_result.len(), 5, "mem should have 5 data points");

        for dp in &cpu_result {
            assert_eq!(dp.measurement, "cpu");
            assert!(dp.fields.contains_key("usage"));
        }
        for dp in &mem_result {
            assert_eq!(dp.measurement, "mem");
            assert!(dp.fields.contains_key("percent"));
        }
    }
}
