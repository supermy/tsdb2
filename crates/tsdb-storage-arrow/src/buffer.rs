use crate::error::{Result, TsdbStorageArrowError};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tsdb_arrow::schema::DataPoint;
use tsdb_parquet::partition::micros_to_date;
use tsdb_parquet::writer::TsdbParquetWriter;

/// 同步写入缓冲区
///
/// 按日期分区缓冲 DataPoint, 达到行数阈值时返回 true 表示需要刷盘。
pub struct WriteBuffer {
    /// 日期分区 → 数据点列表
    buffers: BTreeMap<chrono::NaiveDate, Vec<DataPoint>>,
    /// 缓冲区最大行数
    max_buffer_rows: usize,
    /// 当前总行数
    total_rows: usize,
}

impl WriteBuffer {
    /// 创建指定最大行数的缓冲区
    pub fn new(max_buffer_rows: usize) -> Self {
        Self {
            buffers: BTreeMap::new(),
            max_buffer_rows,
            total_rows: 0,
        }
    }

    /// 写入单个数据点, 返回是否达到刷盘阈值
    pub fn write(&mut self, dp: &DataPoint) -> bool {
        let date = micros_to_date(dp.timestamp);
        self.buffers.entry(date).or_default().push(dp.clone());
        self.total_rows += 1;
        self.total_rows >= self.max_buffer_rows
    }

    /// 批量写入数据点, 返回是否达到刷盘阈值
    pub fn write_batch(&mut self, datapoints: &[DataPoint]) -> bool {
        for dp in datapoints {
            let date = micros_to_date(dp.timestamp);
            self.buffers.entry(date).or_default().push(dp.clone());
        }
        self.total_rows += datapoints.len();
        self.total_rows >= self.max_buffer_rows
    }

    /// 排空缓冲区, 返回所有日期分区的数据
    pub fn drain(&mut self) -> Vec<(chrono::NaiveDate, Vec<DataPoint>)> {
        let keys: Vec<chrono::NaiveDate> = self.buffers.keys().copied().collect();
        let mut result = Vec::new();
        for key in keys {
            if let Some(v) = self.buffers.remove(&key) {
                result.push((key, v));
            }
        }
        self.total_rows = 0;
        result
    }

    /// 获取当前总行数
    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    /// 获取缓冲区中的分区数量
    pub fn buffer_count(&self) -> usize {
        self.buffers.len()
    }

    pub fn reinsert(&mut self, data: Vec<(chrono::NaiveDate, Vec<DataPoint>)>) {
        for (date, datapoints) in data {
            self.total_rows += datapoints.len();
            self.buffers.entry(date).or_default().extend(datapoints);
        }
    }
}

/// 异步写入缓冲区
///
/// 后台线程定时将缓冲区数据刷盘到 Parquet 文件。
/// 支持并发写入 (通过 Mutex 保护内部缓冲区)。
pub struct AsyncWriteBuffer {
    /// 内部同步缓冲区
    inner: Arc<Mutex<WriteBuffer>>,
    /// Parquet 写入器
    writer: Arc<Mutex<TsdbParquetWriter>>,
    /// 停止标志
    stop_flag: Arc<std::sync::atomic::AtomicBool>,
    /// 后台刷盘线程句柄
    handle: Option<std::thread::JoinHandle<()>>,
}

impl AsyncWriteBuffer {
    /// 创建异步写入缓冲区
    ///
    /// 启动后台线程, 每隔 `flush_interval_ms` 毫秒自动刷盘。
    pub fn new(
        buffer: Mutex<WriteBuffer>,
        writer: TsdbParquetWriter,
        flush_interval_ms: u64,
    ) -> Self {
        let inner = Arc::new(buffer);
        let writer_arc = Arc::new(Mutex::new(writer));
        let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let inner_clone = inner.clone();
        let writer_clone = writer_arc.clone();
        let stop_clone = stop_flag.clone();

        let handle = std::thread::Builder::new()
            .name("tsdb2-async-writer".to_string())
            .spawn(move || {
                while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_millis(flush_interval_ms));
                    Self::do_flush(&inner_clone, &writer_clone);
                }
                Self::do_flush(&inner_clone, &writer_clone);
            })
            .expect("failed to spawn async writer thread");

        Self {
            inner,
            writer: writer_arc,
            stop_flag,
            handle: Some(handle),
        }
    }

    /// 写入单个数据点 (线程安全)
    pub fn write(&self, dp: &DataPoint) -> Result<bool> {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(guard.write(dp))
    }

    /// 批量写入数据点 (线程安全)
    pub fn write_batch(&self, datapoints: &[DataPoint]) -> Result<bool> {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(guard.write_batch(datapoints))
    }

    /// 手动触发刷盘
    pub fn flush(&self) -> Result<usize> {
        Self::do_flush(&self.inner, &self.writer);
        Ok(0)
    }

    /// 执行刷盘: 排空缓冲区 → 写入 Parquet → flush_all
    fn do_flush(buffer: &Arc<Mutex<WriteBuffer>>, writer: &Arc<Mutex<TsdbParquetWriter>>) {
        let drained = {
            let mut guard = buffer.lock().unwrap_or_else(|e| e.into_inner());
            guard.drain()
        };

        if drained.is_empty() {
            return;
        }

        let mut writer_guard = writer.lock().unwrap_or_else(|e| e.into_inner());
        let mut failed = Vec::new();
        for (date, datapoints) in drained {
            if !datapoints.is_empty() {
                if let Err(e) = writer_guard.write_batch(&datapoints) {
                    tracing::error!("async flush write_batch failed: {}", e);
                    failed.push((date, datapoints));
                }
            }
        }

        if let Err(e) = writer_guard.flush_all() {
            tracing::error!("async flush flush_all failed: {}", e);
        }

        if !failed.is_empty() {
            let mut guard = buffer.lock().unwrap_or_else(|e| e.into_inner());
            guard.reinsert(failed);
        }
    }

    /// 获取缓冲区中的分区数量
    pub fn buffer_count(&self) -> usize {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.buffer_count()
    }

    /// 停止后台刷盘线程
    pub fn stop(&mut self) -> Result<()> {
        self.stop_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle
                .join()
                .map_err(|_| TsdbStorageArrowError::Storage("flush thread join failed".into()))?;
        }
        Ok(())
    }
}

impl Drop for AsyncWriteBuffer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tsdb_arrow::schema::FieldValue;
    use tsdb_parquet::partition::{PartitionConfig, PartitionManager};
    use tsdb_parquet::writer::WriteBufferConfig;

    #[test]
    fn test_write_buffer_basic() {
        let mut buffer = WriteBuffer::new(10);

        for i in 0..5 {
            let should_flush = buffer.write(
                &DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "s1")
                    .with_field("usage", FieldValue::Float(0.5)),
            );
            assert!(!should_flush);
        }

        assert_eq!(buffer.total_rows(), 5);
        assert_eq!(buffer.buffer_count(), 1);
    }

    #[test]
    fn test_write_buffer_auto_flush() {
        let mut buffer = WriteBuffer::new(3);

        for i in 0..3 {
            buffer.write(
                &DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "s1")
                    .with_field("usage", FieldValue::Float(0.5)),
            );
        }

        assert!(buffer.total_rows() >= 3);
    }

    #[test]
    fn test_async_write_buffer() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let writer_config = WriteBufferConfig::default();
        let writer = TsdbParquetWriter::new(Arc::new(pm), writer_config);

        let buffer = Mutex::new(WriteBuffer::new(100));
        let mut async_buf = AsyncWriteBuffer::new(buffer, writer, 100);

        for i in 0..5 {
            async_buf
                .write(
                    &DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                        .with_tag("host", "s1")
                        .with_field("usage", FieldValue::Float(0.5)),
                )
                .unwrap();
        }

        async_buf.flush().unwrap();
        async_buf.stop().unwrap();
    }
}
