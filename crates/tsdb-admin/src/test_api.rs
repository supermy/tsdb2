use crate::error::Result;
use crate::protocol::*;
use std::sync::Arc;
use tsdb_arrow::engine::StorageEngine;
use tsdb_arrow::schema::{DataPoint, FieldValue, Tags};

pub struct TestRunner {
    engine: Arc<dyn StorageEngine>,
}

impl TestRunner {
    pub fn new(engine: Arc<dyn StorageEngine>) -> Self {
        Self { engine }
    }

    pub async fn run_sql(&self, sql: &str) -> Result<BenchResult> {
        let start = std::time::Instant::now();
        let ctx = datafusion::prelude::SessionContext::new();
        let result = match ctx.sql(sql).await {
            Err(e) => Err(crate::error::AdminError::Storage(e.to_string())),
            Ok(df) => match df.collect().await {
                Err(e) => Err(crate::error::AdminError::Storage(e.to_string())),
                Ok(b) => Ok::<usize, crate::error::AdminError>(
                    b.iter().map(|b| b.num_rows()).sum(),
                ),
            },
        };
        let elapsed = start.elapsed();
        let row_count = result.unwrap_or(0);

        Ok(BenchResult {
            operation: "sql".to_string(),
            total_points: row_count,
            elapsed_secs: elapsed.as_secs_f64(),
            rate_per_sec: if elapsed.as_secs_f64() > 0.0 {
                row_count as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            },
            avg_latency_us: elapsed.as_micros() as f64,
            p99_latency_us: elapsed.as_micros() as f64,
        })
    }

    pub async fn run_write_bench(
        &self,
        measurement: &str,
        total_points: usize,
        workers: usize,
        batch_size: usize,
    ) -> Result<BenchResult> {
        let start = std::time::Instant::now();
        let points_per_worker = if workers > 0 {
            total_points.div_ceil(workers)
        } else {
            total_points
        };
        let mut total_written = 0usize;
        let base_ts = chrono::Utc::now().timestamp_micros();

        for w in 0..workers.max(1) {
            let mut tags = Tags::new();
            tags.insert("host".to_string(), format!("worker-{}", w));
            let mut batch = Vec::with_capacity(batch_size);
            for i in 0..points_per_worker {
                let mut fields = std::collections::BTreeMap::new();
                fields.insert("value".to_string(), FieldValue::Float(i as f64));
                let dp = DataPoint {
                    measurement: measurement.to_string(),
                    tags: tags.clone(),
                    fields: fields.into(),
                    timestamp: base_ts + (total_written + i) as i64 * 1000,
                };
                batch.push(dp);
                if batch.len() >= batch_size {
                    self.engine
                        .write_batch(&batch)
                        .map_err(|e| crate::error::AdminError::Storage(e.to_string()))?;
                    total_written += batch.len();
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                self.engine
                    .write_batch(&batch)
                    .map_err(|e| crate::error::AdminError::Storage(e.to_string()))?;
                total_written += batch.len();
            }
        }

        let elapsed = start.elapsed();
        Ok(BenchResult {
            operation: "write".to_string(),
            total_points: total_written,
            elapsed_secs: elapsed.as_secs_f64(),
            rate_per_sec: if elapsed.as_secs_f64() > 0.0 {
                total_written as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            },
            avg_latency_us: (elapsed.as_micros() as f64) / (total_written.max(1) as f64),
            p99_latency_us: 0.0,
        })
    }

    pub async fn run_read_bench(
        &self,
        measurement: &str,
        queries: usize,
        _workers: usize,
    ) -> Result<BenchResult> {
        let now = chrono::Utc::now().timestamp_micros();
        let start = std::time::Instant::now();
        let mut total_rows = 0usize;

        for _ in 0..queries {
            let result = self
                .engine
                .read_range(measurement, &Tags::new(), now - 3_600_000_000, now)
                .map_err(|e| crate::error::AdminError::Storage(e.to_string()))?;
            total_rows += result.len();
        }

        let elapsed = start.elapsed();
        Ok(BenchResult {
            operation: "read".to_string(),
            total_points: total_rows,
            elapsed_secs: elapsed.as_secs_f64(),
            rate_per_sec: if elapsed.as_secs_f64() > 0.0 {
                total_rows as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            },
            avg_latency_us: (elapsed.as_micros() as f64) / (queries.max(1) as f64),
            p99_latency_us: 0.0,
        })
    }
}
