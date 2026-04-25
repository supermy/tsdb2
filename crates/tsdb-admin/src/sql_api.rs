use crate::protocol::*;
use std::sync::Arc;
use tsdb_arrow::engine::StorageEngine;

pub struct SqlApi {
    engine: Arc<dyn StorageEngine>,
}

impl SqlApi {
    pub fn new(engine: Arc<dyn StorageEngine>) -> Self {
        Self { engine }
    }

    pub async fn execute(&self, sql: &str) -> Result<SqlResult, String> {
        let config = datafusion::execution::context::SessionConfig::new()
            .with_information_schema(true);
        let ctx = datafusion::prelude::SessionContext::new_with_config(config);

        let measurements = self.engine.list_measurements();
        for m in &measurements {
            if let Some(schema) = self.engine.measurement_schema(m) {
                let now = chrono::Utc::now().timestamp_micros();
                let start = now - 365 * 86_400_000_000i64;

                match self.engine.read_range_arrow(m, start, now, None) {
                    Ok(batches) if !batches.is_empty() => {
                        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches])
                            .map_err(|e| format!("create memtable for {}: {}", m, e))?;
                        if let Err(e) = ctx.register_table(m, Arc::new(mem_table)) {
                            tracing::debug!("register table {}: {}", m, e);
                        }
                    }
                    Ok(_) => {
                        let empty_batch = arrow::record_batch::RecordBatch::new_empty(schema);
                        let mem_table = datafusion::datasource::MemTable::try_new(
                            empty_batch.schema(),
                            vec![vec![empty_batch]],
                        )
                        .map_err(|e| format!("create empty memtable for {}: {}", m, e))?;
                        if let Err(e) = ctx.register_table(m, Arc::new(mem_table)) {
                            tracing::debug!("register empty table {}: {}", m, e);
                        }
                    }
                    Err(e) => {
                        tracing::debug!("read_range_arrow for {}: {}", m, e);
                    }
                }
            }
        }

        let start = std::time::Instant::now();
        let df = ctx.sql(sql).await.map_err(|e| format!("SQL parse error: {}", e))?;
        let batches = df.collect().await.map_err(|e| format!("SQL execute error: {}", e))?;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

        let mut columns = Vec::new();
        let mut rows: Vec<serde_json::Value> = Vec::new();

        for batch in &batches {
            if columns.is_empty() {
                columns = batch.schema().fields().iter().map(|f| f.name().clone()).collect();
            }
            for row_idx in 0..batch.num_rows() {
                if rows.len() >= 1000 {
                    break;
                }
                let mut map = serde_json::Map::new();
                for (col_idx, col_name) in columns.iter().enumerate() {
                    let val = Self::arrow_col_to_json(batch, col_idx, row_idx);
                    map.insert(col_name.clone(), val);
                }
                rows.push(serde_json::Value::Object(map));
            }
        }

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        Ok(SqlResult {
            sql: sql.to_string(),
            columns,
            rows,
            total_rows,
            elapsed_ms,
        })
    }

    pub async fn tables(&self) -> Result<Vec<String>, String> {
        Ok(self.engine.list_measurements())
    }

    fn arrow_col_to_json(batch: &arrow::record_batch::RecordBatch, col_idx: usize, row_idx: usize) -> serde_json::Value {
        let col = batch.column(col_idx);
        if row_idx >= col.len() {
            return serde_json::Value::Null;
        }

        use arrow::array::*;
        use arrow::datatypes::DataType;

        match col.data_type() {
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                if arr.is_null(row_idx) { serde_json::Value::Null }
                else {
                    let v = arr.value(row_idx);
                    if v.is_nan() { serde_json::Value::Null } else { serde_json::json!(v) }
                }
            }
            DataType::Float32 => {
                let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                if arr.is_null(row_idx) { serde_json::Value::Null }
                else {
                    let v = arr.value(row_idx);
                    if v.is_nan() { serde_json::Value::Null } else { serde_json::json!(v) }
                }
            }
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                if arr.is_null(row_idx) { serde_json::Value::Null }
                else { serde_json::json!(arr.value(row_idx)) }
            }
            DataType::Int32 => {
                let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                if arr.is_null(row_idx) { serde_json::Value::Null }
                else { serde_json::json!(arr.value(row_idx)) }
            }
            DataType::UInt64 => {
                let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                if arr.is_null(row_idx) { serde_json::Value::Null }
                else { serde_json::json!(arr.value(row_idx)) }
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                if arr.is_null(row_idx) { serde_json::Value::Null }
                else { serde_json::json!(arr.value(row_idx)) }
            }
            DataType::Boolean => {
                let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                if arr.is_null(row_idx) { serde_json::Value::Null }
                else { serde_json::json!(arr.value(row_idx)) }
            }
            DataType::Timestamp(unit, _) => {
                let ts_val = match unit {
                    arrow::datatypes::TimeUnit::Microsecond => {
                        let arr = col.as_any().downcast_ref::<arrow::array::PrimitiveArray<arrow::datatypes::TimestampMicrosecondType>>().unwrap();
                        if arr.is_null(row_idx) { return serde_json::Value::Null; }
                        arr.value(row_idx)
                    }
                    arrow::datatypes::TimeUnit::Millisecond => {
                        let arr = col.as_any().downcast_ref::<arrow::array::PrimitiveArray<arrow::datatypes::TimestampMillisecondType>>().unwrap();
                        if arr.is_null(row_idx) { return serde_json::Value::Null; }
                        arr.value(row_idx) * 1000
                    }
                    arrow::datatypes::TimeUnit::Second => {
                        let arr = col.as_any().downcast_ref::<arrow::array::PrimitiveArray<arrow::datatypes::TimestampSecondType>>().unwrap();
                        if arr.is_null(row_idx) { return serde_json::Value::Null; }
                        arr.value(row_idx) * 1_000_000
                    }
                    arrow::datatypes::TimeUnit::Nanosecond => {
                        let arr = col.as_any().downcast_ref::<arrow::array::PrimitiveArray<arrow::datatypes::TimestampNanosecondType>>().unwrap();
                        if arr.is_null(row_idx) { return serde_json::Value::Null; }
                        arr.value(row_idx) / 1000
                    }
                };
                let secs = ts_val / 1_000_000;
                let dt = chrono::DateTime::from_timestamp(secs, ((ts_val % 1_000_000) * 1000) as u32)
                    .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| ts_val.to_string());
                serde_json::json!(dt)
            }
            _ => serde_json::Value::Null,
        }
    }
}
