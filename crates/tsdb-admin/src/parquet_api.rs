use crate::protocol::*;
use std::path::Path;
use std::sync::Arc;
use tsdb_arrow::engine::StorageEngine;

pub struct ParquetApi {
    engine: Arc<dyn StorageEngine>,
}

impl ParquetApi {
    pub fn new(engine: Arc<dyn StorageEngine>) -> Self {
        Self { engine }
    }

    pub fn list_parquet_files(&self) -> Vec<ParquetFileInfo> {
        let any_ref = self.engine.as_any();
        let Some(rocksdb_engine) = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>() else {
            return vec![];
        };
        let base_dir = rocksdb_engine.base_dir();
        let archive_dir = base_dir.join("archive");

        let mut files = Vec::new();

        if let Ok(entries) = std::fs::read_dir(&archive_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    if let Some(info) = self.read_parquet_meta(&path) {
                        files.push(info);
                    }
                }
            }
        }

        let data_dir = base_dir.join("parquet_data");
        if let Ok(entries) = std::fs::read_dir(&data_dir) {
            for entry in entries.flatten() {
                let partition_dir = entry.path();
                if partition_dir.is_dir() {
                    if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                        for sub_entry in sub_entries.flatten() {
                            let path = sub_entry.path();
                            if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                                if let Some(info) = self.read_parquet_meta(&path) {
                                    files.push(info);
                                }
                            }
                        }
                    }
                }
            }
        }

        files.sort_by(|a, b| b.modified.cmp(&a.modified));
        files
    }

    pub fn file_detail(&self, path: &str) -> Result<ParquetFileInfo, String> {
        let path = Path::new(path);
        if !path.exists() {
            return Err(format!("File not found: {}", path.display()));
        }
        self.read_parquet_meta(path).ok_or_else(|| "Failed to read parquet metadata".to_string())
    }

    pub fn preview(&self, path: &str, limit: usize) -> Result<ParquetPreview, String> {
        let path = Path::new(path);
        if !path.exists() {
            return Err(format!("File not found: {}", path.display()));
        }

        let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| format!("open parquet: {}", e))?;

        let schema = builder.schema();
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

        let reader = builder.build().map_err(|e| format!("build reader: {}", e))?;

        let mut rows = Vec::new();
        let mut total_rows = 0usize;
        for batch_result in reader {
            let batch = batch_result.map_err(|e| format!("read batch: {}", e))?;
            total_rows += batch.num_rows();
            for row_idx in 0..batch.num_rows() {
                if rows.len() >= limit {
                    break;
                }
                let mut row = serde_json::Map::new();
                for (col_idx, col_name) in columns.iter().enumerate() {
                    let val = Self::arrow_value_to_json(&batch, col_idx, row_idx);
                    row.insert(col_name.clone(), val);
                }
                rows.push(serde_json::Value::Object(row));
            }
            if rows.len() >= limit {
                break;
            }
        }

        Ok(ParquetPreview {
            path: path.to_string_lossy().to_string(),
            columns,
            rows,
            total_rows,
        })
    }

    fn read_parquet_meta(&self, path: &Path) -> Option<ParquetFileInfo> {
        let metadata = std::fs::metadata(path).ok()?;
        let modified = metadata.modified().ok()
            .map(|t| {
                let datetime: chrono::DateTime<chrono::Utc> = t.into();
                datetime.format("%Y-%m-%d %H:%M:%S").to_string()
            })
            .unwrap_or_default();

        let file = std::fs::File::open(path).ok()?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
        let parquet_meta = builder.metadata();
        let file_meta = parquet_meta.file_metadata();

        let num_rows = file_meta.num_rows() as usize;
        let num_row_groups = parquet_meta.num_row_groups();
        let created_by = file_meta.created_by().unwrap_or("unknown").to_string();

        let schema = builder.schema();
        let num_columns = schema.fields().len();

        let mut columns = Vec::new();
        for (i, field) in schema.fields().iter().enumerate() {
            let mut compressed = 0i64;
            let mut uncompressed = 0i64;
            for rg_idx in 0..num_row_groups {
                let rg = parquet_meta.row_group(rg_idx);
                if i < rg.num_columns() {
                    let col = rg.column(i);
                    compressed += col.compressed_size();
                    uncompressed += col.uncompressed_size();
                }
            }
            columns.push(ColumnMeta {
                name: field.name().clone(),
                data_type: format!("{:?}", field.data_type()),
                compressed_size: compressed,
                uncompressed_size: uncompressed,
            });
        }

        let compression = if num_row_groups > 0 && parquet_meta.row_group(0).num_columns() > 0 {
            let col = parquet_meta.row_group(0).column(0);
            format!("{:?}", col.compression())
        } else {
            "unknown".to_string()
        };

        Some(ParquetFileInfo {
            path: path.to_string_lossy().to_string(),
            file_size: metadata.len(),
            modified,
            num_rows,
            num_columns,
            num_row_groups,
            columns,
            compression,
            created_by,
        })
    }

    fn arrow_value_to_json(batch: &arrow::array::RecordBatch, col_idx: usize, row_idx: usize) -> serde_json::Value {
        let col = batch.column(col_idx);
        if row_idx >= col.len() || col.is_null(row_idx) {
            return serde_json::Value::Null;
        }
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
            return serde_json::json!(arr.value(row_idx));
        }
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
            return serde_json::json!(arr.value(row_idx));
        }
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
            return serde_json::json!(arr.value(row_idx));
        }
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
            return serde_json::json!(arr.value(row_idx));
        }
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Float32Array>() {
            return serde_json::json!(arr.value(row_idx));
        }
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
            return serde_json::json!(arr.value(row_idx));
        }
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::BooleanArray>() {
            return serde_json::json!(arr.value(row_idx));
        }
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>() {
            let ts = arr.value(row_idx);
            let secs = ts / 1_000_000;
            return serde_json::json!(chrono::DateTime::from_timestamp(secs, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| ts.to_string()));
        }
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::TimestampSecondArray>() {
            let ts = arr.value(row_idx);
            return serde_json::json!(chrono::DateTime::from_timestamp(ts, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| ts.to_string()));
        }
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Date32Array>() {
            let days = arr.value(row_idx);
            return serde_json::json!(chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                .map(|d| d.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| days.to_string()));
        }
        serde_json::json!(format!("{:?}", col.data_type()))
    }
}
