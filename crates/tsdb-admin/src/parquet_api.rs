use crate::protocol::*;
use crate::utils::{parse_partition_dir, validate_path_within_dir};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tsdb_arrow::engine::StorageEngine;

pub struct ParquetApi {
    parquet_dir: PathBuf,
}

impl ParquetApi {
    pub fn new(_engine: Arc<dyn StorageEngine>, parquet_dir: PathBuf) -> Self {
        Self { parquet_dir }
    }

    pub fn list_parquet_files(&self) -> Vec<ParquetFileInfo> {
        let mut files = Vec::new();

        let warm_dir = self.parquet_dir.join("warm");
        Self::scan_parquet_dir(&warm_dir, "warm", &mut files);

        let cold_dir = self.parquet_dir.join("cold");
        Self::scan_parquet_dir(&cold_dir, "cold", &mut files);

        let archive_dir = self.parquet_dir.join("archive");
        Self::scan_parquet_dir(&archive_dir, "archive", &mut files);

        if self.parquet_dir.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&self.parquet_dir) {
                for entry in entries.flatten() {
                    let partition_dir = entry.path();
                    if !partition_dir.is_dir() {
                        continue;
                    }
                    let dir_name = partition_dir
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string();
                    if dir_name == "warm" || dir_name == "cold" || dir_name == "archive" {
                        continue;
                    }
                    if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                        for sub_entry in sub_entries.flatten() {
                            let p = sub_entry.path();
                            if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                                if let Some(mut info) = Self::read_parquet_meta_internal(&p) {
                                    info.tier = "unknown".to_string();
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
        validate_path_within_dir(path, &self.parquet_dir)?;
        let path = Path::new(path);
        if !path.exists() {
            return Err(format!("File not found: {}", path.display()));
        }
        let mut info = self
            .read_parquet_meta(path)
            .ok_or_else(|| "Failed to read parquet metadata".to_string())?;
        info.tier = Self::detect_tier_from_path(path);
        info.measurement = Self::extract_measurement_from_path(path);
        Ok(info)
    }

    pub fn preview(&self, path: &str, limit: usize) -> Result<ParquetPreview, String> {
        validate_path_within_dir(path, &self.parquet_dir)?;
        let path = Path::new(path);
        if !path.exists() {
            return Err(format!("File not found: {}", path.display()));
        }

        let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| format!("open parquet: {}", e))?;

        let schema = builder.schema();
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

        let reader = builder
            .build()
            .map_err(|e| format!("build reader: {}", e))?;

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

    pub fn parquet_paths_for_measurement(&self, measurement: &str) -> Vec<String> {
        let mut paths = Vec::new();

        for tier in &["warm", "cold"] {
            let tier_dir = self.parquet_dir.join(tier);
            if let Ok(entries) = std::fs::read_dir(&tier_dir) {
                for entry in entries.flatten() {
                    let partition_dir = entry.path();
                    if !partition_dir.is_dir() {
                        continue;
                    }
                    let dir_name = partition_dir
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string();
                    let (m, _) = parse_partition_dir(&dir_name);
                    if m == measurement {
                        if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                            for sub_entry in sub_entries.flatten() {
                                let p = sub_entry.path();
                                if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                                    paths.push(p.to_string_lossy().to_string());
                                }
                            }
                        }
                    }
                }
            }
        }

        paths
    }

    fn scan_parquet_dir(dir: &std::path::Path, tier: &str, files: &mut Vec<ParquetFileInfo>) {
        if !dir.is_dir() {
            return;
        }
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if let Ok(sub_entries) = std::fs::read_dir(&path) {
                        for sub_entry in sub_entries.flatten() {
                            let p = sub_entry.path();
                            if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                                if let Some(mut info) = Self::read_parquet_meta_internal(&p) {
                                    info.tier = tier.to_string();
                                    info.measurement = Self::extract_measurement_from_path(&p);
                                    files.push(info);
                                }
                            }
                        }
                    }
                } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    if let Some(mut info) = Self::read_parquet_meta_internal(&path) {
                        info.tier = tier.to_string();
                        info.measurement = Self::extract_measurement_from_path(&path);
                        files.push(info);
                    }
                }
            }
        }
    }

    fn detect_tier_from_path(path: &Path) -> String {
        let path_str = path.to_string_lossy();
        if path_str.contains("parquet_data/warm/") {
            return "warm".to_string();
        }
        if path_str.contains("parquet_data/cold/") {
            return "cold".to_string();
        }
        if path_str.contains("/archive/") {
            return "archive".to_string();
        }
        "unknown".to_string()
    }

    fn extract_measurement_from_path(path: &Path) -> String {
        for ancestor in path.ancestors() {
            let name = ancestor
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            if name.starts_with("ts_") {
                let (m, _) = parse_partition_dir(&name);
                return m;
            }
        }
        String::new()
    }

    fn read_parquet_meta(&self, path: &Path) -> Option<ParquetFileInfo> {
        Self::read_parquet_meta_internal(path)
    }

    fn read_parquet_meta_internal(path: &Path) -> Option<ParquetFileInfo> {
        let metadata = std::fs::metadata(path).ok()?;
        let modified = metadata
            .modified()
            .ok()
            .map(|t| {
                let datetime: chrono::DateTime<chrono::Utc> = t.into();
                datetime.format("%Y-%m-%d %H:%M:%S").to_string()
            })
            .unwrap_or_default();

        let file = std::fs::File::open(path).ok()?;
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
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
            tier: String::new(),
            measurement: String::new(),
        })
    }

    fn arrow_value_to_json(
        batch: &arrow::array::RecordBatch,
        col_idx: usize,
        row_idx: usize,
    ) -> serde_json::Value {
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
        if let Some(arr) = col
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        {
            let ts = arr.value(row_idx);
            let secs = ts / 1_000_000;
            return serde_json::json!(chrono::DateTime::from_timestamp(secs, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| ts.to_string()));
        }
        if let Some(arr) = col
            .as_any()
            .downcast_ref::<arrow::array::TimestampSecondArray>()
        {
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
