use crate::protocol::*;
use crate::utils::{parse_partition_dir, validate_path_within_dir};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tsdb_arrow::engine::StorageEngine;

pub struct ParquetApi {
    parquet_dir: PathBuf,
    data_dir: PathBuf,
}

impl ParquetApi {
    pub fn new(_engine: Arc<dyn StorageEngine>, parquet_dir: PathBuf, data_dir: PathBuf) -> Self {
        Self { parquet_dir, data_dir }
    }

    #[cfg(test)]
    pub fn new_for_test(parquet_dir: PathBuf, data_dir: PathBuf) -> Self {
        Self { parquet_dir, data_dir }
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
                    if dir_name == "warm" || dir_name == "cold" || dir_name == "archive" || dir_name == "wal" || dir_name == "metadata" {
                        continue;
                    }
                    if dir_name.starts_with("data_") {
                        Self::scan_parquet_dir_recursive(
                            &partition_dir,
                            "active",
                            &mut files,
                            0,
                        );
                    }
                }
            }
        }

        if self.data_dir != self.parquet_dir && self.data_dir.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&self.data_dir) {
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
                    if dir_name == "warm" || dir_name == "cold" || dir_name == "archive" || dir_name == "wal" || dir_name == "metadata" {
                        continue;
                    }
                    if dir_name.starts_with("data_") {
                        Self::scan_parquet_dir_recursive(
                            &partition_dir,
                            "active",
                            &mut files,
                            0,
                        );
                    }
                }
            }
        }

        files.sort_by(|a, b| b.modified.cmp(&a.modified));
        files
    }

    pub fn file_detail(&self, path: &str) -> Result<ParquetFileInfo, String> {
        validate_path_within_dir(path, &self.parquet_dir)
            .or_else(|_| validate_path_within_dir(path, &self.data_dir))?;
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
        validate_path_within_dir(path, &self.parquet_dir)
            .or_else(|_| validate_path_within_dir(path, &self.data_dir))?;
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

        for tier in &["warm", "cold", "archive"] {
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

                    if dir_name.starts_with("data_") {
                        let measurement_dir = partition_dir.join(measurement);
                        if measurement_dir.is_dir() {
                            if let Ok(sub_entries) = std::fs::read_dir(&measurement_dir) {
                                for sub_entry in sub_entries.flatten() {
                                    let p = sub_entry.path();
                                    if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                                        paths.push(p.to_string_lossy().to_string());
                                    }
                                }
                            }
                        }
                    } else {
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
        }

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
                    if dir_name == "warm" || dir_name == "cold" || dir_name == "archive" || dir_name == "wal" || dir_name == "metadata" {
                        continue;
                    }
                    if !dir_name.starts_with("data_") {
                        continue;
                    }
                    let measurement_dir = partition_dir.join(measurement);
                    if measurement_dir.is_dir() {
                        if let Ok(sub_entries) = std::fs::read_dir(&measurement_dir) {
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

        if self.data_dir != self.parquet_dir && self.data_dir.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&self.data_dir) {
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
                    if dir_name == "warm" || dir_name == "cold" || dir_name == "archive" || dir_name == "wal" || dir_name == "metadata" {
                        continue;
                    }
                    if !dir_name.starts_with("data_") {
                        continue;
                    }
                    let measurement_dir = partition_dir.join(measurement);
                    if measurement_dir.is_dir() {
                        if let Ok(sub_entries) = std::fs::read_dir(&measurement_dir) {
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
        Self::scan_parquet_dir_recursive(dir, tier, files, 0);
    }

    fn scan_parquet_dir_recursive(
        dir: &std::path::Path,
        tier: &str,
        files: &mut Vec<ParquetFileInfo>,
        depth: usize,
    ) {
        if depth > 5 {
            return;
        }
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    Self::scan_parquet_dir_recursive(&path, tier, files, depth + 1);
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
        let mut found_tier = None;
        let mut after_tier = false;
        for component in path.components() {
            if let std::path::Component::Normal(os_str) = component {
                if let Some(s) = os_str.to_str() {
                    match s {
                        "warm" | "cold" | "archive" if found_tier.is_none() => {
                            found_tier = Some(s.to_string());
                            after_tier = true;
                        }
                        _ if after_tier && s.starts_with("data_") => {
                            return found_tier.unwrap();
                        }
                        _ => {}
                    }
                }
            }
        }
        if found_tier.is_some() {
            return found_tier.unwrap();
        }
        for ancestor in path.ancestors() {
            if let Some(name) = ancestor.file_name() {
                let name_str = name.to_string_lossy();
                if name_str.starts_with("data_") {
                    return "active".to_string();
                }
            }
        }
        "unknown".to_string()
    }

    fn extract_measurement_from_path(path: &Path) -> String {
        if let Some(parent) = path.parent() {
            if let Some(measurement_name) = parent.file_name() {
                let m_str = measurement_name.to_string_lossy();
                if !m_str.starts_with("data_")
                    && !m_str.starts_with("ts_")
                    && m_str != "warm"
                    && m_str != "cold"
                    && m_str != "archive"
                    && !m_str.is_empty()
                {
                    if let Some(grandparent) = parent.parent() {
                        if let Some(gp_name) = grandparent.file_name() {
                            let gp_str = gp_name.to_string_lossy();
                            if gp_str.starts_with("data_") {
                                return m_str.to_string();
                            }
                        }
                    }
                }
            }
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_real_parquet(dir: &std::path::Path, name: &str) -> PathBuf {
        use arrow::array::{Int64Array, StringArray, Float64Array, TimestampMicrosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join(name);
        let file = std::fs::File::create(&path).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("measurement", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![1000000i64, 2000000i64])),
                Arc::new(StringArray::from(vec!["cpu", "cpu"])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ],
        ).unwrap();

        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        path
    }

    fn setup_dirs(tmp: &TempDir) -> (PathBuf, PathBuf) {
        let parquet_dir = tmp.path().join("parquet");
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&parquet_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        (parquet_dir, data_dir)
    }

    #[test]
    fn test_detect_tier_from_path_warm() {
        let path = Path::new("/data/parquet/warm/data_20260401/cpu/part.parquet");
        assert_eq!(ParquetApi::detect_tier_from_path(path), "warm");
    }

    #[test]
    fn test_detect_tier_from_path_cold() {
        let path = Path::new("/data/parquet/cold/data_20260401/cpu/part.parquet");
        assert_eq!(ParquetApi::detect_tier_from_path(path), "cold");
    }

    #[test]
    fn test_detect_tier_from_path_archive() {
        let path = Path::new("/data/parquet/archive/data_20260401/cpu/part.parquet");
        assert_eq!(ParquetApi::detect_tier_from_path(path), "archive");
    }

    #[test]
    fn test_detect_tier_from_path_active() {
        let path = Path::new("/data/data_20260401/cpu/part.parquet");
        assert_eq!(ParquetApi::detect_tier_from_path(path), "active");
    }

    #[test]
    fn test_detect_tier_from_path_unknown() {
        let path = Path::new("/data/some_random_dir/file.parquet");
        assert_eq!(ParquetApi::detect_tier_from_path(path), "unknown");
    }

    #[test]
    fn test_extract_measurement_from_path_with_measurement_dir() {
        let path = Path::new("/data/data_20260401/cpu/part.parquet");
        assert_eq!(ParquetApi::extract_measurement_from_path(path), "cpu");
    }

    #[test]
    fn test_extract_measurement_from_path_with_ts_prefix() {
        let path = Path::new("/data/ts_cpu_20260401/part.parquet");
        assert_eq!(ParquetApi::extract_measurement_from_path(path), "cpu");
    }

    #[test]
    fn test_extract_measurement_from_path_no_measurement() {
        let path = Path::new("/data/random/part.parquet");
        assert_eq!(ParquetApi::extract_measurement_from_path(path), "");
    }

    #[test]
    fn test_list_parquet_files_empty() {
        let tmp = TempDir::new().unwrap();
        let (parquet_dir, data_dir) = setup_dirs(&tmp);

        let api = ParquetApi::new_for_test(parquet_dir, data_dir);
        let files = api.list_parquet_files();
        assert!(files.is_empty());
    }

    #[test]
    fn test_list_parquet_files_warm_and_cold() {
        let tmp = TempDir::new().unwrap();
        let (parquet_dir, data_dir) = setup_dirs(&tmp);

        write_real_parquet(&parquet_dir.join("warm/data_20260401/cpu"), "part1.parquet");
        write_real_parquet(&parquet_dir.join("cold/data_20260301/mem"), "part2.parquet");

        let api = ParquetApi::new_for_test(parquet_dir, data_dir);
        let files = api.list_parquet_files();

        assert_eq!(files.len(), 2);

        let warm_files: Vec<_> = files.iter().filter(|f| f.tier == "warm").collect();
        let cold_files: Vec<_> = files.iter().filter(|f| f.tier == "cold").collect();
        assert_eq!(warm_files.len(), 1);
        assert_eq!(cold_files.len(), 1);
    }

    #[test]
    fn test_list_parquet_files_active_tier() {
        let tmp = TempDir::new().unwrap();
        let (parquet_dir, data_dir) = setup_dirs(&tmp);

        write_real_parquet(&data_dir.join("data_20260401/cpu"), "part1.parquet");

        let api = ParquetApi::new_for_test(parquet_dir, data_dir);
        let files = api.list_parquet_files();

        let active_files: Vec<_> = files.iter().filter(|f| f.tier == "active").collect();
        assert_eq!(active_files.len(), 1);
    }

    #[test]
    fn test_file_detail_not_found() {
        let tmp = TempDir::new().unwrap();
        let (parquet_dir, data_dir) = setup_dirs(&tmp);

        let api = ParquetApi::new_for_test(parquet_dir, data_dir);
        let result = api.file_detail("/nonexistent/path/file.parquet");
        assert!(result.is_err());
    }

    #[test]
    fn test_preview_not_found() {
        let tmp = TempDir::new().unwrap();
        let (parquet_dir, data_dir) = setup_dirs(&tmp);

        let api = ParquetApi::new_for_test(parquet_dir, data_dir);
        let result = api.preview("/nonexistent/path/file.parquet", 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_parquet_paths_for_measurement() {
        let tmp = TempDir::new().unwrap();
        let (parquet_dir, data_dir) = setup_dirs(&tmp);

        write_real_parquet(&parquet_dir.join("warm/data_20260401/cpu"), "part1.parquet");
        write_real_parquet(&parquet_dir.join("cold/data_20260301/cpu"), "part2.parquet");

        let api = ParquetApi::new_for_test(parquet_dir, data_dir);
        let paths = api.parquet_paths_for_measurement("cpu");

        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn test_parquet_paths_for_measurement_not_found() {
        let tmp = TempDir::new().unwrap();
        let (parquet_dir, data_dir) = setup_dirs(&tmp);

        let api = ParquetApi::new_for_test(parquet_dir, data_dir);
        let paths = api.parquet_paths_for_measurement("nonexistent");
        assert!(paths.is_empty());
    }
}
