use crate::error::{Result, TsdbParquetError};
use parquet::file::metadata::ParquetMetaData;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStats {
    pub file_path: String,
    pub measurement: String,
    pub date: String,
    pub tier: String,
    pub row_count: u64,
    pub size_bytes: u64,
    pub timestamp_min: Option<i64>,
    pub timestamp_max: Option<i64>,
    pub tags_hash_min: Option<u64>,
    pub tags_hash_max: Option<u64>,
    pub tag_values: HashMap<String, ValueStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueStats {
    pub min: Option<String>,
    pub max: Option<String>,
    pub null_count: u64,
}

pub fn extract_file_stats(
    file_path: &Path,
    measurement: &str,
    date: &str,
    tier: &str,
) -> Result<FileStats> {
    let file = File::open(file_path).map_err(|e| {
        TsdbParquetError::Io(std::io::Error::other(format!(
            "failed to open parquet file for stats: {}",
            e
        )))
    })?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let arrow_schema = builder.schema();
    let tag_columns: std::collections::HashSet<String> = arrow_schema
        .fields()
        .iter()
        .filter(|f| tsdb_arrow::schema::is_tag_field(f))
        .map(|f| f.name().clone())
        .collect();
    let metadata = builder.metadata();
    let stats = extract_stats_from_metadata(metadata, file_path, measurement, date, tier, &tag_columns);
    Ok(stats)
}

pub fn write_stats_file(parquet_path: &Path, stats: &FileStats) -> Result<PathBuf> {
    let stats_path = parquet_path.with_extension("stats.json");
    let tmp_path = parquet_path.with_extension("stats.json.tmp");
    let json = serde_json::to_string_pretty(stats)
        .map_err(TsdbParquetError::Serde)?;
    std::fs::write(&tmp_path, &json)?;
    std::fs::rename(&tmp_path, &stats_path)?;
    Ok(stats_path)
}

pub fn read_stats_file(stats_path: &Path) -> Result<FileStats> {
    let json = std::fs::read_to_string(stats_path)?;
    let stats: FileStats = serde_json::from_str(&json)?;
    Ok(stats)
}

pub fn extract_stats_from_metadata(
    metadata: &ParquetMetaData,
    file_path: &Path,
    measurement: &str,
    date: &str,
    tier: &str,
    tag_columns: &std::collections::HashSet<String>,
) -> FileStats {
    let mut found_ts = false;
    let mut found_th = false;
    let mut ts_min = 0i64;
    let mut ts_max = 0i64;
    let mut th_min = 0u64;
    let mut th_max = 0u64;
    let mut total_rows = 0u64;
    let mut tag_values: HashMap<String, ValueStats> = HashMap::new();

    for rg in metadata.row_groups() {
        total_rows += rg.num_rows() as u64;

        for col in rg.columns() {
            let col_path = col.column_path().string();
            if let Some(stats) = col.statistics() {
                let min_opt = stats.min_bytes_opt();
                let max_opt = stats.max_bytes_opt();
                if min_opt.is_none() && max_opt.is_none() {
                    continue;
                }
                match col_path.as_str() {
                    "timestamp" => {
                        if let Some(min_bs) = min_opt {
                            if min_bs.len() == 8 {
                                let v = i64::from_le_bytes(min_bs.try_into().unwrap_or([0; 8]));
                                ts_min = if found_ts { ts_min.min(v) } else { v };
                                found_ts = true;
                            }
                        }
                        if let Some(max_bs) = max_opt {
                            if max_bs.len() == 8 {
                                let v = i64::from_le_bytes(max_bs.try_into().unwrap_or([0; 8]));
                                ts_max = if found_ts { ts_max.max(v) } else { v };
                                found_ts = true;
                            }
                        }
                    }
                    "tags_hash" => {
                        if let Some(min_bs) = min_opt {
                            if min_bs.len() == 8 {
                                let v = u64::from_le_bytes(min_bs.try_into().unwrap_or([0; 8]));
                                th_min = if found_th { th_min.min(v) } else { v };
                                found_th = true;
                            }
                        }
                        if let Some(max_bs) = max_opt {
                            if max_bs.len() == 8 {
                                let v = u64::from_le_bytes(max_bs.try_into().unwrap_or([0; 8]));
                                th_max = if found_th { th_max.max(v) } else { v };
                                found_th = true;
                            }
                        }
                    }
                    _ => {
                        if tag_columns.contains(&col_path) {
                            let tag_key = col_path.strip_prefix("tag_").unwrap_or(&col_path);
                            let min_val = min_opt.and_then(|bs| String::from_utf8(bs.to_vec()).ok());
                            let max_val = max_opt.and_then(|bs| String::from_utf8(bs.to_vec()).ok());
                            let null_count = stats.null_count_opt().unwrap_or(0);

                            tag_values
                                .entry(tag_key.to_string())
                                .and_modify(|existing| {
                                    if let Some(ref min) = min_val {
                                        match &existing.min {
                                            Some(ex_min) if min >= ex_min => {}
                                            _ => existing.min = Some(min.clone()),
                                        }
                                    }
                                    if let Some(ref max) = max_val {
                                        match &existing.max {
                                            Some(ex_max) if max <= ex_max => {}
                                            _ => existing.max = Some(max.clone()),
                                        }
                                    }
                                    existing.null_count += null_count;
                                })
                                .or_insert_with(|| ValueStats {
                                    min: min_val,
                                    max: max_val,
                                    null_count,
                                });
                        }
                    }
                }
            }
        }
    }

    let file_size = std::fs::metadata(file_path).map(|m| m.len()).unwrap_or(0);

    FileStats {
        file_path: file_path.to_string_lossy().to_string(),
        measurement: measurement.to_string(),
        date: date.to_string(),
        tier: tier.to_string(),
        row_count: total_rows,
        size_bytes: file_size,
        timestamp_min: if found_ts { Some(ts_min) } else { None },
        timestamp_max: if found_ts { Some(ts_max) } else { None },
        tags_hash_min: if found_th { Some(th_min) } else { None },
        tags_hash_max: if found_th { Some(th_max) } else { None },
        tag_values,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::{PartitionConfig, PartitionManager};
    use crate::writer::{TsdbParquetWriter, WriteBufferConfig};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tsdb_arrow::schema::{DataPoint, FieldValue};

    #[test]
    fn test_extract_file_stats() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default(), "cpu");

        let dps: Vec<DataPoint> = (0..100)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", format!("server{:02}", i % 5))
                    .with_tag("region", if i % 2 == 0 { "east" } else { "west" })
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.01))
            })
            .collect();

        writer.write_batch(&dps).unwrap();
        let paths = writer.flush_all().unwrap();
        assert!(!paths.is_empty());

        let stats = extract_file_stats(&paths[0], "cpu", "20010101", "hot").unwrap();
        assert_eq!(stats.measurement, "cpu");
        assert!(stats.row_count > 0);
        assert!(stats.timestamp_min.is_some());
        assert!(stats.timestamp_max.is_some());
    }

    #[test]
    fn test_write_read_stats_file() {
        let dir = TempDir::new().unwrap();
        let stats = FileStats {
            file_path: "/tmp/test.parquet".to_string(),
            measurement: "cpu".to_string(),
            date: "20260427".to_string(),
            tier: "warm".to_string(),
            row_count: 1000,
            size_bytes: 4096,
            timestamp_min: Some(1000),
            timestamp_max: Some(2000),
            tags_hash_min: Some(100),
            tags_hash_max: Some(200),
            tag_values: {
                let mut m = HashMap::new();
                m.insert("host".to_string(), ValueStats {
                    min: Some("server01".to_string()),
                    max: Some("server99".to_string()),
                    null_count: 0,
                });
                m
            },
        };

        let parquet_path = dir.path().join("test.parquet");
        std::fs::write(&parquet_path, "").unwrap();
        let stats_path = write_stats_file(&parquet_path, &stats).unwrap();
        assert!(stats_path.exists());

        let loaded = read_stats_file(&stats_path).unwrap();
        assert_eq!(loaded.measurement, "cpu");
        assert_eq!(loaded.row_count, 1000);
        assert!(loaded.tag_values.contains_key("host"));
    }
}
