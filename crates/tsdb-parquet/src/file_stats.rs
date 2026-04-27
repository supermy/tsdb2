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
    pub timestamp_min: i64,
    pub timestamp_max: i64,
    pub tags_hash_min: u64,
    pub tags_hash_max: u64,
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
    let file_size = std::fs::metadata(file_path)
        .map(|m| m.len())
        .unwrap_or(0);

    let file = File::open(file_path).map_err(|e| {
        TsdbParquetError::Io(std::io::Error::other(format!(
            "failed to open parquet file for stats: {}",
            e
        )))
    })?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata();
    let stats = extract_stats_from_metadata(metadata, file_path, measurement, date, tier);
    Ok(stats)
}

pub fn write_stats_file(parquet_path: &Path, stats: &FileStats) -> Result<PathBuf> {
    let stats_path = parquet_path.with_extension("stats.json");
    let json = serde_json::to_string_pretty(stats)
        .map_err(|e| TsdbParquetError::Serde(e))?;
    std::fs::write(&stats_path, json)?;
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
) -> FileStats {
    let mut ts_min = i64::MAX;
    let mut ts_max = i64::MIN;
    let mut th_min = u64::MAX;
    let mut th_max = u64::MIN;
    let mut total_rows = 0u64;
    let mut tag_values: HashMap<String, ValueStats> = HashMap::new();

    for rg in metadata.row_groups() {
        total_rows += rg.num_rows() as u64;

        for col in rg.columns() {
            let col_path = col.column_path().string();
            if let Some(stats) = col.statistics() {
                if !stats.has_min_max_set() {
                    continue;
                }
                match col_path.as_str() {
                    "timestamp" => {
                        let min_bs = stats.min_bytes();
                        let max_bs = stats.max_bytes();
                        if min_bs.len() == 8 {
                            ts_min = ts_min.min(i64::from_le_bytes(min_bs.try_into().unwrap_or([0; 8])));
                        }
                        if max_bs.len() == 8 {
                            ts_max = ts_max.max(i64::from_le_bytes(max_bs.try_into().unwrap_or([0; 8])));
                        }
                    }
                    "tags_hash" => {
                        let min_bs = stats.min_bytes();
                        let max_bs = stats.max_bytes();
                        if min_bs.len() == 8 {
                            th_min = th_min.min(u64::from_le_bytes(min_bs.try_into().unwrap_or([0; 8])));
                        }
                        if max_bs.len() == 8 {
                            th_max = th_max.max(u64::from_le_bytes(max_bs.try_into().unwrap_or([0; 8])));
                        }
                    }
                    _ => {
                        if let Some(tag_key) = col_path.strip_prefix("tag_") {
                            let min_val = String::from_utf8(stats.min_bytes().to_vec()).ok();
                            let max_val = String::from_utf8(stats.max_bytes().to_vec()).ok();
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

    if ts_min == i64::MAX { ts_min = 0; }
    if ts_max == i64::MIN { ts_max = 0; }
    if th_min == u64::MAX { th_min = 0; }
    if th_max == u64::MIN { th_max = 0; }

    let file_size = std::fs::metadata(file_path).map(|m| m.len()).unwrap_or(0);

    FileStats {
        file_path: file_path.to_string_lossy().to_string(),
        measurement: measurement.to_string(),
        date: date.to_string(),
        tier: tier.to_string(),
        row_count: total_rows,
        size_bytes: file_size,
        timestamp_min: ts_min,
        timestamp_max: ts_max,
        tags_hash_min: th_min,
        tags_hash_max: th_max,
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
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default());

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
        assert!(stats.timestamp_min > 0);
        assert!(stats.timestamp_max > 0);
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
            timestamp_min: 1000,
            timestamp_max: 2000,
            tags_hash_min: 100,
            tags_hash_max: 200,
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
