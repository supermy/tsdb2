use crate::error::{Result, TsdbParquetError};
use crate::file_stats::FileStats;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const MANIFEST_FILE: &str = "_manifest.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionManifest {
    pub measurement: String,
    pub date: String,
    pub tier: String,
    pub files: Vec<FileStats>,
}

impl PartitionManifest {
    pub fn new(measurement: &str, date: &str, tier: &str) -> Self {
        Self {
            measurement: measurement.to_string(),
            date: date.to_string(),
            tier: tier.to_string(),
            files: Vec::new(),
        }
    }

    pub fn manifest_path(partition_dir: &Path) -> PathBuf {
        partition_dir.join(MANIFEST_FILE)
    }

    pub fn read(partition_dir: &Path) -> Result<Self> {
        let path = Self::manifest_path(partition_dir);
        if !path.exists() {
            return Err(TsdbParquetError::Partition(format!(
                "manifest not found: {}",
                path.display()
            )));
        }
        let json = std::fs::read_to_string(&path)?;
        let manifest: PartitionManifest = serde_json::from_str(&json)?;
        Ok(manifest)
    }

    pub fn read_or_create(partition_dir: &Path, measurement: &str, date: &str, tier: &str) -> Result<Self> {
        let path = Self::manifest_path(partition_dir);
        if path.exists() {
            Self::read(partition_dir)
        } else {
            Ok(Self::new(measurement, date, tier))
        }
    }

    pub fn write(&self, partition_dir: &Path) -> Result<()> {
        let path = Self::manifest_path(partition_dir);
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    pub fn add_file(&mut self, stats: FileStats) {
        if let Some(existing) = self.files.iter_mut().find(|f| f.file_path == stats.file_path) {
            *existing = stats;
        } else {
            self.files.push(stats);
        }
    }

    pub fn remove_file(&mut self, file_path: &str) {
        self.files.retain(|f| f.file_path != file_path);
    }

    pub fn refresh_from_dir(
        partition_dir: &Path,
        measurement: &str,
        date: &str,
        tier: &str,
    ) -> Result<Self> {
        let mut manifest = Self::new(measurement, date, tier);

        let entries = std::fs::read_dir(partition_dir)
            .map_err(TsdbParquetError::Io)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                if let Ok(stats) = crate::file_stats::extract_file_stats(
                    &path, measurement, date, tier,
                ) {
                    manifest.files.push(stats);
                }
            }
        }

        manifest.files.sort_by(|a, b| a.file_path.cmp(&b.file_path));
        Ok(manifest)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestIndex {
    manifests: HashMap<String, PartitionManifest>,
}

impl ManifestIndex {
    pub fn new() -> Self {
        Self {
            manifests: HashMap::new(),
        }
    }

    pub fn load_from_base_dir(base_dir: &Path) -> Result<Self> {
        let mut index = Self::new();

        for tier in &["warm", "cold", "archive"] {
            let tier_dir = base_dir.join(tier);
            if !tier_dir.exists() {
                continue;
            }

            let measurement_entries = std::fs::read_dir(&tier_dir)
                .map_err(TsdbParquetError::Io)?;

            for measurement_entry in measurement_entries {
                let measurement_entry = measurement_entry?;
                let measurement_path = measurement_entry.path();
                if !measurement_path.is_dir() {
                    continue;
                }

                let date_entries = std::fs::read_dir(&measurement_path)
                    .map_err(TsdbParquetError::Io)?;

                for date_entry in date_entries {
                    let date_entry = date_entry?;
                    let date_path = date_entry.path();
                    if !date_path.is_dir() {
                        continue;
                    }

                    let date_str = date_path
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_default();

                    let measurement_str = measurement_path
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_default();

                    let key = format!("{}_{}_{}", tier, measurement_str, date_str);

                    if let Ok(manifest) = PartitionManifest::read_or_create(
                        &date_path,
                        &measurement_str,
                        &date_str,
                        tier,
                    ) {
                        index.manifests.insert(key, manifest);
                    }
                }
            }
        }

        Ok(index)
    }

    pub fn get_manifest(&self, tier: &str, measurement: &str, date: &str) -> Option<&PartitionManifest> {
        let key = format!("{}_{}_{}", tier, measurement, date);
        self.manifests.get(&key)
    }

    pub fn all_manifests(&self) -> Vec<&PartitionManifest> {
        self.manifests.values().collect()
    }

    pub fn files_for_measurement(&self, measurement: &str) -> Vec<&FileStats> {
        self.manifests
            .values()
            .filter(|m| m.measurement == measurement)
            .flat_map(|m| m.files.iter())
            .collect()
    }

    pub fn files_in_time_range(
        &self,
        measurement: &str,
        start_micros: i64,
        end_micros: i64,
    ) -> Vec<&FileStats> {
        self.files_for_measurement(measurement)
            .into_iter()
            .filter(|f| {
                f.timestamp_max.is_some_and(|max| max >= start_micros)
                    && f.timestamp_min.is_some_and(|min| min <= end_micros)
            })
            .collect()
    }
}

impl Default for ManifestIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_manifest_write_read() {
        let dir = TempDir::new().unwrap();
        let mut manifest = PartitionManifest::new("cpu", "20260427", "warm");
        manifest.add_file(FileStats {
            file_path: "cpu_20260427_000001.parquet".to_string(),
            measurement: "cpu".to_string(),
            date: "20260427".to_string(),
            tier: "warm".to_string(),
            row_count: 1000,
            size_bytes: 4096,
            timestamp_min: Some(1000),
            timestamp_max: Some(2000),
            tags_hash_min: Some(100),
            tags_hash_max: Some(200),
            tag_values: HashMap::new(),
        });

        manifest.write(dir.path()).unwrap();

        let loaded = PartitionManifest::read(dir.path()).unwrap();
        assert_eq!(loaded.measurement, "cpu");
        assert_eq!(loaded.files.len(), 1);
        assert_eq!(loaded.files[0].row_count, 1000);
    }

    #[test]
    fn test_manifest_add_remove() {
        let mut manifest = PartitionManifest::new("cpu", "20260427", "warm");
        manifest.add_file(FileStats {
            file_path: "a.parquet".to_string(),
            measurement: "cpu".to_string(),
            date: "20260427".to_string(),
            tier: "warm".to_string(),
            row_count: 100,
            size_bytes: 100,
            timestamp_min: None,
            timestamp_max: None,
            tags_hash_min: None,
            tags_hash_max: None,
            tag_values: HashMap::new(),
        });
        assert_eq!(manifest.files.len(), 1);

        manifest.add_file(FileStats {
            file_path: "a.parquet".to_string(),
            measurement: "cpu".to_string(),
            date: "20260427".to_string(),
            tier: "warm".to_string(),
            row_count: 200,
            size_bytes: 200,
            timestamp_min: None,
            timestamp_max: None,
            tags_hash_min: None,
            tags_hash_max: None,
            tag_values: HashMap::new(),
        });
        assert_eq!(manifest.files.len(), 1);
        assert_eq!(manifest.files[0].row_count, 200);

        manifest.remove_file("a.parquet");
        assert!(manifest.files.is_empty());
    }
}
