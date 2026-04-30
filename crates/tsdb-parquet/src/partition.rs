use crate::error::{Result, TsdbParquetError};
use chrono::NaiveDate;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tracing::info;

pub const CF_PREFIX: &str = "data_";
pub const METADATA_DIR: &str = "metadata";
pub const DEFAULT_HOT_DAYS: u64 = 7;
pub const DEFAULT_RETENTION_DAYS: u64 = 30;
pub const TIER_ACTIVE: &str = "active";
pub const TIER_WARM: &str = "warm";
pub const TIER_COLD: &str = "cold";

#[derive(Debug, Clone)]
pub struct PartitionConfig {
    pub hot_days: u64,
    pub retention_days: u64,
    pub target_file_size: usize,
    pub row_group_size: usize,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            hot_days: DEFAULT_HOT_DAYS,
            retention_days: DEFAULT_RETENTION_DAYS,
            target_file_size: 64 * 1024 * 1024,
            row_group_size: 1_000_000,
        }
    }
}

type PartitionKey = (NaiveDate, String);

pub struct PartitionManager {
    base_dir: PathBuf,
    config: PartitionConfig,
    known_partitions: Mutex<BTreeMap<PartitionKey, PartitionInfo>>,
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub date: NaiveDate,
    pub dir: PathBuf,
    pub file_count: usize,
    pub total_size: u64,
    pub is_hot: bool,
    pub tier: String,
}

impl PartitionManager {
    pub fn new(base_dir: impl AsRef<Path>, config: PartitionConfig) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        fs::create_dir_all(&base_dir).map_err(|e| {
            TsdbParquetError::Partition(format!("failed to create base dir: {}", e))
        })?;

        let metadata_dir = base_dir.join(METADATA_DIR);
        fs::create_dir_all(&metadata_dir).map_err(|e| {
            TsdbParquetError::Partition(format!("failed to create metadata dir: {}", e))
        })?;

        let mut pm = Self {
            base_dir,
            config,
            known_partitions: Mutex::new(BTreeMap::new()),
        };

        pm.discover_partitions_impl()?;
        Ok(pm)
    }

    pub fn refresh(&self) -> Result<()> {
        let partitions = self.scan_all_partitions();
        *self
            .known_partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = partitions;
        Ok(())
    }

    fn scan_all_partitions(&self) -> BTreeMap<PartitionKey, PartitionInfo> {
        let mut partitions = BTreeMap::new();

        if let Ok(entries) = fs::read_dir(&self.base_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str == "warm"
                    || name_str == "cold"
                    || name_str == "archive"
                    || name_str == "wal"
                    || name_str == METADATA_DIR
                {
                    continue;
                }
                if let Some(date_str) = name_str.strip_prefix(CF_PREFIX) {
                    if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y%m%d") {
                        let dir = entry.path();
                        let (file_count, total_size) = count_parquet_files_recursive(&dir);
                        let is_hot = self.is_hot_date(date);
                        partitions.insert(
                            (date, TIER_ACTIVE.to_string()),
                            PartitionInfo {
                                date,
                                dir,
                                file_count,
                                total_size,
                                is_hot,
                                tier: TIER_ACTIVE.to_string(),
                            },
                        );
                    }
                }
            }
        }

        for tier_name in &[TIER_WARM, TIER_COLD, "archive"] {
            let tier_dir = self.base_dir.join(tier_name);
            if !tier_dir.is_dir() {
                continue;
            }
            if let Ok(entries) = fs::read_dir(&tier_dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name_str = name.to_string_lossy();
                    if let Some(date_str) = name_str.strip_prefix(CF_PREFIX) {
                        if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y%m%d") {
                            let dir = entry.path();
                            let (file_count, total_size) = count_parquet_files_recursive(&dir);
                            partitions.insert(
                                (date, tier_name.to_string()),
                                PartitionInfo {
                                    date,
                                    dir,
                                    file_count,
                                    total_size,
                                    is_hot: false,
                                    tier: tier_name.to_string(),
                                },
                            );
                        }
                    }
                }
            }
        }

        partitions
    }

    fn discover_partitions_impl(&mut self) -> Result<()> {
        let partitions = self.scan_all_partitions();
        *self.known_partitions.get_mut().unwrap_or_else(|e| {
            tracing::warn!("Mutex poisoned in scan_partitions, recovering: {}", e);
            e.into_inner()
        }) = partitions;
        Ok(())
    }

    pub fn ensure_partition(&self, date: NaiveDate) -> Result<PathBuf> {
        let key = (date, TIER_ACTIVE.to_string());
        {
            let partitions = self
                .known_partitions
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(info) = partitions.get(&key) {
                return Ok(info.dir.clone());
            }
        }

        let dir_name = format!("{}{}", CF_PREFIX, date.format("%Y%m%d"));
        let dir = self.base_dir.join(&dir_name);
        fs::create_dir_all(&dir).map_err(|e| {
            TsdbParquetError::Partition(format!("failed to create partition dir: {}", e))
        })?;

        let is_hot = self.is_hot_date(date);
        let (file_count, total_size) = count_parquet_files_recursive(&dir);

        let info = PartitionInfo {
            date,
            dir: dir.clone(),
            file_count,
            total_size,
            is_hot,
            tier: TIER_ACTIVE.to_string(),
        };

        self.known_partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, info);

        info!("created partition {} (hot={})", dir_name, is_hot);
        Ok(dir)
    }

    pub fn ensure_measurement_partition(
        &self,
        date: NaiveDate,
        measurement: &str,
    ) -> Result<PathBuf> {
        let date_dir = self.ensure_partition(date)?;
        let measurement_dir = date_dir.join(measurement);
        fs::create_dir_all(&measurement_dir).map_err(|e| {
            TsdbParquetError::Partition(format!(
                "failed to create measurement partition dir: {}",
                e
            ))
        })?;
        Ok(measurement_dir)
    }

    pub fn get_partition_dir(&self, date: NaiveDate) -> Option<PathBuf> {
        let key = (date, TIER_ACTIVE.to_string());
        self.known_partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .map(|i| i.dir.clone())
    }

    pub fn get_partitions_in_range(&self, start: NaiveDate, end: NaiveDate) -> Vec<PartitionInfo> {
        self.known_partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .range((start, String::new())..=(end, String::from("\x7f")))
            .map(|(_, v)| v.clone())
            .collect()
    }

    pub fn cleanup_expired(&self) -> Result<Vec<String>> {
        self.cleanup_expired_with_retention(self.config.retention_days)
    }

    pub fn cleanup_expired_with_retention(&self, retention_days: u64) -> Result<Vec<String>> {
        self.refresh()?;
        let today = chrono::Utc::now().date_naive();

        let mut dropped = Vec::new();
        let mut errors = Vec::new();
        let expired: Vec<PartitionKey> = self
            .known_partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .filter(|(d, tier)| {
                let tier_retention = match tier.as_str() {
                    TIER_ACTIVE => retention_days,
                    TIER_WARM => retention_days * 2,
                    TIER_COLD => retention_days * 4,
                    "archive" => u64::MAX,
                    _ => retention_days,
                };
                *d < today - chrono::Duration::days(tier_retention as i64)
            })
            .cloned()
            .collect();

        for key in expired {
            let info = self
                .known_partitions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&key)
                .cloned();
            if let Some(info) = info {
                info!("dropping expired partition: {}", info.dir.display());
                if info.dir.exists() {
                    if let Err(e) = fs::remove_dir_all(&info.dir) {
                        tracing::error!("failed to remove partition {}: {}", info.dir.display(), e);
                        errors.push(format!("{}: {}", info.dir.display(), e));
                        continue;
                    }
                }
                self.known_partitions
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&key);
                dropped.push(info.dir.to_string_lossy().to_string());
            }
        }

        if !errors.is_empty() {
            tracing::warn!("cleanup_expired completed with {} errors", errors.len());
        }
        Ok(dropped)
    }

    pub fn list_parquet_files(&self, date: NaiveDate) -> Result<Vec<PathBuf>> {
        let dir = self.ensure_partition(date)?;
        let mut files = Vec::new();
        collect_parquet_files(&dir, &mut files);
        files.sort();
        Ok(files)
    }

    pub fn list_measurement_parquet_files(
        &self,
        date: NaiveDate,
        measurement: &str,
    ) -> Result<Vec<PathBuf>> {
        let dir = self.ensure_partition(date)?;
        let measurement_dir = dir.join(measurement);
        if !measurement_dir.exists() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        collect_parquet_files(&measurement_dir, &mut files);
        files.sort();
        Ok(files)
    }

    pub fn list_measurements_for_date(&self, date: NaiveDate) -> Result<Vec<String>> {
        let dir = self.ensure_partition(date)?;
        let mut measurements = Vec::new();
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() && has_parquet_files(&path) {
                    if let Some(name) = entry.file_name().to_str() {
                        measurements.push(name.to_string());
                    }
                }
            }
        }
        measurements.sort();
        Ok(measurements)
    }

    pub fn metadata_dir(&self) -> PathBuf {
        self.base_dir.join(METADATA_DIR)
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn config(&self) -> &PartitionConfig {
        &self.config
    }

    pub fn remove_partition(&self, date: NaiveDate) -> Result<()> {
        let key = (date, TIER_ACTIVE.to_string());
        let info = self
            .known_partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .cloned();
        let info = match info {
            Some(i) => i,
            None => return Ok(()),
        };
        if info.dir.exists() {
            fs::remove_dir_all(&info.dir).map_err(|e| {
                TsdbParquetError::Partition(format!("failed to remove partition: {}", e))
            })?;
        }
        self.known_partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&key);
        Ok(())
    }

    pub fn list_measurements(&self) -> Vec<String> {
        let mut measurements = std::collections::HashSet::new();

        let mut scan_dir = |dir: &Path| {
            if let Ok(entries) = fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with(CF_PREFIX) {
                        let date_str = name.trim_start_matches(CF_PREFIX).to_string();
                        if NaiveDate::parse_from_str(&date_str, "%Y%m%d").is_ok() {
                            let date_dir = entry.path();
                            if let Ok(sub_entries) = fs::read_dir(&date_dir) {
                                for sub_entry in sub_entries.flatten() {
                                    if sub_entry.path().is_dir() {
                                        let m_name =
                                            sub_entry.file_name().to_string_lossy().to_string();
                                        let sub_path = sub_entry.path();
                                        if has_parquet_files(&sub_path) {
                                            measurements.insert(m_name);
                                        }
                                    }
                                }
                            }
                            let (file_count, _) = count_parquet_files_flat(&date_dir);
                            if file_count > 0 {
                                if let Ok(files) = fs::read_dir(&date_dir) {
                                    for file_entry in files.flatten() {
                                        let file_path = file_entry.path();
                                        if file_path
                                            .extension()
                                            .map(|e| e == "parquet")
                                            .unwrap_or(false)
                                        {
                                            if let Ok(file) = std::fs::File::open(&file_path) {
                                                if let Ok(builder) = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file) {
                                                    let arrow_schema = builder.schema();
                                                    if let Some(mf) = arrow_schema.metadata().get("measurement") {
                                                        measurements.insert(mf.clone());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        scan_dir(&self.base_dir);

        for tier in &[TIER_WARM, TIER_COLD, "archive"] {
            let tier_dir = self.base_dir.join(tier);
            if tier_dir.is_dir() {
                scan_dir(&tier_dir);
            }
        }

        let mut result: Vec<String> = measurements.into_iter().collect();
        result.sort();
        result
    }

    fn is_hot_date(&self, date: NaiveDate) -> bool {
        let today = chrono::Utc::now().date_naive();
        let diff = (today - date).num_days();
        diff >= 0 && (diff as u64) <= self.config.hot_days
    }
}

fn has_parquet_files(dir: &Path) -> bool {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                return true;
            }
        }
    }
    false
}

fn count_parquet_files_recursive(dir: &Path) -> (usize, u64) {
    let mut count = 0;
    let mut size = 0u64;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let (c, s) = count_parquet_files_recursive(&path);
                count += c;
                size += s;
            } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                count += 1;
                if let Ok(meta) = path.metadata() {
                    size += meta.len();
                }
            }
        }
    }
    (count, size)
}

fn count_parquet_files_flat(dir: &Path) -> (usize, u64) {
    let mut count = 0;
    let mut size = 0u64;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                count += 1;
                if let Ok(meta) = path.metadata() {
                    size += meta.len();
                }
            }
        }
    }
    (count, size)
}

pub fn collect_parquet_files(dir: &Path, files: &mut Vec<PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_parquet_files(&path, files);
            } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                files.push(path);
            }
        }
    }
}

pub fn micros_to_date(micros: i64) -> Result<NaiveDate> {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|dt| dt.date_naive())
        .ok_or_else(|| {
            crate::error::TsdbParquetError::Io(std::io::Error::other(format!(
                "invalid timestamp: {}",
                micros
            )))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_partition_create() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();

        let today = chrono::Utc::now().date_naive();
        let partition_dir = pm.ensure_partition(today).unwrap();
        assert!(partition_dir.exists());
        assert!(partition_dir.to_string_lossy().contains("data_"));
    }

    #[test]
    fn test_partition_idempotent() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();

        let today = chrono::Utc::now().date_naive();
        let d1 = pm.ensure_partition(today).unwrap();
        let d2 = pm.ensure_partition(today).unwrap();
        assert_eq!(d1, d2);
    }

    #[test]
    fn test_partition_range() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();

        let today = chrono::Utc::now().date_naive();
        let d1 = today - chrono::Duration::days(2);
        let d2 = today - chrono::Duration::days(1);
        let d3 = today;
        pm.ensure_partition(d1).unwrap();
        pm.ensure_partition(d2).unwrap();
        pm.ensure_partition(d3).unwrap();

        let range = pm.get_partitions_in_range(d1, d2);
        assert_eq!(range.len(), 2);
    }

    #[test]
    fn test_micros_to_date() {
        let today = chrono::Utc::now().date_naive();
        let ts = today
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        let date = micros_to_date(ts).unwrap();
        assert_eq!(date, today);
    }

    #[test]
    fn test_remove_partition() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();

        let today = chrono::Utc::now().date_naive();
        let partition_dir = pm.ensure_partition(today).unwrap();
        assert!(partition_dir.exists());

        pm.remove_partition(today).unwrap();
        assert!(
            !partition_dir.exists(),
            "partition directory should be deleted after remove"
        );

        let range = pm.get_partitions_in_range(today, today);
        assert!(
            range.is_empty(),
            "partition should be removed from known_partitions"
        );
    }

    #[test]
    fn test_remove_partition_nonexistent() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();

        let today = chrono::Utc::now().date_naive();
        let result = pm.remove_partition(today);
        assert!(
            result.is_ok(),
            "removing nonexistent partition should not error"
        );
    }

    #[test]
    fn test_cleanup_expired_continues_on_error() {
        let dir = TempDir::new().unwrap();
        let config = PartitionConfig {
            retention_days: 0,
            ..Default::default()
        };
        let pm = PartitionManager::new(dir.path(), config).unwrap();

        let old_date = chrono::Utc::now().date_naive() - chrono::Duration::days(10);
        let older_date = chrono::Utc::now().date_naive() - chrono::Duration::days(20);

        pm.ensure_partition(old_date).unwrap();
        pm.ensure_partition(older_date).unwrap();

        let dropped = pm.cleanup_expired().unwrap();
        assert_eq!(
            dropped.len(),
            2,
            "both expired partitions should be cleaned up"
        );
    }

    #[test]
    fn test_measurement_subdirectory() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();

        let today = chrono::Utc::now().date_naive();
        let cpu_dir = pm.ensure_measurement_partition(today, "cpu").unwrap();
        let mem_dir = pm.ensure_measurement_partition(today, "mem").unwrap();

        assert!(cpu_dir.exists());
        assert!(mem_dir.exists());
        assert_ne!(cpu_dir, mem_dir);
        assert!(cpu_dir.to_string_lossy().ends_with("/cpu"));
        assert!(mem_dir.to_string_lossy().ends_with("/mem"));

        let date_dir = pm.ensure_partition(today).unwrap();
        assert!(cpu_dir.starts_with(&date_dir));
        assert!(mem_dir.starts_with(&date_dir));
    }

    #[test]
    fn test_list_measurement_parquet_files_empty() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();

        let today = chrono::Utc::now().date_naive();
        let files = pm
            .list_measurement_parquet_files(today, "nonexistent")
            .unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_same_date_active_and_warm_not_overwrite() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();

        let today = chrono::Utc::now().date_naive();
        let active_dir = pm.ensure_partition(today).unwrap();

        let warm_dir_path = dir
            .path()
            .join("warm")
            .join(format!("data_{}", today.format("%Y%m%d")));
        std::fs::create_dir_all(&warm_dir_path).unwrap();
        let warm_cpu_dir = warm_dir_path.join("cpu");
        std::fs::create_dir_all(&warm_cpu_dir).unwrap();
        std::fs::write(warm_cpu_dir.join("test.parquet"), "fake").unwrap();

        pm.refresh().unwrap();

        let partitions = pm.get_partitions_in_range(today, today);
        assert_eq!(
            partitions.len(),
            2,
            "should have both active and warm partitions for the same date"
        );

        let active_partition = partitions.iter().find(|p| p.tier == TIER_ACTIVE);
        let warm_partition = partitions.iter().find(|p| p.tier == TIER_WARM);

        assert!(active_partition.is_some(), "active partition should exist");
        assert!(warm_partition.is_some(), "warm partition should exist");

        assert_eq!(active_partition.unwrap().dir, active_dir);
        assert_eq!(warm_partition.unwrap().dir, warm_dir_path);
    }

    #[test]
    fn test_same_date_all_tiers() {
        let dir = TempDir::new().unwrap();
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();

        let old_date = chrono::Utc::now().date_naive() - chrono::Duration::days(10);
        let _active_dir = pm.ensure_partition(old_date).unwrap();

        for tier in &["warm", "cold"] {
            let tier_dir_path = dir
                .path()
                .join(tier)
                .join(format!("data_{}", old_date.format("%Y%m%d")));
            std::fs::create_dir_all(&tier_dir_path).unwrap();
            let m_dir = tier_dir_path.join("mem");
            std::fs::create_dir_all(&m_dir).unwrap();
            std::fs::write(m_dir.join("test.parquet"), "fake").unwrap();
        }

        pm.refresh().unwrap();

        let partitions = pm.get_partitions_in_range(old_date, old_date);
        assert_eq!(
            partitions.len(),
            3,
            "should have active + warm + cold partitions for the same date"
        );

        let tiers: Vec<&str> = partitions.iter().map(|p| p.tier.as_str()).collect();
        assert!(tiers.contains(&TIER_ACTIVE));
        assert!(tiers.contains(&TIER_WARM));
        assert!(tiers.contains(&TIER_COLD));
    }
}
