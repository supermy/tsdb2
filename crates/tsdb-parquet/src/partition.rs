use crate::error::{Result, TsdbParquetError};
use chrono::NaiveDate;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tracing::info;

/// 数据目录前缀
pub const CF_PREFIX: &str = "data_";
/// 元数据目录名
pub const METADATA_DIR: &str = "metadata";
/// 默认热数据保留天数
pub const DEFAULT_HOT_DAYS: u64 = 7;
/// 默认数据总保留天数
pub const DEFAULT_RETENTION_DAYS: u64 = 30;

/// 分区配置
#[derive(Debug, Clone)]
pub struct PartitionConfig {
    /// 热数据保留天数, 默认 7 天
    pub hot_days: u64,
    /// 数据总保留天数, 默认 30 天
    pub retention_days: u64,
    /// 目标文件大小 (字节), 默认 64MB
    pub target_file_size: usize,
    /// 行组大小, 默认 100 万行
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

/// 日期分区管理器
///
/// 按日期组织 Parquet 文件, 每个日期对应一个目录 `data_{YYYYMMDD}`。
/// 支持热/冷数据判定、过期分区清理、范围查询分区列表。
pub struct PartitionManager {
    /// 基础目录
    base_dir: PathBuf,
    /// 分区配置
    config: PartitionConfig,
    /// 已发现的分区 (线程安全, 按日期排序)
    known_partitions: Mutex<BTreeMap<NaiveDate, PartitionInfo>>,
}

/// 分区信息
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// 分区日期
    pub date: NaiveDate,
    /// 分区目录路径
    pub dir: PathBuf,
    /// Parquet 文件数量
    pub file_count: usize,
    /// 总文件大小 (字节)
    pub total_size: u64,
    /// 是否为热数据分区
    pub is_hot: bool,
}

impl PartitionManager {
    /// 创建分区管理器
    ///
    /// 自动创建基础目录和元数据目录, 并扫描已有分区。
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

        pm.discover_partitions()?;
        Ok(pm)
    }

    fn discover_partitions(&mut self) -> Result<()> {
        self.discover_partitions_impl()
    }

    /// 刷新分区列表 (重新扫描磁盘目录)
    pub fn refresh(&self) -> Result<()> {
        let entries = fs::read_dir(&self.base_dir)
            .map_err(|e| TsdbParquetError::Partition(format!("failed to read base dir: {}", e)))?;

        let mut partitions = BTreeMap::new();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(date_str) = name_str.strip_prefix(CF_PREFIX) {
                if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y%m%d") {
                    let dir = entry.path();
                    let (file_count, total_size) = count_parquet_files(&dir);
                    let is_hot = self.is_hot_date(date);
                    partitions.insert(
                        date,
                        PartitionInfo {
                            date,
                            dir,
                            file_count,
                            total_size,
                            is_hot,
                        },
                    );
                }
            }
        }

        *self
            .known_partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = partitions;
        Ok(())
    }

    fn discover_partitions_impl(&mut self) -> Result<()> {
        let entries = fs::read_dir(&self.base_dir)
            .map_err(|e| TsdbParquetError::Partition(format!("failed to read base dir: {}", e)))?;

        let mut partitions = BTreeMap::new();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(date_str) = name_str.strip_prefix(CF_PREFIX) {
                if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y%m%d") {
                    let dir = entry.path();
                    let (file_count, total_size) = count_parquet_files(&dir);
                    let is_hot = self.is_hot_date(date);
                    partitions.insert(
                        date,
                        PartitionInfo {
                            date,
                            dir,
                            file_count,
                            total_size,
                            is_hot,
                        },
                    );
                }
            }
        }

        *self.known_partitions.get_mut().unwrap() = partitions;
        Ok(())
    }

    /// 确保分区目录存在 (不存在则创建)
    pub fn ensure_partition(&self, date: NaiveDate) -> Result<PathBuf> {
        {
            let partitions = self
                .known_partitions
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(info) = partitions.get(&date) {
                return Ok(info.dir.clone());
            }
        }

        let dir_name = format!("{}{}", CF_PREFIX, date.format("%Y%m%d"));
        let dir = self.base_dir.join(&dir_name);
        fs::create_dir_all(&dir).map_err(|e| {
            TsdbParquetError::Partition(format!("failed to create partition dir: {}", e))
        })?;

        let is_hot = self.is_hot_date(date);
        let (file_count, total_size) = count_parquet_files(&dir);

        let info = PartitionInfo {
            date,
            dir: dir.clone(),
            file_count,
            total_size,
            is_hot,
        };

        self.known_partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(date, info);

        info!("created partition {} (hot={})", dir_name, is_hot);
        Ok(dir)
    }

    /// 获取分区目录路径
    pub fn get_partition_dir(&self, date: NaiveDate) -> Option<PathBuf> {
        self.known_partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&date)
            .map(|i| i.dir.clone())
    }

    /// 获取日期范围内的分区列表
    pub fn get_partitions_in_range(&self, start: NaiveDate, end: NaiveDate) -> Vec<PartitionInfo> {
        self.known_partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .range(start..=end)
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// 清理过期分区
    ///
    /// 删除早于 `now - retention_days` 的分区目录
    pub fn cleanup_expired(&self) -> Result<Vec<String>> {
        let cutoff = chrono::Local::now().date_naive()
            - chrono::Duration::days(self.config.retention_days as i64);

        let mut dropped = Vec::new();
        let expired: Vec<NaiveDate> = self
            .known_partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .filter(|&&d| d < cutoff)
            .copied()
            .collect();

        for date in expired {
            if let Some(info) = self
                .known_partitions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&date)
            {
                info!("dropping expired partition: {}", info.dir.display());
                if info.dir.exists() {
                    fs::remove_dir_all(&info.dir).map_err(|e| {
                        TsdbParquetError::Partition(format!("failed to remove partition: {}", e))
                    })?;
                }
                dropped.push(info.dir.to_string_lossy().to_string());
            }
        }

        Ok(dropped)
    }

    /// 列出指定日期分区内的所有 Parquet 文件
    pub fn list_parquet_files(&self, date: NaiveDate) -> Result<Vec<PathBuf>> {
        let dir = self.ensure_partition(date)?;
        let mut files = Vec::new();
        collect_parquet_files(&dir, &mut files);
        files.sort();
        Ok(files)
    }

    /// 获取元数据目录路径
    pub fn metadata_dir(&self) -> PathBuf {
        self.base_dir.join(METADATA_DIR)
    }

    /// 获取基础目录路径
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// 获取分区配置
    pub fn config(&self) -> &PartitionConfig {
        &self.config
    }

    /// 删除指定日期的分区
    pub fn remove_partition(&self, date: NaiveDate) -> Result<()> {
        if let Some(info) = self
            .known_partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&date)
        {
            if info.dir.exists() {
                fs::remove_dir_all(&info.dir).map_err(|e| {
                    TsdbParquetError::Partition(format!("failed to remove partition: {}", e))
                })?;
            }
        }
        Ok(())
    }

    pub fn list_measurements(&self) -> Vec<String> {
        let mut measurements = std::collections::HashSet::new();
        if let Ok(entries) = fs::read_dir(&self.base_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with(CF_PREFIX) {
                    let measurement = name.trim_start_matches(CF_PREFIX).to_string();
                    measurements.insert(measurement);
                }
            }
        }
        let mut result: Vec<String> = measurements.into_iter().collect();
        result.sort();
        result
    }

    /// 判断日期是否为热数据
    fn is_hot_date(&self, date: NaiveDate) -> bool {
        let today = chrono::Local::now().date_naive();
        let diff = (today - date).num_days();
        diff >= 0 && (diff as u64) <= self.config.hot_days
    }
}

/// 统计目录中的 Parquet 文件数量和总大小
fn count_parquet_files(dir: &Path) -> (usize, u64) {
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

/// 递归收集目录中的 Parquet 文件
fn collect_parquet_files(dir: &Path, files: &mut Vec<PathBuf>) {
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

/// 微秒时间戳转日期
///
/// 将微秒时间戳截断为秒, 再转换为 UTC 日期。
/// 转换失败时回退到当前本地日期。
pub fn micros_to_date(micros: i64) -> NaiveDate {
    let secs = micros / 1_000_000;
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|dt| dt.date_naive())
        .unwrap_or_else(|| chrono::Local::now().date_naive())
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
        let date = micros_to_date(ts);
        assert_eq!(date, today);
    }
}
