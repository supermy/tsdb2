use crate::protocol::*;
use crate::utils::{parse_partition_dir, validate_name};
use std::path::PathBuf;
use std::sync::Arc;
use tsdb_arrow::engine::StorageEngine;

pub struct LifecycleApi {
    engine: Arc<dyn StorageEngine>,
    parquet_dir: PathBuf,
}

impl LifecycleApi {
    pub fn new(engine: Arc<dyn StorageEngine>, parquet_dir: PathBuf) -> Self {
        Self {
            engine,
            parquet_dir,
        }
    }

    pub fn status(&self) -> LifecycleStatus {
        let any_ref = self.engine.as_any();
        let Some(rocksdb_engine) = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>() else {
            return LifecycleStatus {
                hot_cfs: vec![],
                warm_cfs: vec![],
                cold_cfs: vec![],
                archive_files: vec![],
                parquet_partitions: vec![],
                total_hot_bytes: 0,
                total_warm_bytes: 0,
                total_cold_bytes: 0,
                total_archive_bytes: 0,
            };
        };

        let db = rocksdb_engine.db();
        let rocksdb_dir = rocksdb_engine.base_dir();
        let ts_cfs = rocksdb_engine.list_ts_cfs();
        let today = chrono::Local::now().date_naive();

        let mut hot_cfs = Vec::new();
        let mut warm_cfs = Vec::new();
        let mut cold_cfs = Vec::new();
        let mut total_hot = 0u64;
        let mut total_warm = 0u64;
        let mut total_cold = 0u64;

        for cf_name in &ts_cfs {
            let date_str = Self::extract_date_from_cf(cf_name);
            let measurement = Self::extract_measurement_from_cf(cf_name);
            let age_days = Self::calc_age_days(&date_str, today);

            let sst_size = db
                .cf_handle(cf_name)
                .and_then(|cf| {
                    db.property_int_value_cf(&cf, rocksdb::properties::TOTAL_SST_FILES_SIZE)
                        .ok()
                        .flatten()
                })
                .unwrap_or(0);
            let num_keys = db
                .cf_handle(cf_name)
                .and_then(|cf| {
                    db.property_int_value_cf(&cf, rocksdb::properties::ESTIMATE_NUM_KEYS)
                        .ok()
                        .flatten()
                })
                .unwrap_or(0);

            let demote_eligible = if age_days > 14 {
                "cold".to_string()
            } else if age_days > 3 {
                "warm".to_string()
            } else {
                "none".to_string()
            };

            let info = DataTierInfo {
                cf_name: cf_name.clone(),
                measurement,
                date: date_str.unwrap_or_default(),
                age_days,
                sst_size,
                num_keys,
                tier: "hot".to_string(),
                path: rocksdb_dir.to_string_lossy().to_string(),
                storage: "rocksdb".to_string(),
                demote_eligible,
            };

            total_hot += sst_size;
            hot_cfs.push(info);
        }

        let warm_dir = self.parquet_dir.join("warm");
        if let Ok(entries) = std::fs::read_dir(&warm_dir) {
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
                let (measurement, date_str) = parse_partition_dir(&dir_name);
                let age_days = Self::calc_age_days(&Some(date_str.clone()), today);

                let mut total_size = 0u64;
                let mut num_keys = 0u64;
                if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                    for sub_entry in sub_entries.flatten() {
                        let p = sub_entry.path();
                        if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                            if let Ok(meta) = p.metadata() {
                                total_size += meta.len();
                            }
                            if let Ok(meta) = Self::read_parquet_row_count(&p) {
                                num_keys += meta;
                            }
                        }
                    }
                }

                total_warm += total_size;
                warm_cfs.push(DataTierInfo {
                    cf_name: dir_name.clone(),
                    measurement,
                    date: date_str,
                    age_days,
                    sst_size: total_size,
                    num_keys,
                    tier: "warm".to_string(),
                    path: warm_dir.join(&dir_name).to_string_lossy().to_string(),
                    storage: "parquet".to_string(),
                    demote_eligible: "cold".to_string(),
                });
            }
        }

        let cold_dir = self.parquet_dir.join("cold");
        if let Ok(entries) = std::fs::read_dir(&cold_dir) {
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
                let (measurement, date_str) = parse_partition_dir(&dir_name);
                let age_days = Self::calc_age_days(&Some(date_str.clone()), today);

                let mut total_size = 0u64;
                let mut num_keys = 0u64;
                if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                    for sub_entry in sub_entries.flatten() {
                        let p = sub_entry.path();
                        if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                            if let Ok(meta) = p.metadata() {
                                total_size += meta.len();
                            }
                            if let Ok(meta) = Self::read_parquet_row_count(&p) {
                                num_keys += meta;
                            }
                        }
                    }
                }

                total_cold += total_size;
                cold_cfs.push(DataTierInfo {
                    cf_name: dir_name.clone(),
                    measurement,
                    date: date_str,
                    age_days,
                    sst_size: total_size,
                    num_keys,
                    tier: "cold".to_string(),
                    path: cold_dir.join(&dir_name).to_string_lossy().to_string(),
                    storage: "parquet".to_string(),
                    demote_eligible: "none".to_string(),
                });
            }
        }

        let archive_dir = self.parquet_dir.join("archive");
        let mut archive_files = Vec::new();
        let mut total_archive = 0u64;

        if let Ok(entries) = std::fs::read_dir(&archive_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
                if ext == "parquet" || ext == "jsonl" {
                    if let Ok(meta) = entry.metadata() {
                        let modified = meta
                            .modified()
                            .ok()
                            .map(|t| {
                                let dt: chrono::DateTime<chrono::Utc> = t.into();
                                dt.format("%Y-%m-%d %H:%M:%S").to_string()
                            })
                            .unwrap_or_default();
                        total_archive += meta.len();
                        archive_files.push(ArchiveFileInfo {
                            name: path
                                .file_name()
                                .unwrap_or_default()
                                .to_string_lossy()
                                .to_string(),
                            size: meta.len(),
                            modified,
                        });
                    }
                }
            }
        }

        let mut parquet_partitions = Vec::new();
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
                    let (_, date_str) = parse_partition_dir(&dir_name);

                    let mut files = Vec::new();
                    let mut total_size = 0u64;
                    if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                        for sub_entry in sub_entries.flatten() {
                            let p = sub_entry.path();
                            if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                                if let Ok(meta) = p.metadata() {
                                    total_size += meta.len();
                                }
                                files.push(
                                    p.file_name()
                                        .unwrap_or_default()
                                        .to_string_lossy()
                                        .to_string(),
                                );
                            }
                        }
                    }

                    if !files.is_empty() {
                        parquet_partitions.push(ParquetPartitionInfo {
                            date: date_str,
                            files,
                            total_size,
                            tier: tier.to_string(),
                        });
                    }
                }
            }
        }

        LifecycleStatus {
            hot_cfs,
            warm_cfs,
            cold_cfs,
            archive_files,
            parquet_partitions,
            total_hot_bytes: total_hot,
            total_warm_bytes: total_warm,
            total_cold_bytes: total_cold,
            total_archive_bytes: total_archive,
        }
    }

    pub fn demote_to_warm(&self, cf_names: &[String]) -> Result<Vec<String>, String> {
        for cf_name in cf_names {
            validate_name(cf_name).map_err(|e| format!("invalid cf_name '{}': {}", cf_name, e))?;
        }
        let any_ref = self.engine.as_any();
        let rocksdb_engine = any_ref
            .downcast_ref::<tsdb_rocksdb::TsdbRocksDb>()
            .ok_or("Not using RocksDB engine")?;
        let warm_dir = self.parquet_dir.join("warm");

        let mut demoted = Vec::new();
        for cf_name in cf_names {
            if !rocksdb_engine.list_ts_cfs().contains(cf_name) {
                let partition_dir = warm_dir.join(cf_name);
                let marker = partition_dir.join(".demote_marker");
                if marker.exists() && partition_dir.is_dir() {
                    tracing::info!("found demote marker for {}, Parquet export was complete, attempting CF drop again", cf_name);
                    if rocksdb_engine.drop_cf(cf_name).is_ok() {
                        let _ = std::fs::remove_file(&marker);
                        demoted.push(format!("{} (recovered: dropped CF after crash)", cf_name));
                    }
                } else {
                    tracing::warn!("CF {} not found, skip", cf_name);
                }
                continue;
            }

            let partition_dir = warm_dir.join(cf_name);
            std::fs::create_dir_all(&partition_dir)
                .map_err(|e| format!("create warm dir: {}", e))?;

            let marker = partition_dir.join(".demote_marker");
            let _ = std::fs::write(&marker, "demoting_to_warm");

            match tsdb_rocksdb::DataArchiver::export_cf_to_parquet(
                rocksdb_engine,
                cf_name,
                &partition_dir,
                10000,
                parquet::basic::Compression::SNAPPY,
            ) {
                Ok(count) => {
                    tracing::info!("exported {} ({} points) to warm parquet", cf_name, count);

                    match rocksdb_engine.drop_cf(cf_name) {
                        Ok(_) => {
                            let _ = std::fs::remove_file(&marker);
                            tracing::info!("dropped CF {} from RocksDB (demoted to warm)", cf_name);
                            demoted.push(format!("{} ({} points → warm Parquet)", cf_name, count));
                        }
                        Err(e) => {
                            tracing::warn!("drop CF {} failed: {}", cf_name, e);
                            demoted.push(format!(
                                "{} ({} points → warm Parquet, but CF drop failed: {})",
                                cf_name, count, e
                            ));
                        }
                    }
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&marker);
                    tracing::warn!("export {} to warm parquet failed: {}", cf_name, e);
                }
            }
        }

        Ok(demoted)
    }

    pub fn demote_to_cold(&self, cf_names: &[String]) -> Result<Vec<String>, String> {
        for cf_name in cf_names {
            validate_name(cf_name).map_err(|e| format!("invalid cf_name '{}': {}", cf_name, e))?;
        }
        let any_ref = self.engine.as_any();
        let rocksdb_engine = any_ref
            .downcast_ref::<tsdb_rocksdb::TsdbRocksDb>()
            .ok_or("Not using RocksDB engine")?;
        let warm_dir = self.parquet_dir.join("warm");
        let cold_dir = self.parquet_dir.join("cold");
        std::fs::create_dir_all(&cold_dir).map_err(|e| format!("create cold dir: {}", e))?;

        let mut demoted = Vec::new();

        for cf_name in cf_names {
            if rocksdb_engine.list_ts_cfs().contains(cf_name) {
                let partition_dir = cold_dir.join(cf_name);
                std::fs::create_dir_all(&partition_dir)
                    .map_err(|e| format!("create cold dir: {}", e))?;

                let marker = partition_dir.join(".demote_marker");
                let _ = std::fs::write(&marker, "demoting_to_cold");

                match tsdb_rocksdb::DataArchiver::export_cf_to_parquet(
                    rocksdb_engine,
                    cf_name,
                    &partition_dir,
                    10000,
                    parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default()),
                ) {
                    Ok(count) => {
                        tracing::info!("exported {} ({} points) to cold parquet", cf_name, count);
                        match rocksdb_engine.drop_cf(cf_name) {
                            Ok(_) => {
                                let _ = std::fs::remove_file(&marker);
                                demoted
                                    .push(format!("{} ({} points → cold Parquet)", cf_name, count));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "drop CF {} after cold export failed: {}",
                                    cf_name,
                                    e
                                );
                                demoted.push(format!(
                                    "{} ({} points → cold Parquet, but CF drop failed: {})",
                                    cf_name, count, e
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        let _ = std::fs::remove_file(&marker);
                        tracing::warn!("export {} to cold parquet failed: {}", cf_name, e);
                    }
                }
                continue;
            }

            let warm_partition = warm_dir.join(cf_name);
            if warm_partition.is_dir() {
                let cold_partition = cold_dir.join(cf_name);
                std::fs::create_dir_all(&cold_partition)
                    .map_err(|e| format!("create cold partition dir: {}", e))?;

                let mut moved = 0u64;
                if let Ok(entries) = std::fs::read_dir(&warm_partition) {
                    for entry in entries.flatten() {
                        let src = entry.path();
                        let dst = cold_partition.join(src.file_name().unwrap_or_default());
                        let moved_ok = if std::fs::rename(&src, &dst).is_ok() {
                            true
                        } else {
                            std::fs::copy(&src, &dst)
                                .and_then(|_| std::fs::remove_file(&src))
                                .is_ok()
                        };
                        if moved_ok {
                            moved += 1;
                        }
                    }
                }

                if std::fs::remove_dir(&warm_partition).is_err() {
                    let _ = std::fs::remove_dir_all(&warm_partition);
                }

                tracing::info!("moved {} from warm to cold ({} files)", cf_name, moved);
                demoted.push(format!("{} ({} files warm → cold)", cf_name, moved));
                continue;
            }

            tracing::warn!("{} not found in RocksDB or warm Parquet, skip", cf_name);
        }

        Ok(demoted)
    }

    pub fn archive(&self, older_than_days: u64) -> Result<Vec<String>, String> {
        let any_ref = self.engine.as_any();
        let rocksdb_engine = any_ref
            .downcast_ref::<tsdb_rocksdb::TsdbRocksDb>()
            .ok_or("Not using RocksDB engine")?;
        let archive_dir = self.parquet_dir.join("archive");
        std::fs::create_dir_all(&archive_dir).map_err(|e| format!("create archive dir: {}", e))?;

        let today = chrono::Local::now().date_naive();
        let ts_cfs = rocksdb_engine.list_ts_cfs();
        let mut archived = Vec::new();

        for cf_name in &ts_cfs {
            if let Some(date_str) = Self::extract_date_from_cf(cf_name) {
                if let Ok(cf_date) = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d") {
                    let age = (today - cf_date).num_days() as u64;
                    if age > older_than_days {
                        let cf_archive_dir = archive_dir.join(cf_name);
                        std::fs::create_dir_all(&cf_archive_dir)
                            .map_err(|e| format!("create archive cf dir: {}", e))?;
                        match tsdb_rocksdb::DataArchiver::export_cf_to_parquet(
                            rocksdb_engine,
                            cf_name,
                            &cf_archive_dir,
                            10000,
                            parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default()),
                        ) {
                            Ok(count) => {
                                tracing::info!(
                                    "archived {} ({} points) to {}",
                                    cf_name,
                                    count,
                                    archive_dir.display()
                                );
                                match rocksdb_engine.drop_cf(cf_name) {
                                    Ok(_) => {
                                        archived.push(format!(
                                            "{} ({} points → archive)",
                                            cf_name, count
                                        ));
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            "drop CF {} after archive failed: {}",
                                            cf_name,
                                            e
                                        );
                                        archived.push(format!(
                                            "{} ({} points → archive, but CF drop failed: {})",
                                            cf_name, count, e
                                        ));
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!("archive {} failed: {}", cf_name, e);
                            }
                        }
                    }
                }
            }
        }

        Ok(archived)
    }

    fn read_parquet_row_count(path: &std::path::Path) -> Result<u64, String> {
        let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| e.to_string())?;
        let num_rows = builder.metadata().file_metadata().num_rows() as u64;
        Ok(num_rows)
    }

    fn calc_age_days(date_str: &Option<String>, today: chrono::NaiveDate) -> u64 {
        match date_str {
            Some(ds) if !ds.is_empty() => chrono::NaiveDate::parse_from_str(ds, "%Y%m%d")
                .map(|d| (today - d).num_days().max(0) as u64)
                .unwrap_or(0),
            _ => 0,
        }
    }

    pub fn extract_date_from_cf(cf_name: &str) -> Option<String> {
        let rest = cf_name.strip_prefix("ts_")?;
        let last_underscore = rest.rfind('_')?;
        let date_part = &rest[last_underscore + 1..];
        if date_part.len() == 8 && date_part.chars().all(|c| c.is_ascii_digit()) {
            Some(date_part.to_string())
        } else {
            None
        }
    }

    fn extract_measurement_from_cf(cf_name: &str) -> String {
        if let Some(rest) = cf_name.strip_prefix("ts_") {
            if let Some(last_underscore) = rest.rfind('_') {
                return rest[..last_underscore].to_string();
            }
        }
        cf_name.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_date_from_cf_valid() {
        assert_eq!(
            LifecycleApi::extract_date_from_cf("ts_cpu_20260401"),
            Some("20260401".to_string())
        );
        assert_eq!(
            LifecycleApi::extract_date_from_cf("ts_memory_usage_20251231"),
            Some("20251231".to_string())
        );
    }

    #[test]
    fn test_extract_date_from_cf_no_prefix() {
        assert_eq!(LifecycleApi::extract_date_from_cf("default"), None);
        assert_eq!(LifecycleApi::extract_date_from_cf("_meta"), None);
    }

    #[test]
    fn test_extract_date_from_cf_no_underscore() {
        assert_eq!(LifecycleApi::extract_date_from_cf("ts_something"), None);
    }

    #[test]
    fn test_extract_date_from_cf_non_digit_date() {
        assert_eq!(LifecycleApi::extract_date_from_cf("ts_cpu_abc"), None);
        assert_eq!(LifecycleApi::extract_date_from_cf("ts_cpu_2026abc"), None);
    }

    #[test]
    fn test_extract_date_from_cf_short_date() {
        assert_eq!(LifecycleApi::extract_date_from_cf("ts_cpu_202604"), None);
    }

    #[test]
    fn test_extract_measurement_from_cf_valid() {
        assert_eq!(
            LifecycleApi::extract_measurement_from_cf("ts_cpu_20260401"),
            "cpu"
        );
        assert_eq!(
            LifecycleApi::extract_measurement_from_cf("ts_memory_usage_20251231"),
            "memory_usage"
        );
    }

    #[test]
    fn test_extract_measurement_from_cf_no_prefix() {
        assert_eq!(
            LifecycleApi::extract_measurement_from_cf("default"),
            "default"
        );
    }

    #[test]
    fn test_calc_age_days_valid() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 4, 26).unwrap();
        assert_eq!(
            LifecycleApi::calc_age_days(&Some("20260420".to_string()), today),
            6
        );
        assert_eq!(
            LifecycleApi::calc_age_days(&Some("20260426".to_string()), today),
            0
        );
        assert_eq!(
            LifecycleApi::calc_age_days(&Some("20260101".to_string()), today),
            115
        );
    }

    #[test]
    fn test_calc_age_days_none() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 4, 26).unwrap();
        assert_eq!(LifecycleApi::calc_age_days(&None, today), 0);
    }

    #[test]
    fn test_calc_age_days_empty() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 4, 26).unwrap();
        assert_eq!(LifecycleApi::calc_age_days(&Some("".to_string()), today), 0);
    }

    #[test]
    fn test_calc_age_days_invalid_format() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 4, 26).unwrap();
        assert_eq!(
            LifecycleApi::calc_age_days(&Some("not-a-date".to_string()), today),
            0
        );
    }

    #[test]
    fn test_calc_age_days_future_date() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 4, 26).unwrap();
        assert_eq!(
            LifecycleApi::calc_age_days(&Some("20260501".to_string()), today),
            0
        );
    }
}
