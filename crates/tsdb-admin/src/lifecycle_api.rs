use crate::protocol::*;
use crate::utils::{parse_partition_dir, validate_name};
use std::path::PathBuf;
use std::sync::Arc;
use tsdb_arrow::engine::StorageEngine;

pub struct LifecycleApi {
    engine: Arc<dyn StorageEngine>,
    parquet_dir: PathBuf,
    data_dir: PathBuf,
}

impl LifecycleApi {
    pub fn new(engine: Arc<dyn StorageEngine>, parquet_dir: PathBuf, data_dir: PathBuf) -> Self {
        Self {
            engine,
            parquet_dir,
            data_dir,
        }
    }

    #[cfg(test)]
    pub fn new_for_test(parquet_dir: PathBuf, data_dir: PathBuf) -> Self {
        use tsdb_arrow::engine::EngineResult;
        use tsdb_arrow::schema::{DataPoint, Tags};

        struct MockEngine;

        impl tsdb_arrow::StorageEngine for MockEngine {
            fn write(&self, _dp: &DataPoint) -> EngineResult<()> { Ok(()) }
            fn write_batch(&self, _datapoints: &[DataPoint]) -> EngineResult<()> { Ok(()) }
            fn read_range(&self, _measurement: &str, _tags: &Tags, _start: i64, _end: i64) -> EngineResult<Vec<DataPoint>> { Ok(vec![]) }
            fn get_point(&self, _measurement: &str, _tags: &Tags, _timestamp: i64) -> EngineResult<Option<DataPoint>> { Ok(None) }
            fn list_measurements(&self) -> Vec<String> { vec![] }
            fn flush(&self) -> EngineResult<()> { Ok(()) }
            fn as_any(&self) -> &dyn std::any::Any { self }
        }

        Self {
            engine: Arc::new(MockEngine),
            parquet_dir,
            data_dir,
        }
    }

    pub fn status(&self) -> LifecycleStatus {
        let any_ref = self.engine.as_any();

        if let Some(rocksdb_engine) = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>() {
            self.status_rocksdb(rocksdb_engine)
        } else if let Some(arrow_engine) =
            any_ref.downcast_ref::<tsdb_storage_arrow::ArrowStorageEngine>()
        {
            self.status_arrow(arrow_engine)
        } else {
            LifecycleStatus {
                engine_type: "unknown".to_string(),
                hot_cfs: vec![],
                warm_cfs: vec![],
                cold_cfs: vec![],
                archive_files: vec![],
                parquet_partitions: vec![],
                total_hot_bytes: 0,
                total_warm_bytes: 0,
                total_cold_bytes: 0,
                total_archive_bytes: 0,
            }
        }
    }

    fn status_rocksdb(&self, rocksdb_engine: &tsdb_rocksdb::TsdbRocksDb) -> LifecycleStatus {
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

        if archive_dir.is_dir() {
            Self::scan_archive_dir_recursive(&archive_dir, &mut archive_files, &mut total_archive);
        }

        let mut parquet_partitions = Vec::new();
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
                    let (_, date_str) = parse_partition_dir(&dir_name);

                    let mut files = Vec::new();
                    let mut total_size = 0u64;
                    if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                        for sub_entry in sub_entries.flatten() {
                            let sub_path = sub_entry.path();
                            if sub_path.is_dir() {
                                if let Ok(measurements) = std::fs::read_dir(&sub_path) {
                                    for m_entry in measurements.flatten() {
                                        let p = m_entry.path();
                                        if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                                            if let Ok(meta) = p.metadata() {
                                                total_size += meta.len();
                                            }
                                            files.push(p.file_name().unwrap_or_default().to_string_lossy().to_string());
                                        }
                                    }
                                }
                            } else if sub_path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                                if let Ok(meta) = sub_path.metadata() {
                                    total_size += meta.len();
                                }
                                files.push(sub_path.file_name().unwrap_or_default().to_string_lossy().to_string());
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
            engine_type: "rocksdb".to_string(),
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

    fn status_arrow(&self, _arrow_engine: &tsdb_storage_arrow::ArrowStorageEngine) -> LifecycleStatus {
        self.status_arrow_from_fs()
    }

    fn status_arrow_from_fs(&self) -> LifecycleStatus {
        let arrow_base = &self.data_dir;
        let mut active_cfs = Vec::new();
        let mut total_active = 0u64;
        let today = chrono::Local::now().date_naive();

        if let Ok(entries) = std::fs::read_dir(arrow_base) {
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

                let date_str = dir_name.trim_start_matches("data_").to_string();
                let age_days = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d")
                    .map(|d| (today - d).num_days().max(0) as u64)
                    .unwrap_or(0);

                if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                    for sub_entry in sub_entries.flatten() {
                        let sub_path = sub_entry.path();
                        if sub_path.is_dir() {
                            let m_name = sub_entry.file_name().to_string_lossy().to_string();
                            let mut total_size = 0u64;
                            let mut num_keys = 0u64;
                            if let Ok(files) = std::fs::read_dir(&sub_path) {
                                for file_entry in files.flatten() {
                                    let p = file_entry.path();
                                    if p.extension().map(|e| e == "parquet").unwrap_or(false) {
                                        if let Ok(meta) = p.metadata() {
                                            total_size += meta.len();
                                        }
                                        if let Ok(meta) = Self::read_parquet_row_count(&p) {
                                            num_keys += meta;
                                        }
                                    }
                                }
                            }
                            total_active += total_size;
                            active_cfs.push(DataTierInfo {
                                cf_name: format!("{}_{}", m_name, date_str),
                                measurement: m_name,
                                date: date_str.clone(),
                                age_days,
                                sst_size: total_size,
                                num_keys,
                                tier: "active".to_string(),
                                path: partition_dir.to_string_lossy().to_string(),
                                storage: "parquet".to_string(),
                                demote_eligible: if age_days > 14 { "cold".to_string() } else if age_days > 3 { "warm".to_string() } else { "none".to_string() },
                            });
                        } else if sub_path.extension().map(|e| e == "parquet").unwrap_or(false) {
                            let mut file_size = 0u64;
                            let mut num_keys = 0u64;
                            if let Ok(meta) = sub_path.metadata() {
                                file_size = meta.len();
                                total_active += file_size;
                            }
                            if let Ok(count) = Self::read_parquet_row_count(&sub_path) {
                                num_keys = count;
                            }
                            let measurement_name = Self::read_parquet_measurement(&sub_path)
                                .unwrap_or_else(|| "unknown".to_string());
                            active_cfs.push(DataTierInfo {
                                cf_name: format!("{}_{}", measurement_name, date_str),
                                measurement: measurement_name,
                                date: date_str.clone(),
                                age_days,
                                sst_size: file_size,
                                num_keys,
                                tier: "active".to_string(),
                                path: sub_path.to_string_lossy().to_string(),
                                storage: "parquet".to_string(),
                                demote_eligible: if age_days > 14 { "cold".to_string() } else if age_days > 3 { "warm".to_string() } else { "none".to_string() },
                            });
                        }
                    }
                }
            }
        }

        let mut warm_cfs = Vec::new();
        let mut total_warm = 0u64;
        let warm_dir = arrow_base.join("warm");
        if let Ok(entries) = std::fs::read_dir(&warm_dir) {
            for entry in entries.flatten() {
                let partition_dir = entry.path();
                if !partition_dir.is_dir() {
                    continue;
                }
                let dir_name = partition_dir.file_name().unwrap_or_default().to_string_lossy().to_string();
                if !dir_name.starts_with("data_") {
                    continue;
                }
                let date_str = dir_name.trim_start_matches("data_").to_string();
                let age_days = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d")
                    .map(|d| (today - d).num_days().max(0) as u64)
                    .unwrap_or(0);

                if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                    for sub_entry in sub_entries.flatten() {
                        let sub_path = sub_entry.path();
                        if sub_path.is_dir() {
                            let m_name = sub_entry.file_name().to_string_lossy().to_string();
                            let mut total_size = 0u64;
                            let mut num_keys = 0u64;
                            if let Ok(files) = std::fs::read_dir(&sub_path) {
                                for file_entry in files.flatten() {
                                    let p = file_entry.path();
                                    if p.extension().map(|e| e == "parquet").unwrap_or(false) {
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
                                cf_name: format!("{}_{}", m_name, date_str),
                                measurement: m_name,
                                date: date_str.clone(),
                                age_days,
                                sst_size: total_size,
                                num_keys,
                                tier: "warm".to_string(),
                                path: partition_dir.to_string_lossy().to_string(),
                                storage: "parquet".to_string(),
                                demote_eligible: "cold".to_string(),
                            });
                        }
                    }
                }
            }
        }

        let mut cold_cfs = Vec::new();
        let mut total_cold = 0u64;
        let cold_dir = arrow_base.join("cold");
        if let Ok(entries) = std::fs::read_dir(&cold_dir) {
            for entry in entries.flatten() {
                let partition_dir = entry.path();
                if !partition_dir.is_dir() {
                    continue;
                }
                let dir_name = partition_dir.file_name().unwrap_or_default().to_string_lossy().to_string();
                if !dir_name.starts_with("data_") {
                    continue;
                }
                let date_str = dir_name.trim_start_matches("data_").to_string();
                let age_days = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d")
                    .map(|d| (today - d).num_days().max(0) as u64)
                    .unwrap_or(0);

                if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                    for sub_entry in sub_entries.flatten() {
                        let sub_path = sub_entry.path();
                        if sub_path.is_dir() {
                            let m_name = sub_entry.file_name().to_string_lossy().to_string();
                            let mut total_size = 0u64;
                            let mut num_keys = 0u64;
                            if let Ok(files) = std::fs::read_dir(&sub_path) {
                                for file_entry in files.flatten() {
                                    let p = file_entry.path();
                                    if p.extension().map(|e| e == "parquet").unwrap_or(false) {
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
                                cf_name: format!("{}_{}", m_name, date_str),
                                measurement: m_name,
                                date: date_str.clone(),
                                age_days,
                                sst_size: total_size,
                                num_keys,
                                tier: "cold".to_string(),
                                path: partition_dir.to_string_lossy().to_string(),
                                storage: "parquet".to_string(),
                                demote_eligible: "none".to_string(),
                            });
                        }
                    }
                }
            }
        }

        let archive_dir = arrow_base.join("archive");
        let mut archive_files = Vec::new();
        let mut total_archive = 0u64;
        if archive_dir.is_dir() {
            Self::scan_archive_dir_recursive(&archive_dir, &mut archive_files, &mut total_archive);
        }

        let mut parquet_partitions = Vec::new();
        for tier in &["warm", "cold", "archive"] {
            let tier_dir = arrow_base.join(tier);
            if let Ok(entries) = std::fs::read_dir(&tier_dir) {
                for entry in entries.flatten() {
                    let partition_dir = entry.path();
                    if !partition_dir.is_dir() {
                        continue;
                    }
                    let dir_name = partition_dir.file_name().unwrap_or_default().to_string_lossy().to_string();
                    if !dir_name.starts_with("data_") {
                        continue;
                    }
                    let date_str = dir_name.trim_start_matches("data_").to_string();

                    let mut files = Vec::new();
                    let mut total_size = 0u64;
                    if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                        for sub_entry in sub_entries.flatten() {
                            let sub_path = sub_entry.path();
                            if sub_path.is_dir() {
                                if let Ok(measurements) = std::fs::read_dir(&sub_path) {
                                    for m_entry in measurements.flatten() {
                                        let p = m_entry.path();
                                        if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                                            if let Ok(meta) = p.metadata() {
                                                total_size += meta.len();
                                            }
                                            files.push(p.file_name().unwrap_or_default().to_string_lossy().to_string());
                                        }
                                    }
                                }
                            } else if sub_path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                                if let Ok(meta) = sub_path.metadata() {
                                    total_size += meta.len();
                                }
                                files.push(sub_path.file_name().unwrap_or_default().to_string_lossy().to_string());
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
            engine_type: "arrow".to_string(),
            hot_cfs: active_cfs,
            warm_cfs,
            cold_cfs,
            archive_files,
            parquet_partitions,
            total_hot_bytes: total_active,
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

        if let Some(rocksdb_engine) = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>() {
            self.demote_to_warm_rocksdb(rocksdb_engine, cf_names)
        } else if let Some(arrow_engine) = any_ref.downcast_ref::<tsdb_storage_arrow::ArrowStorageEngine>() {
            let result = self.demote_to_warm_arrow(cf_names);
            if let Ok(ref demoted) = result {
                if !demoted.is_empty() {
                    if let Err(e) = arrow_engine.refresh_partitions() {
                        tracing::warn!("failed to refresh partitions after demote to warm: {}", e);
                    }
                }
            }
            result
        } else {
            Err("Unknown engine type".to_string())
        }
    }

    fn demote_to_warm_rocksdb(&self, rocksdb_engine: &tsdb_rocksdb::TsdbRocksDb, cf_names: &[String]) -> Result<Vec<String>, String> {
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
            if let Err(e) = std::fs::write(&marker, "demoting_to_warm") {
                tracing::warn!("failed to write demote marker for {}: {}", cf_name, e);
            }

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

    fn demote_to_warm_arrow(&self, cf_names: &[String]) -> Result<Vec<String>, String> {
        let warm_dir = self.data_dir.join("warm");
        std::fs::create_dir_all(&warm_dir).map_err(|e| format!("create warm dir: {}", e))?;

        let mut demoted = Vec::new();
        for cf_name in cf_names {
            let parts: Vec<&str> = cf_name.rsplitn(2, '_').collect();
            if parts.len() < 2 {
                tracing::warn!("invalid cf_name format '{}', expected measurement_date", cf_name);
                continue;
            }
            let date_str = parts[0];
            let measurement = parts[1];
            if !date_str.chars().all(|c| c.is_ascii_digit()) || date_str.len() != 8 {
                tracing::warn!("invalid date in cf_name '{}'", cf_name);
                continue;
            }

            let hot_partition = self.data_dir.join(format!("data_{}", date_str));
            let warm_partition = warm_dir.join(format!("data_{}", date_str));
            let warm_measurement_dir = warm_partition.join(measurement);

            if !hot_partition.is_dir() {
                let marker = warm_measurement_dir.join(".demote_marker");
                if marker.exists() && warm_measurement_dir.is_dir() {
                    tracing::info!("found demote marker for {}, hot partition already removed, cleaning up marker", cf_name);
                    let _ = std::fs::remove_file(&marker);
                    demoted.push(format!("{} (recovered: completed demote after crash)", cf_name));
                } else {
                    tracing::warn!("hot partition not found for {}", cf_name);
                }
                continue;
            }

            std::fs::create_dir_all(&warm_partition)
                .map_err(|e| format!("create warm partition dir: {}", e))?;

            let hot_measurement_dir = hot_partition.join(measurement);

            if !hot_measurement_dir.is_dir() {
                tracing::warn!("measurement dir not found: {:?}", hot_measurement_dir);
                continue;
            }

            std::fs::create_dir_all(&warm_measurement_dir)
                .map_err(|e| format!("create warm measurement dir: {}", e))?;
            let marker = warm_measurement_dir.join(".demote_marker");
            if let Err(e) = std::fs::write(&marker, "demoting_to_warm") {
                tracing::warn!("failed to write demote marker: {}", e);
            }

            let mut moved = 0u64;
            if let Ok(entries) = std::fs::read_dir(&hot_measurement_dir) {
                for entry in entries.flatten() {
                    let src = entry.path();
                    if src.extension().and_then(|e| e.to_str()) != Some("parquet") {
                        continue;
                    }
                    let file_name = src.file_name().unwrap_or_default();
                    let dst = warm_measurement_dir.join(file_name);
                    if Self::safe_move_file(&src, &dst).is_ok() {
                        moved += 1;
                    }
                }
            }

            let _ = std::fs::remove_file(&marker);
            if std::fs::read_dir(&hot_measurement_dir).map(|mut d| d.next().is_none()).unwrap_or(true) {
                let _ = std::fs::remove_dir(&hot_measurement_dir);
            }

            let hot_partition = self.data_dir.join(format!("data_{}", date_str));
            if hot_partition.is_dir()
                && std::fs::read_dir(&hot_partition).map(|mut d| d.next().is_none()).unwrap_or(true)
            {
                let _ = std::fs::remove_dir(&hot_partition);
            }

            tracing::info!("demoted {} from hot to warm ({} files)", cf_name, moved);
            demoted.push(format!("{} ({} files hot → warm)", cf_name, moved));
        }

        Ok(demoted)
    }

    pub fn demote_to_cold(&self, cf_names: &[String]) -> Result<Vec<String>, String> {
        for cf_name in cf_names {
            validate_name(cf_name).map_err(|e| format!("invalid cf_name '{}': {}", cf_name, e))?;
        }
        let any_ref = self.engine.as_any();

        if let Some(rocksdb_engine) = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>() {
            self.demote_to_cold_rocksdb(rocksdb_engine, cf_names)
        } else if let Some(arrow_engine) = any_ref.downcast_ref::<tsdb_storage_arrow::ArrowStorageEngine>() {
            let result = self.demote_to_cold_arrow(cf_names);
            if let Ok(ref demoted) = result {
                if !demoted.is_empty() {
                    if let Err(e) = arrow_engine.refresh_partitions() {
                        tracing::warn!("failed to refresh partitions after demote to cold: {}", e);
                    }
                }
            }
            result
        } else {
            Err("Unknown engine type".to_string())
        }
    }

    fn demote_to_cold_rocksdb(&self, rocksdb_engine: &tsdb_rocksdb::TsdbRocksDb, cf_names: &[String]) -> Result<Vec<String>, String> {
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
                if let Err(e) = std::fs::write(&marker, "demoting_to_cold") {
                    tracing::warn!("failed to write cold demote marker for {}: {}", cf_name, e);
                }

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
                        if src.extension().and_then(|e| e.to_str()) != Some("parquet") {
                            if src.file_name().and_then(|n| n.to_str()) == Some(".demote_marker") {
                                let _ = std::fs::remove_file(&src);
                            }
                            continue;
                        }
                        let dst = cold_partition.join(src.file_name().unwrap_or_default());
                        if Self::safe_move_file(&src, &dst).is_ok() {
                            moved += 1;
                        }
                    }
                }

                if let Ok(entries) = std::fs::read_dir(&warm_partition) {
                    let remaining: Vec<_> = entries
                        .filter_map(|e| e.ok())
                        .filter(|e| {
                            e.path().extension().and_then(|e| e.to_str()) == Some("parquet")
                                || e.file_name() == ".demote_marker"
                        })
                        .collect();
                    if remaining.is_empty() {
                        let _ = std::fs::remove_dir(&warm_partition);
                    } else {
                        tracing::warn!(
                            "warm partition {} still has {} files after demote, not removing",
                            warm_partition.display(),
                            remaining.len()
                        );
                    }
                }

                tracing::info!("moved {} from warm to cold ({} files)", cf_name, moved);
                demoted.push(format!("{} ({} files warm → cold)", cf_name, moved));
                continue;
            }

            tracing::warn!("{} not found in RocksDB or warm Parquet, skip", cf_name);
        }

        Ok(demoted)
    }

    fn demote_to_cold_arrow(&self, cf_names: &[String]) -> Result<Vec<String>, String> {
        let warm_dir = self.data_dir.join("warm");
        let cold_dir = self.data_dir.join("cold");
        std::fs::create_dir_all(&cold_dir).map_err(|e| format!("create cold dir: {}", e))?;

        let mut demoted = Vec::new();
        for cf_name in cf_names {
            let parts: Vec<&str> = cf_name.rsplitn(2, '_').collect();
            if parts.len() < 2 {
                tracing::warn!("invalid cf_name format '{}', expected measurement_date", cf_name);
                continue;
            }
            let date_str = parts[0];
            let measurement = parts[1];

            let cold_partition = cold_dir.join(format!("data_{}", date_str));
            let cold_measurement = cold_partition.join(measurement);
            let cold_marker = cold_measurement.join(".demote_cold_marker");

            if cold_marker.exists() {
                let warm_src = warm_dir.join(format!("data_{}", date_str)).join(measurement);
                if !warm_src.is_dir() {
                    tracing::info!("found cold demote marker for {}, warm source already removed, cleaning up marker", cf_name);
                    let _ = std::fs::remove_file(&cold_marker);
                    if cold_measurement.is_dir()
                        && std::fs::read_dir(&cold_measurement).map(|mut d| d.next().is_none()).unwrap_or(true)
                    {
                        let _ = std::fs::remove_dir(&cold_measurement);
                    }
                    demoted.push(format!("{} (recovered: completed cold demote after crash)", cf_name));
                    continue;
                }

                tracing::info!("found cold demote marker for {}, resuming warm→cold move", cf_name);
                std::fs::create_dir_all(&cold_measurement)
                    .map_err(|e| format!("create cold measurement dir: {}", e))?;
                let mut moved = 0u64;
                if let Ok(entries) = std::fs::read_dir(&warm_src) {
                    for entry in entries.flatten() {
                        let src = entry.path();
                        if src.extension().and_then(|e| e.to_str()) != Some("parquet") {
                            continue;
                        }
                        let dst = cold_measurement.join(src.file_name().unwrap_or_default());
                        if Self::safe_move_file(&src, &dst).is_ok() {
                            moved += 1;
                        }
                    }
                }
                let _ = std::fs::remove_file(&cold_marker);
                if std::fs::read_dir(&warm_src).map(|mut d| d.next().is_none()).unwrap_or(true) {
                    let _ = std::fs::remove_dir(&warm_src);
                }
                tracing::info!("resumed cold demote for {} ({} files)", cf_name, moved);
                demoted.push(format!("{} ({} files warm → cold, resumed)", cf_name, moved));
                continue;
            }

            let hot_dir = self.data_dir.join(format!("data_{}", date_str)).join(measurement);
            if hot_dir.is_dir() {
                let warm_partition = warm_dir.join(format!("data_{}", date_str));
                std::fs::create_dir_all(&warm_partition)
                    .map_err(|e| format!("create warm partition dir: {}", e))?;
                let warm_measurement = warm_partition.join(measurement);
                std::fs::create_dir_all(&warm_measurement)
                    .map_err(|e| format!("create warm measurement dir: {}", e))?;

                let mut hot_moved = 0u64;
                if let Ok(entries) = std::fs::read_dir(&hot_dir) {
                    for entry in entries.flatten() {
                        let src = entry.path();
                        if src.extension().and_then(|e| e.to_str()) != Some("parquet") {
                            continue;
                        }
                        let dst = warm_measurement.join(src.file_name().unwrap_or_default());
                        if Self::safe_move_file(&src, &dst).is_ok() {
                            hot_moved += 1;
                        }
                    }
                }
                if hot_moved > 0 {
                    tracing::info!("moved {} from hot to warm ({} files)", cf_name, hot_moved);
                }

                if std::fs::read_dir(&hot_dir).map(|mut d| d.next().is_none()).unwrap_or(true) {
                    let _ = std::fs::remove_dir(&hot_dir);
                }
            }

            let src_dir = warm_dir.join(format!("data_{}", date_str)).join(measurement);
            if !src_dir.is_dir() {
                tracing::warn!("{} not found in hot or warm, skip", cf_name);
                continue;
            }

            std::fs::create_dir_all(&cold_measurement)
                .map_err(|e| format!("create cold measurement dir: {}", e))?;
            if let Err(e) = std::fs::write(&cold_marker, "demoting_to_cold") {
                tracing::warn!("failed to write cold demote marker: {}", e);
            }

            let mut moved = 0u64;
            if let Ok(entries) = std::fs::read_dir(&src_dir) {
                for entry in entries.flatten() {
                    let src = entry.path();
                    if src.extension().and_then(|e| e.to_str()) != Some("parquet") {
                        continue;
                    }
                    let dst = cold_measurement.join(src.file_name().unwrap_or_default());
                    if Self::safe_move_file(&src, &dst).is_ok() {
                        moved += 1;
                    }
                }
            }

            let _ = std::fs::remove_file(&cold_marker);
            if std::fs::read_dir(&src_dir).map(|mut d| d.next().is_none()).unwrap_or(true) {
                let _ = std::fs::remove_dir(&src_dir);
            }

            let hot_partition = self.data_dir.join(format!("data_{}", date_str));
            if hot_partition.is_dir()
                && std::fs::read_dir(&hot_partition).map(|mut d| d.next().is_none()).unwrap_or(true)
            {
                let _ = std::fs::remove_dir(&hot_partition);
            }

            tracing::info!("demoted {} from warm to cold ({} files)", cf_name, moved);
            demoted.push(format!("{} ({} files warm → cold)", cf_name, moved));
        }

        Ok(demoted)
    }

    pub fn archive(&self, older_than_days: u64) -> Result<Vec<String>, String> {
        let any_ref = self.engine.as_any();

        if let Some(rocksdb_engine) = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>() {
            self.archive_rocksdb(rocksdb_engine, older_than_days)
        } else if let Some(arrow_engine) = any_ref.downcast_ref::<tsdb_storage_arrow::ArrowStorageEngine>() {
            let result = self.archive_arrow(older_than_days);
            if let Ok(ref archived) = result {
                if !archived.is_empty() {
                    if let Err(e) = arrow_engine.refresh_partitions() {
                        tracing::warn!("failed to refresh partitions after archive: {}", e);
                    }
                }
            }
            result
        } else {
            Err("Unknown engine type".to_string())
        }
    }

    fn archive_rocksdb(&self, rocksdb_engine: &tsdb_rocksdb::TsdbRocksDb, older_than_days: u64) -> Result<Vec<String>, String> {
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

    fn archive_arrow(&self, older_than_days: u64) -> Result<Vec<String>, String> {
        let archive_dir = self.data_dir.join("archive");
        std::fs::create_dir_all(&archive_dir).map_err(|e| format!("create archive dir: {}", e))?;

        let today = chrono::Local::now().date_naive();
        let mut archived = Vec::new();

        if let Ok(entries) = std::fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                let partition_dir = entry.path();
                if !partition_dir.is_dir() {
                    continue;
                }
                let dir_name = partition_dir.file_name().unwrap_or_default().to_string_lossy().to_string();
                if !dir_name.starts_with("data_") {
                    continue;
                }
                let date_str = dir_name.trim_start_matches("data_").to_string();
                let age_days = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d")
                    .map(|d| (today - d).num_days().max(0) as u64)
                    .unwrap_or(0);

                if age_days <= older_than_days {
                    continue;
                }

                let archive_partition = archive_dir.join(&dir_name);
                std::fs::create_dir_all(&archive_partition)
                    .map_err(|e| format!("create archive partition dir: {}", e))?;

                let mut moved = 0u64;
                if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                    for sub_entry in sub_entries.flatten() {
                        let src = sub_entry.path();
                        let dst = archive_partition.join(src.file_name().unwrap_or_default());
                        if src.is_dir() {
                            let src_dir = src;
                            let dst_dir = dst;
                            std::fs::create_dir_all(&dst_dir)
                                .map_err(|e| format!("create archive measurement dir: {}", e))?;
                            if let Ok(files) = std::fs::read_dir(&src_dir) {
                                for file_entry in files.flatten() {
                                    let fsrc = file_entry.path();
                                    let fdst = dst_dir.join(fsrc.file_name().unwrap_or_default());
                                    if Self::safe_move_file(&fsrc, &fdst).is_ok() {
                                        moved += 1;
                                    }
                                }
                            }
                            let _ = std::fs::remove_dir(&src_dir);
                        } else if src.extension().and_then(|e| e.to_str()) == Some("parquet") && Self::safe_move_file(&src, &dst).is_ok() {
                            moved += 1;
                        }
                    }
                }

                if moved > 0 {
                    let remaining = Self::count_parquet_recursive(&partition_dir);
                    if remaining == 0 {
                        let _ = std::fs::remove_dir_all(&partition_dir);
                    } else {
                        tracing::warn!(
                            "archive {} has {} remaining files after move, not deleting source",
                            dir_name, remaining
                        );
                    }
                    tracing::info!("archived {} ({} files)", dir_name, moved);
                    archived.push(format!("{} ({} files → archive)", dir_name, moved));
                } else {
                    tracing::warn!("archive {} failed: 0 files moved, keeping source", dir_name);
                }
            }
        }

        let warm_dir = self.data_dir.join("warm");
        if let Ok(entries) = std::fs::read_dir(&warm_dir) {
            for entry in entries.flatten() {
                let partition_dir = entry.path();
                if !partition_dir.is_dir() {
                    continue;
                }
                let dir_name = partition_dir.file_name().unwrap_or_default().to_string_lossy().to_string();
                if !dir_name.starts_with("data_") {
                    continue;
                }
                let date_str = dir_name.trim_start_matches("data_").to_string();
                let age_days = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d")
                    .map(|d| (today - d).num_days().max(0) as u64)
                    .unwrap_or(0);

                if age_days <= older_than_days {
                    continue;
                }

                let archive_partition = archive_dir.join(&dir_name);
                if !archive_partition.is_dir() {
                    std::fs::create_dir_all(&archive_partition)
                        .map_err(|e| format!("create archive partition dir: {}", e))?;
                }

                let mut moved = 0u64;
                if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                    for sub_entry in sub_entries.flatten() {
                        let src = sub_entry.path();
                        let dst = archive_partition.join(src.file_name().unwrap_or_default());
                        if src.is_dir() {
                            let src_dir = src;
                            let dst_dir = dst;
                            std::fs::create_dir_all(&dst_dir)
                                .map_err(|e| format!("create archive measurement dir: {}", e))?;
                            if let Ok(files) = std::fs::read_dir(&src_dir) {
                                for file_entry in files.flatten() {
                                    let fsrc = file_entry.path();
                                    let fdst = dst_dir.join(fsrc.file_name().unwrap_or_default());
                                    if Self::safe_move_file(&fsrc, &fdst).is_ok() {
                                        moved += 1;
                                    }
                                }
                            }
                            let _ = std::fs::remove_dir(&src_dir);
                        }
                    }
                }

                if moved > 0 {
                    let remaining = Self::count_parquet_recursive(&partition_dir);
                    if remaining == 0 {
                        let _ = std::fs::remove_dir_all(&partition_dir);
                    } else {
                        tracing::warn!(
                            "archive warm {} has {} remaining files after move, not deleting source",
                            dir_name, remaining
                        );
                    }
                    tracing::info!("archived warm {} ({} files)", dir_name, moved);
                    archived.push(format!("{} ({} files warm → archive)", dir_name, moved));
                } else {
                    tracing::warn!("archive warm {} failed: 0 files moved, keeping source", dir_name);
                }
            }
        }

        let cold_dir = self.data_dir.join("cold");
        if let Ok(entries) = std::fs::read_dir(&cold_dir) {
            for entry in entries.flatten() {
                let partition_dir = entry.path();
                if !partition_dir.is_dir() {
                    continue;
                }
                let dir_name = partition_dir.file_name().unwrap_or_default().to_string_lossy().to_string();
                if !dir_name.starts_with("data_") {
                    continue;
                }
                let date_str = dir_name.trim_start_matches("data_").to_string();
                let age_days = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d")
                    .map(|d| (today - d).num_days().max(0) as u64)
                    .unwrap_or(0);

                if age_days <= older_than_days {
                    continue;
                }

                let archive_partition = archive_dir.join(&dir_name);
                if !archive_partition.is_dir() {
                    std::fs::create_dir_all(&archive_partition)
                        .map_err(|e| format!("create archive partition dir: {}", e))?;
                }

                let mut moved = 0u64;
                if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                    for sub_entry in sub_entries.flatten() {
                        let src = sub_entry.path();
                        let dst = archive_partition.join(src.file_name().unwrap_or_default());
                        if src.is_dir() {
                            let src_dir = src;
                            let dst_dir = dst;
                            std::fs::create_dir_all(&dst_dir)
                                .map_err(|e| format!("create archive measurement dir: {}", e))?;
                            if let Ok(files) = std::fs::read_dir(&src_dir) {
                                for file_entry in files.flatten() {
                                    let fsrc = file_entry.path();
                                    let fdst = dst_dir.join(fsrc.file_name().unwrap_or_default());
                                    if Self::safe_move_file(&fsrc, &fdst).is_ok() {
                                        moved += 1;
                                    }
                                }
                            }
                            let _ = std::fs::remove_dir(&src_dir);
                        }
                    }
                }

                if moved > 0 {
                    let remaining = Self::count_parquet_recursive(&partition_dir);
                    if remaining == 0 {
                        let _ = std::fs::remove_dir_all(&partition_dir);
                    } else {
                        tracing::warn!(
                            "archive cold {} has {} remaining files after move, not deleting source",
                            dir_name, remaining
                        );
                    }
                    tracing::info!("archived cold {} ({} files)", dir_name, moved);
                    archived.push(format!("{} ({} files cold → archive)", dir_name, moved));
                } else {
                    tracing::warn!("archive cold {} failed: 0 files moved, keeping source", dir_name);
                }
            }
        }

        Ok(archived)
    }

    fn safe_move_file(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
        if dst.exists() {
            let stem = dst
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("part");
            let ext = dst
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("parquet");
            let mut counter = 1u32;
            let parent = dst.parent().unwrap_or(dst);
            loop {
                let new_name = format!("{}_{}.{}", stem, counter, ext);
                let new_dst = parent.join(&new_name);
                if !new_dst.exists() {
                    return Self::safe_move_file(src, &new_dst);
                }
                counter += 1;
                if counter > 1000 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!("too many file name conflicts for {:?}", dst),
                    ));
                }
            }
        }
        if std::fs::rename(src, dst).is_ok() {
            Ok(())
        } else {
            std::fs::copy(src, dst).and_then(|_| std::fs::remove_file(src))
        }
    }

    fn read_parquet_row_count(path: &std::path::Path) -> Result<u64, String> {
        let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| e.to_string())?;
        let num_rows = builder.metadata().file_metadata().num_rows() as u64;
        Ok(num_rows)
    }

    fn read_parquet_measurement(path: &std::path::Path) -> Option<String> {
        let file = std::fs::File::open(path).ok()?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
        let arrow_schema = builder.schema();
        arrow_schema.metadata().get("measurement").cloned()
    }

    fn count_parquet_recursive(dir: &std::path::Path) -> usize {
        let mut count = 0;
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    count += Self::count_parquet_recursive(&path);
                } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    count += 1;
                }
            }
        }
        count
    }

    fn scan_archive_dir_recursive(
        dir: &std::path::Path,
        archive_files: &mut Vec<ArchiveFileInfo>,
        total_archive: &mut u64,
    ) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    Self::scan_archive_dir_recursive(&path, archive_files, total_archive);
                } else {
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
                            *total_archive += meta.len();
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
        }
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
    use tempfile::TempDir;

    fn create_fake_parquet(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, b"fake parquet").unwrap();
        path
    }

    fn setup_arrow_dirs(tmp: &TempDir) -> (PathBuf, PathBuf) {
        let data_dir = tmp.path().join("data");
        let parquet_dir = tmp.path().join("parquet");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&parquet_dir).unwrap();
        (data_dir.clone(), parquet_dir)
    }

    fn create_arrow_active_partition(data_dir: &std::path::Path, date: &str, measurement: &str, file_count: usize) {
        let partition_dir = data_dir.join(format!("data_{}", date));
        let measurement_dir = partition_dir.join(measurement);
        std::fs::create_dir_all(&measurement_dir).unwrap();
        for i in 0..file_count {
            create_fake_parquet(&measurement_dir, &format!("part_{}.parquet", i));
        }
    }

    fn create_warm_partition(base_dir: &std::path::Path, date: &str, measurement: &str, file_count: usize) {
        let warm_dir = base_dir.join("warm").join(format!("data_{}", date)).join(measurement);
        std::fs::create_dir_all(&warm_dir).unwrap();
        for i in 0..file_count {
            create_fake_parquet(&warm_dir, &format!("warm_{}.parquet", i));
        }
    }

    fn create_cold_partition(base_dir: &std::path::Path, date: &str, measurement: &str, file_count: usize) {
        let cold_dir = base_dir.join("cold").join(format!("data_{}", date)).join(measurement);
        std::fs::create_dir_all(&cold_dir).unwrap();
        for i in 0..file_count {
            create_fake_parquet(&cold_dir, &format!("cold_{}.parquet", i));
        }
    }

    fn count_parquet_in_dir(dir: &std::path::Path) -> usize {
        if !dir.is_dir() {
            return 0;
        }
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .filter(|e| {
                        e.path()
                            .extension()
                            .map(|ext| ext == "parquet")
                            .unwrap_or(false)
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    fn dir_exists(path: &std::path::Path) -> bool {
        path.is_dir()
    }

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

    #[test]
    fn test_demote_to_warm_arrow_moves_files() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        create_arrow_active_partition(&data_dir, "20260401", "cpu", 3);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir.clone());
        let result = api.demote_to_warm_arrow(&["cpu_20260401".to_string()]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result[0].contains("3 files hot → warm"));

        let warm_cpu_dir = data_dir.join("warm").join("data_20260401").join("cpu");
        assert!(dir_exists(&warm_cpu_dir));
        assert_eq!(count_parquet_in_dir(&warm_cpu_dir), 3);

        let hot_cpu_dir = data_dir.join("data_20260401").join("cpu");
        assert!(!dir_exists(&hot_cpu_dir));
    }

    #[test]
    fn test_demote_to_warm_arrow_invalid_cf_name() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir);
        let result = api.demote_to_warm_arrow(&["invalid".to_string()]).unwrap();
        assert!(result.is_empty(), "invalid cf_name should be skipped");
    }

    #[test]
    fn test_demote_to_warm_arrow_nonexistent_partition() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir);
        let result = api.demote_to_warm_arrow(&["cpu_20260401".to_string()]).unwrap();
        assert!(result.is_empty(), "nonexistent partition should be skipped");
    }

    #[test]
    fn test_demote_to_warm_arrow_multiple_measurements() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        create_arrow_active_partition(&data_dir, "20260401", "cpu", 2);
        create_arrow_active_partition(&data_dir, "20260401", "mem", 3);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir.clone());
        let result = api.demote_to_warm_arrow(&[
            "cpu_20260401".to_string(),
            "mem_20260401".to_string(),
        ]).unwrap();

        assert_eq!(result.len(), 2);

        let warm_cpu = data_dir.join("warm/data_20260401/cpu");
        let warm_mem = data_dir.join("warm/data_20260401/mem");
        assert_eq!(count_parquet_in_dir(&warm_cpu), 2);
        assert_eq!(count_parquet_in_dir(&warm_mem), 3);
    }

    #[test]
    fn test_demote_to_cold_arrow_from_warm() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        create_warm_partition(&data_dir, "20260301", "cpu", 2);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir.clone());
        let result = api.demote_to_cold_arrow(&["cpu_20260301".to_string()]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result[0].contains("warm → cold"));

        let cold_cpu = data_dir.join("cold/data_20260301/cpu");
        assert_eq!(count_parquet_in_dir(&cold_cpu), 2);

        let warm_cpu = data_dir.join("warm/data_20260301/cpu");
        assert!(!dir_exists(&warm_cpu));
    }

    #[test]
    fn test_demote_to_cold_arrow_from_active() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        create_arrow_active_partition(&data_dir, "20260301", "cpu", 2);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir.clone());
        let result = api.demote_to_cold_arrow(&["cpu_20260301".to_string()]).unwrap();

        assert_eq!(result.len(), 1);

        let cold_cpu = data_dir.join("cold/data_20260301/cpu");
        assert_eq!(count_parquet_in_dir(&cold_cpu), 2);

        let warm_cpu = data_dir.join("warm/data_20260301/cpu");
        assert!(!dir_exists(&warm_cpu));
    }

    #[test]
    fn test_demote_to_cold_arrow_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir);
        let result = api.demote_to_cold_arrow(&["cpu_20260301".to_string()]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_archive_arrow_moves_old_partitions() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        let old_date = (chrono::Local::now().date_naive() - chrono::Duration::days(60))
            .format("%Y%m%d")
            .to_string();
        create_arrow_active_partition(&data_dir, &old_date, "cpu", 2);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir.clone());
        let result = api.archive_arrow(30).unwrap();

        assert_eq!(result.len(), 1);

        let archive_dir = data_dir.join("archive").join(format!("data_{}", old_date));
        assert!(dir_exists(&archive_dir));

        let hot_partition = data_dir.join(format!("data_{}", old_date));
        assert!(!dir_exists(&hot_partition));
    }

    #[test]
    fn test_archive_arrow_skips_recent_partitions() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        let recent_date = chrono::Local::now().date_naive().format("%Y%m%d").to_string();
        create_arrow_active_partition(&data_dir, &recent_date, "cpu", 2);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir.clone());
        let result = api.archive_arrow(30).unwrap();

        assert!(result.is_empty(), "recent partitions should not be archived");
    }

    #[test]
    fn test_archive_arrow_cold_data() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        let old_date = (chrono::Local::now().date_naive() - chrono::Duration::days(60))
            .format("%Y%m%d")
            .to_string();
        create_cold_partition(&data_dir, &old_date, "cpu", 2);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir.clone());
        let result = api.archive_arrow(30).unwrap();

        assert_eq!(result.len(), 1);

        let cold_dir = data_dir.join("cold").join(format!("data_{}", old_date));
        assert!(!dir_exists(&cold_dir));
    }

    #[test]
    fn test_demote_marker_cleanup_on_success() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        create_arrow_active_partition(&data_dir, "20260401", "cpu", 1);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir.clone());
        let _ = api.demote_to_warm_arrow(&["cpu_20260401".to_string()]).unwrap();

        let marker = data_dir.join("warm/data_20260401/cpu/.demote_marker");
        assert!(!marker.exists(), "demote marker should be cleaned up after successful demote");
    }

    #[test]
    fn test_empty_source_dir_cleaned_up_after_demote() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        create_arrow_active_partition(&data_dir, "20260401", "cpu", 1);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir.clone());
        let _ = api.demote_to_warm_arrow(&["cpu_20260401".to_string()]).unwrap();

        let hot_measurement_dir = data_dir.join("data_20260401/cpu");
        assert!(!dir_exists(&hot_measurement_dir), "empty measurement dir should be cleaned up");
    }

    #[test]
    fn test_status_arrow_reports_all_tiers() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        create_arrow_active_partition(&data_dir, "20260401", "cpu", 2);
        create_warm_partition(&data_dir, "20260315", "mem", 1);
        create_cold_partition(&data_dir, "20260301", "disk", 1);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir);
        let status = api.status_arrow_from_fs();

        assert_eq!(status.engine_type, "arrow");
        assert!(!status.hot_cfs.is_empty(), "should have active partitions");
        assert!(!status.warm_cfs.is_empty(), "should have warm partitions");
        assert!(!status.cold_cfs.is_empty(), "should have cold partitions");
        assert!(status.total_hot_bytes > 0);
        assert!(status.total_warm_bytes > 0);
        assert!(status.total_cold_bytes > 0);
    }

    #[test]
    fn test_status_arrow_includes_archive_tier() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        let archive_dir = data_dir.join("archive").join("data_20260201").join("cpu");
        std::fs::create_dir_all(&archive_dir).unwrap();
        create_fake_parquet(&archive_dir, "archived_0.parquet");

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir);
        let status = api.status_arrow_from_fs();

        assert_eq!(status.engine_type, "arrow");
        assert!(status.total_archive_bytes > 0, "archive bytes should be counted");
        assert!(!status.archive_files.is_empty(), "archive files should be listed");
        assert!(status.parquet_partitions.iter().any(|p| p.tier == "archive"), "parquet_partitions should include archive");
    }

    #[test]
    fn test_demote_to_warm_arrow_crash_recovery() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        let warm_measurement = data_dir.join("warm/data_20260401/cpu");
        std::fs::create_dir_all(&warm_measurement).unwrap();
        create_fake_parquet(&warm_measurement, "part_0.parquet");
        std::fs::write(warm_measurement.join(".demote_marker"), "demoting_to_warm").unwrap();

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir);
        let result = api.demote_to_warm_arrow(&["cpu_20260401".to_string()]).unwrap();

        assert!(!result.is_empty(), "should report crash recovery");
        assert!(result[0].contains("recovered"), "should indicate crash recovery");
        assert!(!warm_measurement.join(".demote_marker").exists(), "marker should be cleaned up");
    }

    #[test]
    fn test_demote_to_cold_arrow_crash_recovery() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        let warm_measurement = data_dir.join("warm/data_20260401/cpu");
        std::fs::create_dir_all(&warm_measurement).unwrap();
        create_fake_parquet(&warm_measurement, "part_0.parquet");

        let cold_measurement = data_dir.join("cold/data_20260401/cpu");
        std::fs::create_dir_all(&cold_measurement).unwrap();
        std::fs::write(cold_measurement.join(".demote_cold_marker"), "demoting_to_cold").unwrap();

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir);
        let result = api.demote_to_cold_arrow(&["cpu_20260401".to_string()]).unwrap();

        assert!(!result.is_empty(), "should report crash recovery");
        assert!(result[0].contains("resumed") || result[0].contains("recovered"), "should indicate crash recovery");
        assert!(!cold_measurement.join(".demote_cold_marker").exists(), "marker should be cleaned up");
    }

    #[test]
    fn test_safe_move_file_no_conflict() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("src.parquet");
        let dst = tmp.path().join("dst.parquet");
        std::fs::write(&src, b"test data").unwrap();

        LifecycleApi::safe_move_file(&src, &dst).unwrap();

        assert!(!src.exists(), "source should be removed");
        assert!(dst.exists(), "destination should exist");
        assert_eq!(std::fs::read(&dst).unwrap(), b"test data");
    }

    #[test]
    fn test_safe_move_file_conflict_resolution() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("part.parquet");
        let existing = tmp.path().join("part.parquet");
        std::fs::write(&existing, b"existing").unwrap();
        std::fs::write(&src, b"new data").unwrap();

        LifecycleApi::safe_move_file(&src, &existing).unwrap();

        assert!(tmp.path().join("part_1.parquet").exists(), "should create part_1.parquet on conflict");
        assert_eq!(std::fs::read(tmp.path().join("part_1.parquet")).unwrap(), b"new data");
    }

    #[test]
    fn test_archive_arrow_warm_to_archive() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        let old_date = (chrono::Utc::now().date_naive() - chrono::Duration::days(90))
            .format("%Y%m%d")
            .to_string();
        create_warm_partition(&data_dir, &old_date, "cpu", 2);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir.clone());
        let result = api.archive_arrow(30).unwrap();

        assert!(!result.is_empty(), "should archive warm data");
        assert!(result[0].contains("warm"), "should indicate warm → archive");

        let warm_measurement = data_dir.join("warm").join(format!("data_{}", old_date)).join("cpu");
        assert!(!warm_measurement.exists(), "warm source should be removed");

        let archive_measurement = data_dir.join("archive").join(format!("data_{}", old_date)).join("cpu");
        assert!(archive_measurement.is_dir(), "archive destination should exist");
        assert_eq!(count_parquet_in_dir(&archive_measurement), 2);
    }

    #[test]
    fn test_archive_arrow_no_data_younger_than_threshold() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        let recent_date = (chrono::Utc::now().date_naive() - chrono::Duration::days(5))
            .format("%Y%m%d")
            .to_string();
        create_cold_partition(&data_dir, &recent_date, "cpu", 1);

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir);
        let result = api.archive_arrow(30).unwrap();

        assert!(result.is_empty(), "should not archive recent data");
    }

    #[test]
    fn test_count_parquet_recursive() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path().join("data_20260401");
        let m1 = base.join("cpu");
        let m2 = base.join("mem");
        std::fs::create_dir_all(&m1).unwrap();
        std::fs::create_dir_all(&m2).unwrap();
        create_fake_parquet(&m1, "p1.parquet");
        create_fake_parquet(&m1, "p2.parquet");
        create_fake_parquet(&m2, "p3.parquet");

        let count = LifecycleApi::count_parquet_recursive(&base);
        assert_eq!(count, 3, "should count parquet files in subdirectories");
    }

    #[test]
    fn test_demote_to_warm_arrow_skips_non_parquet() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, parquet_dir) = setup_arrow_dirs(&tmp);

        let partition_dir = data_dir.join("data_20260401").join("cpu");
        std::fs::create_dir_all(&partition_dir).unwrap();
        create_fake_parquet(&partition_dir, "part_0.parquet");
        std::fs::write(partition_dir.join("readme.txt"), b"not parquet").unwrap();
        std::fs::write(partition_dir.join(".demote_marker"), "stale").unwrap();

        let api = LifecycleApi::new_for_test(parquet_dir, data_dir.clone());
        let result = api.demote_to_warm_arrow(&["cpu_20260401".to_string()]).unwrap();

        assert!(!result.is_empty());
        let warm_measurement = data_dir.join("warm/data_20260401/cpu");
        assert_eq!(count_parquet_in_dir(&warm_measurement), 1, "only parquet files should be moved");
        assert!(!warm_measurement.join("readme.txt").exists(), "non-parquet files should not be moved");
    }
}
