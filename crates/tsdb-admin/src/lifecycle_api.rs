use crate::protocol::*;
use std::path::Path;
use std::sync::Arc;
use tsdb_arrow::engine::StorageEngine;

pub struct LifecycleApi {
    engine: Arc<dyn StorageEngine>,
}

impl LifecycleApi {
    pub fn new(engine: Arc<dyn StorageEngine>) -> Self {
        Self { engine }
    }

    pub fn status(&self) -> LifecycleStatus {
        let any_ref = self.engine.as_any();
        let Some(rocksdb_engine) = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>() else {
            return LifecycleStatus {
                hot_cfs: vec![], warm_cfs: vec![], cold_cfs: vec![],
                archive_files: vec![], parquet_partitions: vec![],
                total_hot_bytes: 0, total_warm_bytes: 0, total_cold_bytes: 0, total_archive_bytes: 0,
            };
        };

        let db = rocksdb_engine.db();
        let base_dir = rocksdb_engine.base_dir();
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

            let (age_days, tier) = match &date_str {
                Some(ds) => {
                    if let Ok(cf_date) = chrono::NaiveDate::parse_from_str(ds, "%Y%m%d") {
                        let age = (today - cf_date).num_days().max(0) as u64;
                        let tier = if age <= 3 { "hot".to_string() } else if age <= 14 { "warm".to_string() } else { "cold".to_string() };
                        (age, tier)
                    } else {
                        (0, "hot".to_string())
                    }
                }
                None => (0, "hot".to_string()),
            };

            let sst_size = db.cf_handle(cf_name)
                .and_then(|cf| db.property_int_value_cf(&cf, rocksdb::properties::TOTAL_SST_FILES_SIZE).ok().flatten())
                .unwrap_or(0);
            let num_keys = db.cf_handle(cf_name)
                .and_then(|cf| db.property_int_value_cf(&cf, rocksdb::properties::ESTIMATE_NUM_KEYS).ok().flatten())
                .unwrap_or(0);

            let info = DataTierInfo {
                cf_name: cf_name.clone(),
                measurement,
                date: date_str.unwrap_or_default(),
                age_days,
                sst_size,
                num_keys,
                tier: tier.clone(),
            };

            match tier.as_str() {
                "hot" => { total_hot += sst_size; hot_cfs.push(info); }
                "warm" => { total_warm += sst_size; warm_cfs.push(info); }
                "cold" => { total_cold += sst_size; cold_cfs.push(info); }
                _ => {}
            }
        }

        let archive_dir = base_dir.join("archive");
        let mut archive_files = Vec::new();
        let mut total_archive = 0u64;

        if let Ok(entries) = std::fs::read_dir(&archive_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
                if ext == "parquet" || ext == "jsonl" {
                    if let Ok(meta) = entry.metadata() {
                        let modified = meta.modified().ok()
                            .map(|t| {
                                let dt: chrono::DateTime<chrono::Utc> = t.into();
                                dt.format("%Y-%m-%d %H:%M:%S").to_string()
                            })
                            .unwrap_or_default();
                        total_archive += meta.len();
                        archive_files.push(ArchiveFileInfo {
                            name: path.file_name().unwrap_or_default().to_string_lossy().to_string(),
                            size: meta.len(),
                            modified,
                        });
                    }
                }
            }
        }

        let parquet_data_dir = base_dir.join("parquet_data");
        let mut parquet_partitions = Vec::new();

        if let Ok(entries) = std::fs::read_dir(&parquet_data_dir) {
            for entry in entries.flatten() {
                let partition_dir = entry.path();
                if !partition_dir.is_dir() { continue; }
                let dir_name = partition_dir.file_name().unwrap_or_default().to_string_lossy().to_string();
                let date_str = dir_name.strip_prefix("data_").unwrap_or(&dir_name).to_string();

                let age_days = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d")
                    .map(|d| (today - d).num_days().max(0) as u64)
                    .unwrap_or(0);
                let tier = if age_days <= 3 { "hot" } else if age_days <= 14 { "warm" } else { "cold" };

                let mut files = Vec::new();
                let mut total_size = 0u64;
                if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                    for sub_entry in sub_entries.flatten() {
                        let p = sub_entry.path();
                        if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                            if let Ok(meta) = p.metadata() {
                                total_size += meta.len();
                            }
                            files.push(p.file_name().unwrap_or_default().to_string_lossy().to_string());
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

        LifecycleStatus {
            hot_cfs, warm_cfs, cold_cfs,
            archive_files, parquet_partitions,
            total_hot_bytes: total_hot,
            total_warm_bytes: total_warm,
            total_cold_bytes: total_cold,
            total_archive_bytes: total_archive,
        }
    }

    pub fn archive(&self, older_than_days: u64) -> Result<Vec<String>, String> {
        let any_ref = self.engine.as_any();
        let rocksdb_engine = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>()
            .ok_or("Not using RocksDB engine")?;
        let base_dir = rocksdb_engine.base_dir();
        let archive_dir = base_dir.join("archive");
        std::fs::create_dir_all(&archive_dir).map_err(|e| format!("create archive dir: {}", e))?;

        let today = chrono::Local::now().date_naive();
        let ts_cfs = rocksdb_engine.list_ts_cfs();
        let mut archived = Vec::new();

        for cf_name in &ts_cfs {
            if let Some(date_str) = Self::extract_date_from_cf(cf_name) {
                if let Ok(cf_date) = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d") {
                    let age = (today - cf_date).num_days() as u64;
                    if age > older_than_days {
                        let output_path = archive_dir.join(format!("{}.parquet", cf_name));
                        match tsdb_rocksdb::DataArchiver::export_cf_to_parquet(
                            rocksdb_engine, cf_name, &archive_dir, 10000,
                        ) {
                            Ok(count) => {
                                tracing::info!("archived {} ({} points) to {}", cf_name, count, output_path.display());
                                archived.push(format!("{} ({} points)", cf_name, count));
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

    fn extract_date_from_cf(cf_name: &str) -> Option<String> {
        if !cf_name.starts_with("ts_") { return None; }
        let rest = &cf_name[3..];
        let last_underscore = rest.rfind('_')?;
        let date_part = &rest[last_underscore + 1..];
        if date_part.len() == 8 && date_part.chars().all(|c| c.is_ascii_digit()) {
            Some(date_part.to_string())
        } else {
            None
        }
    }

    fn extract_measurement_from_cf(cf_name: &str) -> String {
        if !cf_name.starts_with("ts_") { return cf_name.to_string(); }
        let rest = &cf_name[3..];
        if let Some(last_underscore) = rest.rfind('_') {
            rest[..last_underscore].to_string()
        } else {
            rest.to_string()
        }
    }
}
