use crate::protocol::*;
use std::collections::VecDeque;
use std::sync::Arc;
use tsdb_arrow::engine::StorageEngine;

struct CpuSample {
    user: u64,
    nice: u64,
    system: u64,
    idle: u64,
    iowait: u64,
}

struct SystemState {
    prev_cpu: Option<CpuSample>,
    prev_net_rx: u64,
    prev_net_tx: u64,
    prev_write_count: u64,
    prev_read_count: u64,
}

pub struct MetricsCollector {
    engine: Arc<dyn StorageEngine>,
    history: std::sync::Mutex<VecDeque<MetricsSnapshot>>,
    sys_state: std::sync::Mutex<SystemState>,
    start_time: i64,
    collector_state: std::sync::Mutex<CollectorStateInner>,
}

struct CollectorStateInner {
    running: bool,
    interval_secs: u64,
    snapshots_collected: u64,
    last_collect_ms: Option<u64>,
}

impl MetricsCollector {
    pub fn new(engine: Arc<dyn StorageEngine>) -> Self {
        Self {
            engine,
            history: std::sync::Mutex::new(VecDeque::new()),
            sys_state: std::sync::Mutex::new(SystemState {
                prev_cpu: None,
                prev_net_rx: 0,
                prev_net_tx: 0,
                prev_write_count: 0,
                prev_read_count: 0,
            }),
            start_time: chrono::Utc::now().timestamp_micros(),
            collector_state: std::sync::Mutex::new(CollectorStateInner {
                running: true,
                interval_secs: 1,
                snapshots_collected: 0,
                last_collect_ms: None,
            }),
        }
    }

    pub fn collector_status(&self) -> CollectorStatus {
        let state = self.collector_state.lock().unwrap();
        CollectorStatus {
            running: state.running,
            interval_secs: state.interval_secs,
            snapshots_collected: state.snapshots_collected,
            last_collect_ms: state.last_collect_ms,
        }
    }

    pub fn configure_collector(&self, interval_secs: u64, enabled: bool) {
        let mut state = self.collector_state.lock().unwrap();
        state.interval_secs = interval_secs.max(1);
        state.running = enabled;
    }

    pub fn start_collector(&self) {
        let mut state = self.collector_state.lock().unwrap();
        state.running = true;
    }

    pub fn stop_collector(&self) {
        let mut state = self.collector_state.lock().unwrap();
        state.running = false;
    }

    pub fn is_collector_running(&self) -> bool {
        self.collector_state.lock().unwrap().running
    }

    pub fn collector_interval(&self) -> u64 {
        self.collector_state.lock().unwrap().interval_secs
    }

    pub fn health(&self, service_name: &str) -> HealthStatus {
        let measurements = self.engine.list_measurements();
        let (memory_bytes, disk_bytes, l0_file_count, _compaction_count, total_keys) =
            self.collect_rocksdb_metrics();

        let (cpu_pct, sys_memory_pct, sys_disk_pct, load_avg) = self.collect_system_metrics();
        let (cpu_temp, gpu_temp) = Self::read_temperatures();

        let memory_usage_pct = if memory_bytes > 0 {
            let total_mem = Self::system_total_memory();
            if total_mem > 0 {
                (memory_bytes as f64 / total_mem as f64) * 100.0
            } else {
                0.0
            }
        } else {
            0.0
        };

        let disk_usage_pct = if disk_bytes > 0 {
            let total_disk = Self::system_total_disk();
            if total_disk > 0 {
                (disk_bytes as f64 / total_disk as f64) * 100.0
            } else {
                0.0
            }
        } else {
            0.0
        };

        let healthy = l0_file_count < 20 && cpu_pct < 95.0 && sys_memory_pct < 95.0;
        let storage_ok = l0_file_count < 36;

        let now = chrono::Utc::now().timestamp_micros();
        let uptime_secs = ((now - self.start_time) / 1_000_000) as u64;

        HealthStatus {
            service: service_name.to_string(),
            healthy,
            storage_ok,
            memory_usage_pct,
            disk_usage_pct,
            l0_file_count,
            compaction_idle: l0_file_count < 8,
            measurements_count: measurements.len(),
            total_data_points: total_keys,
            cpu_pct,
            sys_memory_pct,
            sys_disk_pct,
            load_avg_1m: load_avg,
            uptime_secs,
            cpu_temp_c: cpu_temp,
            gpu_temp_c: gpu_temp,
        }
    }

    pub fn stats(&self) -> MetricsSnapshot {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let (memory_bytes, disk_bytes, l0_file_count, compaction_count, _total_keys) =
            self.collect_rocksdb_metrics();

        let (write_count, read_count) = self.collect_write_read_counts();
        let mut state = self.sys_state.lock().unwrap();
        let write_rate = (write_count - state.prev_write_count) as f64;
        let read_rate = (read_count - state.prev_read_count) as f64;
        state.prev_write_count = write_count;
        state.prev_read_count = read_count;
        drop(state);

        let (cpu_pct, sys_memory_pct, sys_disk_pct, load_avg) = self.collect_system_metrics();
        let (sys_memory_total, sys_memory_used) = Self::memory_info();
        let (sys_disk_total, sys_disk_used) = Self::disk_info();
        let (net_rx, net_tx) = self.collect_network_stats();
        let (cpu_temp, gpu_temp) = Self::read_temperatures();

        let mut cstate = self.collector_state.lock().unwrap();
        cstate.snapshots_collected += 1;
        cstate.last_collect_ms = Some(now_ms);

        MetricsSnapshot {
            timestamp_ms: now_ms,
            write_rate,
            read_rate,
            write_latency_us: 0.0,
            read_latency_us: 0.0,
            memory_bytes,
            disk_bytes,
            l0_file_count,
            compaction_count,
            cpu_pct,
            sys_memory_total_bytes: sys_memory_total,
            sys_memory_used_bytes: sys_memory_used,
            sys_memory_pct,
            sys_disk_total_bytes: sys_disk_total,
            sys_disk_used_bytes: sys_disk_used,
            sys_disk_pct,
            load_avg_1m: load_avg,
            net_rx_bytes: net_rx,
            net_tx_bytes: net_tx,
            cpu_temp_c: cpu_temp,
            gpu_temp_c: gpu_temp,
        }
    }

    pub fn record_snapshot(&self, snapshot: MetricsSnapshot) {
        let mut history = self.history.lock().unwrap();
        if history.len() >= 3600 {
            history.pop_front();
        }
        history.push_back(snapshot);
    }

    pub fn timeseries(&self, _metric: &str, range_secs: u64) -> Vec<MetricsSnapshot> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff = now_ms - range_secs * 1000;
        let history = self.history.lock().unwrap();
        history
            .iter()
            .filter(|s| s.timestamp_ms >= cutoff)
            .cloned()
            .collect()
    }

    pub fn alerts(&self, health: &HealthStatus) -> Vec<Alert> {
        let mut alerts = Vec::new();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if !health.healthy {
            alerts.push(Alert { level: AlertLevel::Critical, message: "服务不健康".to_string(), timestamp_ms: now_ms });
        }
        if !health.storage_ok {
            alerts.push(Alert { level: AlertLevel::Critical, message: "存储异常".to_string(), timestamp_ms: now_ms });
        }
        if health.cpu_pct > 90.0 {
            alerts.push(Alert { level: AlertLevel::Warning, message: format!("CPU使用率过高: {:.1}%", health.cpu_pct), timestamp_ms: now_ms });
        }
        if health.sys_memory_pct > 90.0 {
            alerts.push(Alert { level: AlertLevel::Warning, message: format!("系统内存使用率过高: {:.1}%", health.sys_memory_pct), timestamp_ms: now_ms });
        }
        if health.sys_disk_pct > 85.0 {
            alerts.push(Alert { level: AlertLevel::Warning, message: format!("磁盘使用率过高: {:.1}%", health.sys_disk_pct), timestamp_ms: now_ms });
        }
        if health.l0_file_count > 16 {
            alerts.push(Alert { level: AlertLevel::Warning, message: format!("L0文件数过多: {}", health.l0_file_count), timestamp_ms: now_ms });
        }
        if health.load_avg_1m > 8.0 {
            alerts.push(Alert { level: AlertLevel::Warning, message: format!("系统负载过高: {:.2}", health.load_avg_1m), timestamp_ms: now_ms });
        }
        if health.cpu_temp_c > 80.0 {
            alerts.push(Alert { level: AlertLevel::Warning, message: format!("CPU温度过高: {:.1}°C", health.cpu_temp_c), timestamp_ms: now_ms });
        }
        if alerts.is_empty() {
            alerts.push(Alert { level: AlertLevel::Info, message: "所有指标正常".to_string(), timestamp_ms: now_ms });
        }
        alerts
    }

    pub fn rocksdb_overview(&self) -> RocksdbOverview {
        let any_ref = self.engine.as_any();
        if let Some(rocksdb_engine) = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>() {
            let db = rocksdb_engine.db();
            let ts_cfs = rocksdb_engine.list_ts_cfs();
            let measurements = rocksdb_engine.list_measurements();
            let total_sst_size = db.property_int_value(rocksdb::properties::TOTAL_SST_FILES_SIZE).ok().flatten().unwrap_or(0);
            let total_memtable = db.property_int_value(rocksdb::properties::CUR_SIZE_ALL_MEM_TABLES).ok().flatten().unwrap_or(0);
            let block_cache = db.property_int_value(rocksdb::properties::BLOCK_CACHE_USAGE).ok().flatten().unwrap_or(0);
            let total_keys = db.property_int_value(rocksdb::properties::ESTIMATE_NUM_KEYS).ok().flatten().unwrap_or(0);
            let l0_count = db.property_int_value(rocksdb::properties::num_files_at_level(0)).ok().flatten().unwrap_or(0) as i32;
            let compaction_pending = db.property_int_value("rocksdb.compaction.pending").ok().flatten().unwrap_or(0) > 0;
            let stats_text = db.property_value(rocksdb::properties::STATS).ok().flatten().unwrap_or_default();

            let total_cf_count = ts_cfs.len();

            RocksdbOverview {
                total_cf_count,
                ts_cf_count: ts_cfs.len(),
                measurements,
                total_sst_size_bytes: total_sst_size,
                total_memtable_bytes: total_memtable,
                total_block_cache_bytes: block_cache,
                total_keys,
                l0_file_count: l0_count,
                compaction_pending,
                stats_text,
            }
        } else {
            RocksdbOverview {
                total_cf_count: 0,
                ts_cf_count: 0,
                measurements: vec![],
                total_sst_size_bytes: 0,
                total_memtable_bytes: 0,
                total_block_cache_bytes: 0,
                total_keys: 0,
                l0_file_count: 0,
                compaction_pending: false,
                stats_text: "Not using RocksDB engine".to_string(),
            }
        }
    }

    pub fn rocksdb_cf_list(&self) -> Vec<String> {
        let any_ref = self.engine.as_any();
        if let Some(rocksdb_engine) = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>() {
            rocksdb_engine.list_ts_cfs()
        } else {
            vec![]
        }
    }

    pub fn rocksdb_cf_detail(&self, cf_name: &str) -> Option<CfDetail> {
        let any_ref = self.engine.as_any();
        let rocksdb_engine = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>()?;
        let db = rocksdb_engine.db();
        let cf = db.cf_handle(cf_name)?;

        let estimate_keys = db.property_int_value_cf(&cf, rocksdb::properties::ESTIMATE_NUM_KEYS).ok().flatten().unwrap_or(0);
        let sst_size = db.property_int_value_cf(&cf, rocksdb::properties::TOTAL_SST_FILES_SIZE).ok().flatten().unwrap_or(0);
        let memtable_size = db.property_int_value_cf(&cf, rocksdb::properties::CUR_SIZE_ALL_MEM_TABLES).ok().flatten().unwrap_or(0);
        let block_cache = db.property_int_value_cf(&cf, rocksdb::properties::BLOCK_CACHE_USAGE).ok().flatten().unwrap_or(0);
        let compaction_pending = db.property_int_value_cf(&cf, "rocksdb.compaction.pending").ok().flatten().unwrap_or(0) > 0;
        let stats_text = db.property_value_cf(&cf, rocksdb::properties::STATS).ok().flatten().unwrap_or_default();

        let mut levels = Vec::new();
        for lvl in 0..7 {
            let count = db.property_int_value_cf(&cf, rocksdb::properties::num_files_at_level(lvl))
                .ok().flatten().unwrap_or(0);
            levels.push(count as i64);
        }

        Some(CfDetail {
            name: cf_name.to_string(),
            estimate_num_keys: estimate_keys,
            total_sst_file_size: sst_size,
            num_files_at_level: levels,
            memtable_size,
            block_cache_usage: block_cache,
            compaction_pending,
            stats_text,
        })
    }

    pub fn rocksdb_compact(&self, cf_name: Option<&str>) -> Result<(), String> {
        let any_ref = self.engine.as_any();
        let rocksdb_engine = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>()
            .ok_or("Not using RocksDB engine")?;

        if let Some(cf) = cf_name {
            rocksdb_engine.compact_cf(cf).map_err(|e| e.to_string())
        } else {
            let cfs = rocksdb_engine.list_ts_cfs();
            for cf in &cfs {
                rocksdb_engine.compact_cf(cf).map_err(|e| e.to_string())?;
            }
            Ok(())
        }
    }

    pub fn rocksdb_kv_scan(
        &self,
        cf_name: &str,
        prefix: Option<&str>,
        start_key_hex: Option<&str>,
        limit: usize,
    ) -> Result<KvScanResult, String> {
        let any_ref = self.engine.as_any();
        let rocksdb_engine = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>()
            .ok_or("Not using RocksDB engine")?;
        let db = rocksdb_engine.db();
        let cf = db.cf_handle(cf_name)
            .ok_or_else(|| format!("CF '{}' not found", cf_name))?;

        let limit = limit.max(1).min(200);
        let mut entries = Vec::new();
        let mut total_scanned = 0;
        let mut has_more = false;
        let mut tags_cache: std::collections::HashMap<u64, Option<serde_json::Value>> = std::collections::HashMap::new();

        let start_bytes = if let Some(hex) = start_key_hex {
            hex_decode(hex).unwrap_or_default()
        } else if let Some(pfx) = prefix {
            hex_decode(pfx).unwrap_or_default()
        } else {
            vec![]
        };

        let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::From(
            &start_bytes,
            rocksdb::Direction::Forward,
        ));

        let prefix_bytes = prefix.and_then(|p| hex_decode(p).ok());

        for item in iter {
            if entries.len() >= limit {
                has_more = true;
                break;
            }
            match item {
                Ok((key, value)) => {
                    total_scanned += 1;
                    let key_vec = key.as_ref().to_vec();
                    let val_vec = value.as_ref().to_vec();

                    if let Some(ref pfx) = prefix_bytes {
                        if !key_vec.starts_with(pfx.as_slice()) {
                            if key_vec.as_slice() > pfx.as_slice() {
                                break;
                            }
                            continue;
                        }
                    }

                    let decoded = Self::try_decode_kv_with_meta(
                        cf_name, &key_vec, &val_vec, db, &mut tags_cache,
                    );

                    entries.push(KvEntry {
                        key_hex: hex_encode(&key_vec),
                        key_ascii: ascii_repr(&key_vec),
                        value_hex: hex_encode(&val_vec),
                        value_ascii: ascii_repr(&val_vec),
                        value_size: val_vec.len(),
                        decoded,
                    });
                }
                Err(_) => break,
            }
        }

        Ok(KvScanResult {
            cf: cf_name.to_string(),
            entries,
            total_scanned,
            has_more,
        })
    }

    pub fn rocksdb_kv_get(&self, cf_name: &str, key_hex: &str) -> Result<KvEntry, String> {
        let any_ref = self.engine.as_any();
        let rocksdb_engine = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>()
            .ok_or("Not using RocksDB engine")?;
        let db = rocksdb_engine.db();
        let cf = db.cf_handle(cf_name)
            .ok_or_else(|| format!("CF '{}' not found", cf_name))?;

        let key_bytes = hex_decode(key_hex)
            .map_err(|e| format!("invalid hex key: {}", e))?;

        let value = db.get_cf(&cf, &key_bytes)
            .map_err(|e| format!("read error: {}", e))?;

        let mut tags_cache = std::collections::HashMap::new();

        match value {
            Some(val_vec) => {
                let decoded = Self::try_decode_kv_with_meta(
                    cf_name, &key_bytes, &val_vec, db, &mut tags_cache,
                );
                Ok(KvEntry {
                    key_hex: hex_encode(&key_bytes),
                    key_ascii: ascii_repr(&key_bytes),
                    value_hex: hex_encode(&val_vec),
                    value_ascii: ascii_repr(&val_vec),
                    value_size: val_vec.len(),
                    decoded,
                })
            }
            None => Err("key not found".to_string()),
        }
    }

    fn try_decode_kv(cf_name: &str, key: &[u8], value: &[u8]) -> Option<serde_json::Value> {
        if cf_name == "_series_meta" && key.len() == 8 {
            return Self::decode_series_meta_value(value);
        }
        if cf_name.starts_with("ts_") && key.len() == 16 {
            return Self::decode_ts_kv(key, value);
        }
        None
    }

    fn try_decode_kv_with_meta(
        cf_name: &str,
        key: &[u8],
        value: &[u8],
        db: &rocksdb::DB,
        tags_cache: &mut std::collections::HashMap<u64, Option<serde_json::Value>>,
    ) -> Option<serde_json::Value> {
        if cf_name == "_series_meta" && key.len() == 8 {
            return Self::decode_series_meta_value(value);
        }
        if cf_name.starts_with("ts_") && key.len() == 16 {
            let tags_hash = u64::from_be_bytes(key[0..8].try_into().ok()?);
            let tags_value = if let Some(cached) = tags_cache.get(&tags_hash) {
                cached.clone()
            } else {
                let resolved = Self::resolve_tags(db, tags_hash);
                tags_cache.insert(tags_hash, resolved.clone());
                resolved
            };
            return Self::decode_ts_kv_with_tags(key, value, tags_value);
        }
        None
    }

    fn resolve_tags(
        db: &rocksdb::DB,
        tags_hash: u64,
    ) -> Option<serde_json::Value> {
        let cf = db.cf_handle("_series_meta")?;
        let hash_bytes = tags_hash.to_be_bytes();
        let value = db.get_cf(&cf, &hash_bytes).ok()??;
        Self::decode_series_meta_value(&value)
    }

    fn decode_ts_kv_with_tags(
        key: &[u8],
        value: &[u8],
        tags: Option<serde_json::Value>,
    ) -> Option<serde_json::Value> {
        let tags_hash = u64::from_be_bytes(key[0..8].try_into().ok()?);
        let timestamp = i64::from_be_bytes(key[8..16].try_into().ok()?);
        let ts_secs = timestamp / 1_000_000;
        let ts_us = timestamp % 1_000_000;

        let mut result = serde_json::Map::new();
        result.insert("tags_hash".to_string(), serde_json::Value::String(format!("0x{:016x}", tags_hash)));
        if let Some(tags) = tags {
            result.insert("tags".to_string(), tags);
        }
        result.insert("timestamp_us".to_string(), serde_json::json!(timestamp));
        result.insert("timestamp".to_string(), serde_json::Value::String(format!("{}.{}", ts_secs, ts_us)));

        if value.len() < 2 {
            return Some(serde_json::Value::Object(result));
        }
        let num_fields = u16::from_be_bytes([value[0], value[1]]) as usize;
        let mut offset = 2;
        let mut fields = serde_json::Map::new();
        let mut field_names = Vec::new();
        for _ in 0..num_fields {
            if offset + 2 > value.len() { break; }
            let name_len = u16::from_be_bytes([value[offset], value[offset + 1]]) as usize;
            offset += 2;
            if offset + name_len > value.len() { break; }
            let name = String::from_utf8_lossy(&value[offset..offset + name_len]).to_string();
            field_names.push(name.clone());
            offset += name_len;
            if offset + 1 > value.len() { break; }
            let type_tag = value[offset];
            offset += 1;
            match type_tag {
                0x00 => {
                    if offset + 8 <= value.len() {
                        let v = f64::from_be_bytes(value[offset..offset + 8].try_into().unwrap_or([0; 8]));
                        fields.insert(name, serde_json::json!(v));
                        offset += 8;
                    }
                }
                0x01 => {
                    if offset + 8 <= value.len() {
                        let v = i64::from_be_bytes(value[offset..offset + 8].try_into().unwrap_or([0; 8]));
                        fields.insert(name, serde_json::json!(v));
                        offset += 8;
                    }
                }
                0x02 => {
                    if offset + 2 <= value.len() {
                        let str_len = u16::from_be_bytes([value[offset], value[offset + 1]]) as usize;
                        offset += 2;
                        if offset + str_len <= value.len() {
                            let s = String::from_utf8_lossy(&value[offset..offset + str_len]).to_string();
                            fields.insert(name, serde_json::Value::String(s));
                            offset += str_len;
                        }
                    }
                }
                0x03 => {
                    if offset + 1 <= value.len() {
                        let b = value[offset] != 0;
                        fields.insert(name, serde_json::json!(b));
                        offset += 1;
                    }
                }
                _ => {
                    fields.insert(name, serde_json::Value::String(format!("<unknown type 0x{:02x}>", type_tag)));
                    break;
                }
            }
        }
        result.insert("fields".to_string(), serde_json::Value::Object(fields));
        result.insert("field_names".to_string(), serde_json::json!(field_names));
        Some(serde_json::Value::Object(result))
    }

    pub fn rocksdb_series_schema(&self) -> Result<SeriesSchema, String> {
        let any_ref = self.engine.as_any();
        let rocksdb_engine = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>()
            .ok_or("Not using RocksDB engine")?;
        let db = rocksdb_engine.db();

        let measurements = rocksdb_engine.list_measurements();
        let ts_cfs = rocksdb_engine.list_ts_cfs();
        let meta_cf = db.cf_handle("_series_meta");

        let mut result = Vec::new();

        for measurement in &measurements {
            let prefix = format!("ts_{}_", measurement);
            let matching_cfs: Vec<String> = ts_cfs.iter()
                .filter(|cf| cf.starts_with(&prefix))
                .cloned()
                .collect();

            let mut series_list = Vec::new();
            let mut all_tag_keys = std::collections::BTreeSet::new();
            let mut all_field_names = std::collections::BTreeSet::new();

            if let Some(ref meta_cf) = meta_cf {
                let iter = db.iterator_cf(meta_cf, rocksdb::IteratorMode::Start);
                for item in iter {
                    if series_list.len() >= 1000 { break; }
                    match item {
                        Ok((key, value)) => {
                            if key.len() == 8 {
                                let tags_hash = u64::from_be_bytes(key[0..8].try_into().unwrap_or([0; 8]));
                                if let Some(tags_val) = Self::decode_series_meta_value(&value) {
                                    if let Some(tags_obj) = tags_val.as_object() {
                                        for tag_key in tags_obj.keys() {
                                            all_tag_keys.insert(tag_key.clone());
                                        }
                                    }
                                    series_list.push(SeriesInfo {
                                        tags_hash: format!("0x{:016x}", tags_hash),
                                        tags: tags_val,
                                    });
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
            }

            if let Some(ref first_cf) = matching_cfs.first() {
                if let Some(cf) = db.cf_handle(first_cf) {
                    let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
                    for item in iter.take(10) {
                        match item {
                            Ok((key, value)) => {
                                if key.len() == 16 && value.len() >= 2 {
                                    let num_fields = u16::from_be_bytes([value[0], value[1]]) as usize;
                                    let mut offset = 2;
                                    for _ in 0..num_fields {
                                        if offset + 2 > value.len() { break; }
                                        let name_len = u16::from_be_bytes([value[offset], value[offset + 1]]) as usize;
                                        offset += 2;
                                        if offset + name_len > value.len() { break; }
                                        let name = String::from_utf8_lossy(&value[offset..offset + name_len]).to_string();
                                        all_field_names.insert(name);
                                        offset += name_len;
                                        if offset + 1 > value.len() { break; }
                                        let type_tag = value[offset];
                                        offset += 1;
                                        match type_tag {
                                            0x00 => offset += 8,
                                            0x01 => offset += 8,
                                            0x02 => {
                                                if offset + 2 <= value.len() {
                                                    let sl = u16::from_be_bytes([value[offset], value[offset+1]]) as usize;
                                                    offset += 2 + sl;
                                                }
                                            }
                                            0x03 => offset += 1,
                                            _ => break,
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }

            result.push(MeasurementSchema {
                name: measurement.clone(),
                column_families: matching_cfs,
                series: series_list,
                tag_keys: all_tag_keys.into_iter().collect(),
                field_names: all_field_names.into_iter().collect(),
            });
        }

        Ok(SeriesSchema { measurements: result })
    }

    fn decode_series_meta_value(value: &[u8]) -> Option<serde_json::Value> {
        if value.len() < 2 {
            return None;
        }
        let num_tags = u16::from_be_bytes([value[0], value[1]]) as usize;
        let mut offset = 2;
        let mut tags = serde_json::Map::new();
        for _ in 0..num_tags {
            if offset + 2 > value.len() {
                break;
            }
            let key_len = u16::from_be_bytes([value[offset], value[offset + 1]]) as usize;
            offset += 2;
            if offset + key_len > value.len() {
                break;
            }
            let key_str = String::from_utf8_lossy(&value[offset..offset + key_len]).to_string();
            offset += key_len;
            if offset + 2 > value.len() {
                break;
            }
            let val_len = u16::from_be_bytes([value[offset], value[offset + 1]]) as usize;
            offset += 2;
            if offset + val_len > value.len() {
                break;
            }
            let val_str = String::from_utf8_lossy(&value[offset..offset + val_len]).to_string();
            offset += val_len;
            tags.insert(key_str, serde_json::Value::String(val_str));
        }
        Some(serde_json::Value::Object(tags))
    }

    fn decode_ts_kv(key: &[u8], value: &[u8]) -> Option<serde_json::Value> {
        let tags_hash = u64::from_be_bytes(key[0..8].try_into().ok()?);
        let timestamp = i64::from_be_bytes(key[8..16].try_into().ok()?);
        let ts_secs = timestamp / 1_000_000;
        let ts_us = timestamp % 1_000_000;

        let mut result = serde_json::Map::new();
        result.insert("tags_hash".to_string(), serde_json::Value::String(format!("0x{:016x}", tags_hash)));
        result.insert("timestamp_us".to_string(), serde_json::json!(timestamp));
        result.insert("timestamp".to_string(), serde_json::Value::String(format!("{}.{}", ts_secs, ts_us)));

        if value.len() < 2 {
            return Some(serde_json::Value::Object(result));
        }
        let num_fields = u16::from_be_bytes([value[0], value[1]]) as usize;
        let mut offset = 2;
        let mut fields = serde_json::Map::new();
        for _ in 0..num_fields {
            if offset + 2 > value.len() {
                break;
            }
            let name_len = u16::from_be_bytes([value[offset], value[offset + 1]]) as usize;
            offset += 2;
            if offset + name_len > value.len() {
                break;
            }
            let name = String::from_utf8_lossy(&value[offset..offset + name_len]).to_string();
            offset += name_len;
            if offset + 1 > value.len() {
                break;
            }
            let type_tag = value[offset];
            offset += 1;
            match type_tag {
                0x00 => {
                    if offset + 8 <= value.len() {
                        let v = f64::from_be_bytes(value[offset..offset + 8].try_into().unwrap_or([0; 8]));
                        fields.insert(name, serde_json::json!(v));
                        offset += 8;
                    }
                }
                0x01 => {
                    if offset + 8 <= value.len() {
                        let v = i64::from_be_bytes(value[offset..offset + 8].try_into().unwrap_or([0; 8]));
                        fields.insert(name, serde_json::json!(v));
                        offset += 8;
                    }
                }
                0x02 => {
                    if offset + 2 <= value.len() {
                        let str_len = u16::from_be_bytes([value[offset], value[offset + 1]]) as usize;
                        offset += 2;
                        if offset + str_len <= value.len() {
                            let s = String::from_utf8_lossy(&value[offset..offset + str_len]).to_string();
                            fields.insert(name, serde_json::Value::String(s));
                            offset += str_len;
                        }
                    }
                }
                0x03 => {
                    if offset + 1 <= value.len() {
                        let b = value[offset] != 0;
                        fields.insert(name, serde_json::json!(b));
                        offset += 1;
                    }
                }
                _ => {
                    fields.insert(name, serde_json::Value::String(format!("<unknown type 0x{:02x}>", type_tag)));
                    break;
                }
            }
        }
        result.insert("fields".to_string(), serde_json::Value::Object(fields));
        Some(serde_json::Value::Object(result))
    }

    fn collect_rocksdb_metrics(&self) -> (u64, u64, i32, u64, u64) {
        let any_ref = self.engine.as_any();
        if let Some(rocksdb_engine) = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>() {
            let db = rocksdb_engine.db();
            let memory_bytes = db.property_int_value(rocksdb::properties::ESTIMATE_TABLE_READERS_MEM).ok().flatten().unwrap_or(0)
                + db.property_int_value(rocksdb::properties::CUR_SIZE_ALL_MEM_TABLES).ok().flatten().unwrap_or(0)
                + db.property_int_value(rocksdb::properties::BLOCK_CACHE_USAGE).ok().flatten().unwrap_or(0);
            let disk_bytes = db.property_int_value(rocksdb::properties::TOTAL_SST_FILES_SIZE).ok().flatten().unwrap_or(0);
            let l0_count = db.property_int_value(rocksdb::properties::num_files_at_level(0)).ok().flatten().unwrap_or(0) as i32;
            let total_keys = db.property_int_value(rocksdb::properties::ESTIMATE_NUM_KEYS).ok().flatten().unwrap_or(0);
            let compaction_count = db.property_int_value("rocksdb.cumulative.compaction.bytes").ok().flatten().unwrap_or(0);
            (memory_bytes, disk_bytes, l0_count, compaction_count, total_keys)
        } else {
            (0, 0, 0, 0, 0)
        }
    }

    fn collect_write_read_counts(&self) -> (u64, u64) {
        let any_ref = self.engine.as_any();
        if let Some(rocksdb_engine) = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>() {
            let db = rocksdb_engine.db();
            let write_count = db.property_int_value("rocksdb.number.keys.written").ok().flatten().unwrap_or(0);
            let read_count = db.property_int_value("rocksdb.number.keys.read").ok().flatten().unwrap_or(0);
            (write_count, read_count)
        } else {
            (0, 0)
        }
    }

    fn collect_system_metrics(&self) -> (f64, f64, f64, f64) {
        let cpu_pct = self.read_cpu_usage();
        let sys_memory_pct = Self::read_memory_pct();
        let sys_disk_pct = Self::read_disk_pct();
        let load_avg = Self::read_load_avg();
        (cpu_pct, sys_memory_pct, sys_disk_pct, load_avg)
    }

    fn read_cpu_usage(&self) -> f64 {
        let current = Self::parse_proc_stat();
        let mut state = self.sys_state.lock().unwrap();
        match (&state.prev_cpu, &current) {
            (Some(prev), Some(curr)) => {
                let prev_total = prev.user + prev.nice + prev.system + prev.idle + prev.iowait;
                let curr_total = curr.user + curr.nice + curr.system + curr.idle + curr.iowait;
                let prev_idle = prev.idle + prev.iowait;
                let curr_idle = curr.idle + curr.iowait;
                let diff_total = curr_total as f64 - prev_total as f64;
                let diff_idle = curr_idle as f64 - prev_idle as f64;
                state.prev_cpu = current;
                if diff_total > 0.0 {
                    ((diff_total - diff_idle) / diff_total) * 100.0
                } else {
                    0.0
                }
            }
            _ => {
                state.prev_cpu = current;
                0.0
            }
        }
    }

    fn parse_proc_stat() -> Option<CpuSample> {
        let content = std::fs::read_to_string("/proc/stat").ok()?;
        let line = content.lines().next()?;
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 6 || parts[0] != "cpu" {
            return None;
        }
        Some(CpuSample {
            user: parts.get(1)?.parse().ok()?,
            nice: parts.get(2)?.parse().ok()?,
            system: parts.get(3)?.parse().ok()?,
            idle: parts.get(4)?.parse().ok()?,
            iowait: parts.get(5)?.parse().ok()?,
        })
    }

    fn read_memory_pct() -> f64 {
        let (total, used) = Self::memory_info();
        if total > 0 { (used as f64 / total as f64) * 100.0 } else { 0.0 }
    }

    fn memory_info() -> (u64, u64) {
        let info = sys_info::mem_info();
        match info {
            Ok(m) => {
                let total = m.total * 1024;
                let used = total.saturating_sub(m.avail * 1024);
                (total, used)
            }
            Err(_) => (0, 0),
        }
    }

    fn read_disk_pct() -> f64 {
        let (total, used) = Self::disk_info();
        if total > 0 { (used as f64 / total as f64) * 100.0 } else { 0.0 }
    }

    fn disk_info() -> (u64, u64) {
        let info = sys_info::disk_info();
        match info {
            Ok(d) => {
                let total = d.total * 1024;
                let used = total.saturating_sub(d.free * 1024);
                (total, used)
            }
            Err(_) => (0, 0),
        }
    }

    fn read_load_avg() -> f64 {
        let content = std::fs::read_to_string("/proc/loadavg").ok();
        match content {
            Some(c) => c.split_whitespace().next().and_then(|s| s.parse().ok()).unwrap_or(0.0),
            None => sys_info::loadavg().map(|l| l.one).unwrap_or(0.0),
        }
    }

    fn system_total_memory() -> u64 {
        Self::memory_info().0
    }

    fn system_total_disk() -> u64 {
        Self::disk_info().0
    }

    fn collect_network_stats(&self) -> (u64, u64) {
        let (rx, tx) = Self::read_proc_net_dev();
        let mut state = self.sys_state.lock().unwrap();
        let delta_rx = rx.saturating_sub(state.prev_net_rx);
        let delta_tx = tx.saturating_sub(state.prev_net_tx);
        state.prev_net_rx = rx;
        state.prev_net_tx = tx;
        (delta_rx, delta_tx)
    }

    fn read_proc_net_dev() -> (u64, u64) {
        let content = match std::fs::read_to_string("/proc/net/dev") {
            Ok(c) => c,
            Err(_) => return (0, 0),
        };
        let mut total_rx: u64 = 0;
        let mut total_tx: u64 = 0;
        for line in content.lines().skip(2) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 10 {
                continue;
            }
            let ifname = parts[0].trim_end_matches(':');
            if ifname == "lo" {
                continue;
            }
            if let Ok(rx) = parts[1].parse::<u64>() {
                total_rx += rx;
            }
            if let Ok(tx) = parts[9].parse::<u64>() {
                total_tx += tx;
            }
        }
        (total_rx, total_tx)
    }

    fn read_temperatures() -> (f64, f64) {
        let cpu_temp = Self::read_thermal_zones(&[
            "/sys/class/thermal/thermal_zone0/temp",
            "/sys/class/hwmon/hwmon0/temp1_input",
        ]);
        let gpu_temp = Self::read_thermal_zones(&[
            "/sys/class/thermal/thermal_zone1/temp",
            "/sys/class/thermal/thermal_zone2/temp",
            "/sys/class/hwmon/hwmon1/temp1_input",
        ]);
        (cpu_temp, gpu_temp)
    }

    fn read_thermal_zones(paths: &[&str]) -> f64 {
        for path in paths {
            if let Ok(content) = std::fs::read_to_string(path) {
                let trimmed = content.trim();
                if let Ok(millidegrees) = trimmed.parse::<f64>() {
                    if millidegrees > 1000.0 {
                        return millidegrees / 1000.0;
                    }
                    return millidegrees;
                }
            }
        }
        0.0
    }
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_decode(hex: &str) -> Result<Vec<u8>, String> {
    let hex = hex.trim();
    if hex.is_empty() {
        return Ok(vec![]);
    }
    if hex.len() % 2 != 0 {
        return Err("hex string must have even length".to_string());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

fn ascii_repr(data: &[u8]) -> String {
    data.iter()
        .map(|&b| {
            if b >= 0x20 && b < 0x7f {
                b as char
            } else {
                '.'
            }
        })
        .collect()
}
