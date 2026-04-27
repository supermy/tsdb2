use crate::error::Result;
use crate::protocol::*;
use std::sync::Arc;
use tsdb_arrow::engine::StorageEngine;
use tsdb_arrow::schema::{DataPoint, FieldValue, Tags};

struct BusinessScenario {
    name: String,
    tag_names: Vec<String>,
    tag_combinations: Vec<Tags>,
    field_names: Vec<String>,
    field_generator: fn(usize, usize) -> std::collections::BTreeMap<String, FieldValue>,
}

pub struct TestRunner {
    engine: Arc<dyn StorageEngine>,
    parquet_dir: std::path::PathBuf,
}

impl TestRunner {
    pub fn new(engine: Arc<dyn StorageEngine>, parquet_dir: std::path::PathBuf) -> Self {
        Self {
            engine,
            parquet_dir,
        }
    }

    pub async fn generate_business_data(
        &self,
        scenario: &str,
        points_per_series: usize,
        skip_auto_demote: bool,
    ) -> Result<serde_json::Value> {
        let start = std::time::Instant::now();
        let now = chrono::Utc::now();
        let today = now.date_naive();
        let mut total_written = 0usize;
        let mut measurements = Vec::new();
        let mut demoted_warm = Vec::new();
        let mut demoted_cold = Vec::new();

        let hot_days = 3usize;
        let warm_days = 11usize;
        let cold_days = 10usize;
        let total_days = hot_days + warm_days + cold_days;

        let scenarios = self.build_scenarios(scenario);
        for s in &scenarios {
            let mut count = 0usize;
            for day_offset in 0..total_days {
                let day = today - chrono::TimeDelta::days(day_offset as i64);
                let day_start_ts = day
                    .and_hms_micro_opt(0, 0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .timestamp_micros();
                let points_this_day = if day_offset == 0 {
                    points_per_series
                } else {
                    points_per_series / 2
                };

                for series_tags in &s.tag_combinations {
                    let mut batch = Vec::with_capacity(100);
                    for i in 0..points_this_day {
                        let ts = day_start_ts
                            + (i as i64 * 3_600_000_000 / points_this_day.max(1) as i64);
                        let fields = (s.field_generator)(i, points_this_day);
                        let dp = DataPoint {
                            measurement: s.name.clone(),
                            tags: series_tags.clone(),
                            fields: fields.into(),
                            timestamp: ts,
                        };
                        batch.push(dp);
                        if batch.len() >= 100 {
                            self.engine
                                .write_batch(&batch)
                                .map_err(|e| crate::error::AdminError::Storage(e.to_string()))?;
                            count += batch.len();
                            batch.clear();
                        }
                    }
                    if !batch.is_empty() {
                        self.engine
                            .write_batch(&batch)
                            .map_err(|e| crate::error::AdminError::Storage(e.to_string()))?;
                        count += batch.len();
                    }
                }
            }
            total_written += count;
            measurements.push(serde_json::json!({
                "name": s.name,
                "series_count": s.tag_combinations.len(),
                "points_per_series": points_per_series,
                "total_points": count,
                "fields": s.field_names,
                "tags": s.tag_names,
                "data_distribution": {
                    "hot_days": hot_days,
                    "warm_days": warm_days,
                    "cold_days": cold_days,
                },
            }));
        }

        if let Err(e) = self.engine.flush() {
            tracing::warn!("flush after generate: {}", e);
        }
        std::thread::sleep(std::time::Duration::from_millis(500));

        if !skip_auto_demote {
            let any_ref = self.engine.as_any();
            if let Some(rocksdb_engine) = any_ref.downcast_ref::<tsdb_rocksdb::TsdbRocksDb>() {
                let ts_cfs = rocksdb_engine.list_ts_cfs();
                let mut warm_cfs = Vec::new();
                let mut cold_cfs = Vec::new();

                for cf_name in &ts_cfs {
                    if let Some(date_str) =
                        crate::lifecycle_api::LifecycleApi::extract_date_from_cf(cf_name)
                    {
                        if let Ok(cf_date) = chrono::NaiveDate::parse_from_str(&date_str, "%Y%m%d")
                        {
                            let age = (today - cf_date).num_days();
                            if age > 14 {
                                cold_cfs.push(cf_name.clone());
                            } else if age > 3 {
                                warm_cfs.push(cf_name.clone());
                            }
                        }
                    }
                }

                if !warm_cfs.is_empty() {
                    let lifecycle = crate::lifecycle_api::LifecycleApi::new(
                        self.engine.clone(),
                        self.parquet_dir.clone(),
                    );
                    match lifecycle.demote_to_warm(&warm_cfs) {
                        Ok(results) => {
                            demoted_warm = results;
                            tracing::info!("auto-demoted {} CFs to warm", warm_cfs.len());
                        }
                        Err(e) => tracing::warn!("auto demote to warm failed: {}", e),
                    }
                }

                if !cold_cfs.is_empty() {
                    let lifecycle = crate::lifecycle_api::LifecycleApi::new(
                        self.engine.clone(),
                        self.parquet_dir.clone(),
                    );
                    match lifecycle.demote_to_cold(&cold_cfs) {
                        Ok(results) => {
                            demoted_cold = results;
                            tracing::info!("auto-demoted {} CFs to cold", cold_cfs.len());
                        }
                        Err(e) => tracing::warn!("auto demote to cold failed: {}", e),
                    }
                }
            }
        }

        let elapsed = start.elapsed();
        let auto_demote_info = if skip_auto_demote {
            serde_json::json!({
                "skipped": true,
                "note": "auto-demotion skipped, use lifecycle page to manually demote"
            })
        } else {
            serde_json::json!({
                "skipped": false,
                "demoted_to_warm": demoted_warm.len(),
                "demoted_to_cold": demoted_cold.len(),
                "warm_details": demoted_warm,
                "cold_details": demoted_cold,
            })
        };
        Ok(serde_json::json!({
            "scenario": scenario,
            "total_points": total_written,
            "elapsed_secs": elapsed.as_secs_f64(),
            "rate_per_sec": total_written as f64 / elapsed.as_secs_f64().max(0.001),
            "measurements": measurements,
            "lifecycle": {
                "hot_days": hot_days,
                "warm_days": warm_days,
                "cold_days": cold_days,
                "auto_demote": auto_demote_info,
            },
        }))
    }

    fn build_scenarios(&self, scenario: &str) -> Vec<BusinessScenario> {
        match scenario {
            "iot" => vec![
                BusinessScenario {
                    name: "iot_temperature".to_string(),
                    tag_names: vec!["device_id".into(), "location".into(), "floor".into()],
                    tag_combinations: Self::iot_tags(),
                    field_names: vec!["celsius".into(), "humidity_pct".into(), "status".into()],
                    field_generator: |i, n| {
                        let t = i as f64 / n as f64;
                        let mut m = std::collections::BTreeMap::new();
                        m.insert(
                            "celsius".to_string(),
                            FieldValue::Float(20.0 + 5.0 * (t * std::f64::consts::PI * 4.0).sin()),
                        );
                        m.insert(
                            "humidity_pct".to_string(),
                            FieldValue::Float(45.0 + 15.0 * (t * std::f64::consts::PI * 2.0).cos()),
                        );
                        m.insert(
                            "status".to_string(),
                            FieldValue::Integer(if i % 100 < 95 { 0i64 } else { 1i64 }),
                        );
                        m
                    },
                },
                BusinessScenario {
                    name: "iot_power".to_string(),
                    tag_names: vec!["device_id".into(), "location".into()],
                    tag_combinations: Self::iot_power_tags(),
                    field_names: vec![
                        "voltage".into(),
                        "current_amp".into(),
                        "power_watt".into(),
                        "energy_kwh".into(),
                    ],
                    field_generator: |i, n| {
                        let t = i as f64 / n as f64;
                        let mut m = std::collections::BTreeMap::new();
                        m.insert(
                            "voltage".to_string(),
                            FieldValue::Float(220.0 + 5.0 * (t * 50.0).sin()),
                        );
                        m.insert(
                            "current_amp".to_string(),
                            FieldValue::Float(2.5 + 1.0 * (t * 30.0).sin().abs()),
                        );
                        m.insert(
                            "power_watt".to_string(),
                            FieldValue::Float(550.0 + 200.0 * (t * 20.0).sin().abs()),
                        );
                        m.insert(
                            "energy_kwh".to_string(),
                            FieldValue::Float(i as f64 * 0.55 / 3600.0),
                        );
                        m
                    },
                },
            ],
            "devops" => vec![
                BusinessScenario {
                    name: "http_requests".to_string(),
                    tag_names: vec![
                        "service".into(),
                        "endpoint".into(),
                        "method".into(),
                        "status_code".into(),
                    ],
                    tag_combinations: Self::http_tags(),
                    field_names: vec![
                        "latency_ms".into(),
                        "request_size".into(),
                        "response_size".into(),
                    ],
                    field_generator: |i, n| {
                        let t = i as f64 / n as f64;
                        let mut m = std::collections::BTreeMap::new();
                        m.insert(
                            "latency_ms".to_string(),
                            FieldValue::Float(
                                50.0 + 200.0 * (t * 100.0).sin().abs()
                                    + (i % 50 == 0) as i32 as f64 * 500.0,
                            ),
                        );
                        m.insert(
                            "request_size".to_string(),
                            FieldValue::Integer((256 + (i * 7) % 2048) as i64),
                        );
                        m.insert(
                            "response_size".to_string(),
                            FieldValue::Integer((1024 + (i * 13) % 8192) as i64),
                        );
                        m
                    },
                },
                BusinessScenario {
                    name: "app_metrics".to_string(),
                    tag_names: vec!["service".into(), "instance".into(), "env".into()],
                    tag_combinations: Self::app_tags(),
                    field_names: vec![
                        "cpu_pct".into(),
                        "memory_mb".into(),
                        "goroutines".into(),
                        "gc_pause_ms".into(),
                        "connections".into(),
                    ],
                    field_generator: |i, n| {
                        let t = i as f64 / n as f64;
                        let mut m = std::collections::BTreeMap::new();
                        m.insert(
                            "cpu_pct".to_string(),
                            FieldValue::Float(30.0 + 40.0 * (t * 20.0).sin().abs()),
                        );
                        m.insert(
                            "memory_mb".to_string(),
                            FieldValue::Float(512.0 + 256.0 * (t * 10.0).sin().abs()),
                        );
                        m.insert(
                            "goroutines".to_string(),
                            FieldValue::Integer((100 + (i * 3) % 500) as i64),
                        );
                        m.insert(
                            "gc_pause_ms".to_string(),
                            FieldValue::Float(0.5 + 2.0 * (t * 50.0).sin().abs()),
                        );
                        m.insert(
                            "connections".to_string(),
                            FieldValue::Integer((50 + (i * 7) % 200) as i64),
                        );
                        m
                    },
                },
            ],
            "ecommerce" => vec![
                BusinessScenario {
                    name: "order_events".to_string(),
                    tag_names: vec!["region".into(), "channel".into(), "product_category".into()],
                    tag_combinations: Self::order_tags(),
                    field_names: vec!["amount".into(), "quantity".into(), "discount_pct".into()],
                    field_generator: |i, n| {
                        let t = i as f64 / n as f64;
                        let mut m = std::collections::BTreeMap::new();
                        m.insert(
                            "amount".to_string(),
                            FieldValue::Float(50.0 + 450.0 * (t * 5.0).sin().abs()),
                        );
                        m.insert(
                            "quantity".to_string(),
                            FieldValue::Integer((1 + i % 5) as i64),
                        );
                        m.insert(
                            "discount_pct".to_string(),
                            FieldValue::Float((i % 4) as f64 * 5.0),
                        );
                        m
                    },
                },
                BusinessScenario {
                    name: "user_activity".to_string(),
                    tag_names: vec!["platform".into(), "action".into(), "region".into()],
                    tag_combinations: Self::activity_tags(),
                    field_names: vec!["duration_secs".into(), "page_views".into(), "clicks".into()],
                    field_generator: |i, n| {
                        let t = i as f64 / n as f64;
                        let mut m = std::collections::BTreeMap::new();
                        m.insert(
                            "duration_secs".to_string(),
                            FieldValue::Float(30.0 + 300.0 * (t * 8.0).sin().abs()),
                        );
                        m.insert(
                            "page_views".to_string(),
                            FieldValue::Integer((3 + (i * 11) % 20) as i64),
                        );
                        m.insert(
                            "clicks".to_string(),
                            FieldValue::Integer((1 + (i * 3) % 10) as i64),
                        );
                        m
                    },
                },
            ],
            _ => vec![BusinessScenario {
                name: "business_metrics".to_string(),
                tag_names: vec!["region".into(), "department".into()],
                tag_combinations: Self::generic_tags(),
                field_names: vec!["value".into(), "count".into(), "score".into()],
                field_generator: |i, n| {
                    let t = i as f64 / n as f64;
                    let mut m = std::collections::BTreeMap::new();
                    m.insert(
                        "value".to_string(),
                        FieldValue::Float(100.0 + 50.0 * (t * std::f64::consts::PI * 4.0).sin()),
                    );
                    m.insert("count".to_string(), FieldValue::Integer(i as i64));
                    m.insert(
                        "score".to_string(),
                        FieldValue::Float(0.5 + 0.4 * (t * 6.0).sin().abs()),
                    );
                    m
                },
            }],
        }
    }

    fn iot_tags() -> Vec<Tags> {
        let locations = ["building_A", "building_B", "building_C"];
        let mut result = Vec::new();
        for loc in &locations {
            for floor in 1..=3 {
                for dev in 1..=4 {
                    let mut tags = Tags::new();
                    tags.insert("device_id".to_string(), format!("temp_sensor_{:03}", dev));
                    tags.insert("location".to_string(), loc.to_string());
                    tags.insert("floor".to_string(), format!("F{}", floor));
                    result.push(tags);
                }
            }
        }
        result
    }

    fn iot_power_tags() -> Vec<Tags> {
        let locations = ["building_A", "building_B"];
        let mut result = Vec::new();
        for loc in &locations {
            for dev in 1..=6 {
                let mut tags = Tags::new();
                tags.insert("device_id".to_string(), format!("power_meter_{:03}", dev));
                tags.insert("location".to_string(), loc.to_string());
                result.push(tags);
            }
        }
        result
    }

    fn http_tags() -> Vec<Tags> {
        let services = ["user-service", "order-service", "payment-service"];
        let endpoints = ["/api/v1/list", "/api/v1/detail", "/api/v1/create"];
        let methods = ["GET", "POST"];
        let codes = ["200", "404", "500"];
        let mut result = Vec::new();
        for svc in &services {
            for ep in &endpoints {
                for method in &methods {
                    for code in &codes {
                        let mut tags = Tags::new();
                        tags.insert("service".to_string(), svc.to_string());
                        tags.insert("endpoint".to_string(), ep.to_string());
                        tags.insert("method".to_string(), method.to_string());
                        tags.insert("status_code".to_string(), code.to_string());
                        result.push(tags);
                    }
                }
            }
        }
        result
    }

    fn app_tags() -> Vec<Tags> {
        let services = [
            "user-service",
            "order-service",
            "payment-service",
            "gateway",
        ];
        let envs = ["production", "staging"];
        let mut result = Vec::new();
        for svc in &services {
            for env in &envs {
                for inst in 1..=3 {
                    let mut tags = Tags::new();
                    tags.insert("service".to_string(), svc.to_string());
                    tags.insert("instance".to_string(), format!("{}-{}", svc, inst));
                    tags.insert("env".to_string(), env.to_string());
                    result.push(tags);
                }
            }
        }
        result
    }

    fn order_tags() -> Vec<Tags> {
        let regions = ["cn-north", "cn-south", "us-west", "eu-central"];
        let channels = ["web", "mobile", "miniapp"];
        let categories = ["electronics", "clothing", "food", "books"];
        let mut result = Vec::new();
        for region in &regions {
            for channel in &channels {
                for cat in &categories {
                    let mut tags = Tags::new();
                    tags.insert("region".to_string(), region.to_string());
                    tags.insert("channel".to_string(), channel.to_string());
                    tags.insert("product_category".to_string(), cat.to_string());
                    result.push(tags);
                }
            }
        }
        result
    }

    fn activity_tags() -> Vec<Tags> {
        let platforms = ["ios", "android", "web"];
        let actions = ["view", "click", "purchase", "share"];
        let regions = ["cn-north", "cn-south", "us-west"];
        let mut result = Vec::new();
        for platform in &platforms {
            for action in &actions {
                for region in &regions {
                    let mut tags = Tags::new();
                    tags.insert("platform".to_string(), platform.to_string());
                    tags.insert("action".to_string(), action.to_string());
                    tags.insert("region".to_string(), region.to_string());
                    result.push(tags);
                }
            }
        }
        result
    }

    fn generic_tags() -> Vec<Tags> {
        let regions = ["cn-north", "cn-south", "us-west", "eu-central"];
        let departments = ["engineering", "marketing", "sales", "finance"];
        let mut result = Vec::new();
        for region in &regions {
            for dept in &departments {
                let mut tags = Tags::new();
                tags.insert("region".to_string(), region.to_string());
                tags.insert("department".to_string(), dept.to_string());
                result.push(tags);
            }
        }
        result
    }

    pub async fn run_sql(&self, sql: &str) -> Result<BenchResult> {
        let start = std::time::Instant::now();
        let ctx = datafusion::prelude::SessionContext::new();
        let result = match ctx.sql(sql).await {
            Err(e) => Err(crate::error::AdminError::Storage(e.to_string())),
            Ok(df) => match df.collect().await {
                Err(e) => Err(crate::error::AdminError::Storage(e.to_string())),
                Ok(b) => {
                    Ok::<usize, crate::error::AdminError>(b.iter().map(|b| b.num_rows()).sum())
                }
            },
        };
        let elapsed = start.elapsed();
        let row_count = result.unwrap_or(0);

        Ok(BenchResult {
            operation: "sql".to_string(),
            total_points: row_count,
            elapsed_secs: elapsed.as_secs_f64(),
            rate_per_sec: if elapsed.as_secs_f64() > 0.0 {
                row_count as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            },
            avg_latency_us: elapsed.as_micros() as f64,
            p99_latency_us: elapsed.as_micros() as f64,
        })
    }

    pub async fn run_write_bench(
        &self,
        measurement: &str,
        total_points: usize,
        workers: usize,
        batch_size: usize,
    ) -> Result<BenchResult> {
        let start = std::time::Instant::now();
        let points_per_worker = if workers > 0 {
            total_points.div_ceil(workers)
        } else {
            total_points
        };
        let mut total_written = 0usize;
        let base_ts = chrono::Utc::now().timestamp_micros();

        for w in 0..workers.max(1) {
            let mut tags = Tags::new();
            tags.insert("host".to_string(), format!("worker-{}", w));
            let mut batch = Vec::with_capacity(batch_size);
            for i in 0..points_per_worker {
                let mut fields = std::collections::BTreeMap::new();
                fields.insert("value".to_string(), FieldValue::Float(i as f64));
                let dp = DataPoint {
                    measurement: measurement.to_string(),
                    tags: tags.clone(),
                    fields: fields.into(),
                    timestamp: base_ts + (total_written + i) as i64 * 1000,
                };
                batch.push(dp);
                if batch.len() >= batch_size {
                    self.engine
                        .write_batch(&batch)
                        .map_err(|e| crate::error::AdminError::Storage(e.to_string()))?;
                    total_written += batch.len();
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                self.engine
                    .write_batch(&batch)
                    .map_err(|e| crate::error::AdminError::Storage(e.to_string()))?;
                total_written += batch.len();
            }
        }

        let elapsed = start.elapsed();
        Ok(BenchResult {
            operation: "write".to_string(),
            total_points: total_written,
            elapsed_secs: elapsed.as_secs_f64(),
            rate_per_sec: if elapsed.as_secs_f64() > 0.0 {
                total_written as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            },
            avg_latency_us: (elapsed.as_micros() as f64) / (total_written.max(1) as f64),
            p99_latency_us: 0.0,
        })
    }

    pub async fn run_read_bench(
        &self,
        measurement: &str,
        queries: usize,
        _workers: usize,
    ) -> Result<BenchResult> {
        let now = chrono::Utc::now().timestamp_micros();
        let start = std::time::Instant::now();
        let mut total_rows = 0usize;

        for _ in 0..queries {
            let result = self
                .engine
                .read_range(measurement, &Tags::new(), now - 3_600_000_000, now)
                .map_err(|e| crate::error::AdminError::Storage(e.to_string()))?;
            total_rows += result.len();
        }

        let elapsed = start.elapsed();
        Ok(BenchResult {
            operation: "read".to_string(),
            total_points: total_rows,
            elapsed_secs: elapsed.as_secs_f64(),
            rate_per_sec: if elapsed.as_secs_f64() > 0.0 {
                total_rows as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            },
            avg_latency_us: (elapsed.as_micros() as f64) / (queries.max(1) as f64),
            p99_latency_us: 0.0,
        })
    }
}
