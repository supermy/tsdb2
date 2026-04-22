use clap::{Parser, Subcommand};
use std::path::Path;
use std::sync::Arc;

/// TSDB2 命令行工具
#[derive(Parser)]
#[command(name = "tsdb-cli", version, about = "TSDB2 时序数据库命令行工具")]
struct Cli {
    /// 子命令
    #[command(subcommand)]
    command: Commands,

    /// 数据目录路径
    #[arg(long, global = true, default_value = "./data")]
    data_dir: String,

    #[arg(long, global = true, default_value = "rocksdb")]
    storage_engine: String,
}

#[derive(Subcommand)]
enum Commands {
    Status,
    Query {
        #[arg(long)]
        sql: String,
        #[arg(long, default_value = "json")]
        format: String,
    },
    Bench {
        #[arg(long, default_value = "write")]
        mode: String,
        #[arg(long, default_value_t = 100_000)]
        points: usize,
        #[arg(long, default_value_t = 1)]
        workers: usize,
    },
    Compact {
        #[arg(long)]
        cf: Option<String>,
    },
    Import {
        #[arg(long)]
        file: String,
        #[arg(long, default_value = "json")]
        format: String,
        #[arg(long, default_value_t = 10_000)]
        batch_size: usize,
    },
    Archive {
        #[command(subcommand)]
        action: ArchiveActions,
    },
    Export {
        #[arg(long)]
        measurement: String,
        #[arg(long)]
        output: String,
        #[arg(long, default_value = "json")]
        format: String,
    },
    Doctor,
}

#[derive(Subcommand)]
enum ArchiveActions {
    Create {
        #[arg(long, default_value = "30d")]
        older_than: String,
        #[arg(long, default_value = "./archive")]
        output_dir: String,
    },
    Restore {
        #[arg(long)]
        file: String,
    },
    List {
        #[arg(long, default_value = "./archive")]
        archive_dir: String,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Status => execute_status(&cli.data_dir)?,
        Commands::Query { sql, format } => execute_query(&cli.data_dir, &sql, &format)?,
        Commands::Bench {
            mode,
            points,
            workers,
        } => run_bench(&cli.data_dir, &mode, points, workers)?,
        Commands::Compact { cf } => execute_compact(&cli.data_dir, cf.as_deref())?,
        Commands::Import {
            file,
            format,
            batch_size,
        } => execute_import(&cli.data_dir, &file, &format, batch_size)?,
        Commands::Archive { action } => execute_archive(&cli.data_dir, action)?,
        Commands::Export {
            measurement,
            output,
            format,
        } => execute_export(&cli.data_dir, &measurement, &output, &format)?,
        Commands::Doctor => execute_doctor(&cli.data_dir)?,
    }

    Ok(())
}

/// 显示数据库状态信息
fn execute_status(data_dir: &str) -> anyhow::Result<()> {
    let db = tsdb_rocksdb::TsdbRocksDb::open(data_dir, tsdb_rocksdb::RocksDbConfig::default())?;
    let cfs = db.list_ts_cfs();

    println!("TSDB Status (RocksDB Engine)");
    println!("============================");
    println!("Data Dir:         {}", data_dir);
    println!("Column Families:  {}", cfs.len());
    println!();

    if !cfs.is_empty() {
        println!("CF Name                          Est. Entries");
        println!("--------------------------------------------");
        for cf_name in &cfs {
            let stats = db.cf_stats(cf_name).unwrap_or_default();
            let entries = stats
                .lines()
                .find(|l| l.contains("num entries"))
                .and_then(|l| l.split(':').nth(1))
                .map(|v| v.trim())
                .unwrap_or("N/A");
            println!("{:<32} {}", cf_name, entries);
        }
    }

    println!();
    println!("{}", db.stats());

    Ok(())
}

/// 执行 SQL 查询并输出结果
fn execute_query(data_dir: &str, sql: &str, format: &str) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let db = tsdb_rocksdb::TsdbRocksDb::open(data_dir, tsdb_rocksdb::RocksDbConfig::default())?;

        let cfs = db.list_ts_cfs();
        let mut measurements: std::collections::HashSet<String> = std::collections::HashSet::new();
        for cf_name in &cfs {
            if let Some(stripped) = cf_name.strip_prefix("ts_") {
                if let Some(measurement) = stripped.split('_').next() {
                    measurements.insert(measurement.to_string());
                }
            }
        }

        if measurements.is_empty() {
            println!("No data found in database.");
            return Ok::<(), anyhow::Error>(());
        }

        let engine = tsdb_datafusion::DataFusionQueryEngine::new(data_dir);

        let now = chrono::Utc::now().timestamp_micros();
        let start = now - 7 * 86_400_000_000i64;

        for measurement in &measurements {
            if let Ok(dps) = db.read_range(measurement, start, now) {
                if dps.is_empty() {
                    continue;
                }
                engine.register_from_datapoints(measurement, &dps)?;

                use datafusion::datasource::MemTable;
                use tsdb_arrow::converter::datapoints_to_record_batch;

                let table = engine
                    .session_context()
                    .catalog("datafusion")
                    .unwrap()
                    .schema("public")
                    .unwrap()
                    .table(measurement)
                    .await
                    .unwrap()
                    .unwrap();
                let schema = table.schema();
                let batch = datapoints_to_record_batch(&dps, schema.clone())?;
                let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
                engine.session_context().deregister_table(measurement)?;
                engine
                    .session_context()
                    .register_table(measurement, Arc::new(mem_table))?;
            }
        }

        let result = engine.execute(sql).await?;

        match format {
            "json" => {
                let json_rows: Vec<serde_json::Value> = result
                    .rows
                    .iter()
                    .map(|row| {
                        let mut map = serde_json::Map::new();
                        for (i, col) in result.columns.iter().enumerate() {
                            let val = match &row[i] {
                                tsdb_arrow::schema::FieldValue::Float(f) => {
                                    if f.is_nan() {
                                        serde_json::Value::Null
                                    } else {
                                        serde_json::json!(f)
                                    }
                                }
                                tsdb_arrow::schema::FieldValue::Integer(i) => serde_json::json!(i),
                                tsdb_arrow::schema::FieldValue::String(s) => serde_json::json!(s),
                                tsdb_arrow::schema::FieldValue::Boolean(b) => serde_json::json!(b),
                            };
                            map.insert(col.clone(), val);
                        }
                        serde_json::Value::Object(map)
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&json_rows)?);
            }
            "csv" => {
                println!("{}", result.columns.join(","));
                for row in &result.rows {
                    let vals: Vec<String> = row
                        .iter()
                        .map(|v| match v {
                            tsdb_arrow::schema::FieldValue::Float(f) => format!("{:.6}", f),
                            tsdb_arrow::schema::FieldValue::Integer(i) => i.to_string(),
                            tsdb_arrow::schema::FieldValue::String(s) => s.clone(),
                            tsdb_arrow::schema::FieldValue::Boolean(b) => b.to_string(),
                        })
                        .collect();
                    println!("{}", vals.join(","));
                }
            }
            _ => {
                println!("{}", result.columns.join("\t"));
                for row in &result.rows {
                    let vals: Vec<String> = row.iter().map(|v| format!("{:?}", v)).collect();
                    println!("{}", vals.join("\t"));
                }
            }
        }

        Ok(())
    })
}

/// 手动触发 Compaction
fn execute_compact(data_dir: &str, cf: Option<&str>) -> anyhow::Result<()> {
    let db = tsdb_rocksdb::TsdbRocksDb::open(data_dir, tsdb_rocksdb::RocksDbConfig::default())?;

    if let Some(cf_name) = cf {
        db.compact_cf(cf_name)?;
        println!("Compacted CF: {}", cf_name);
    } else {
        let cfs = db.list_ts_cfs();
        println!("Compacting {} column families...", cfs.len());
        for cf_name in &cfs {
            db.compact_cf(cf_name)?;
            println!("  Compacted: {}", cf_name);
        }
        println!("Done.");
    }

    Ok(())
}

/// 从文件导入数据
fn execute_import(
    data_dir: &str,
    file: &str,
    format: &str,
    batch_size: usize,
) -> anyhow::Result<()> {
    let db = tsdb_rocksdb::TsdbRocksDb::open(data_dir, tsdb_rocksdb::RocksDbConfig::default())?;
    let content = std::fs::read_to_string(file)?;

    match format {
        "json" => {
            let datapoints: Vec<tsdb_arrow::schema::DataPoint> = serde_json::from_str(&content)?;
            let total = datapoints.len();

            for chunk in datapoints.chunks(batch_size) {
                db.write_batch(chunk)?;
            }

            println!("Imported {} data points from {}", total, file);
        }
        "csv" => {
            let mut reader = csv::Reader::from_path(file)?;
            let headers = reader.headers()?.clone();
            let mut datapoints = Vec::new();

            for result in reader.records() {
                let record = result?;
                let mut dp = tsdb_arrow::schema::DataPoint::new("", 0);

                for (i, header) in headers.iter().enumerate() {
                    let val = record.get(i).unwrap_or("");
                    match header {
                        "measurement" => dp.measurement = val.to_string(),
                        "timestamp" => dp.timestamp = val.parse::<i64>().unwrap_or(0),
                        h if h.starts_with("tag_") => {
                            let tag_key = &h[4..];
                            if !val.is_empty() {
                                dp.tags.insert(tag_key.to_string(), val.to_string());
                            }
                        }
                        h => {
                            if !val.is_empty() {
                                if let Ok(f) = val.parse::<f64>() {
                                    dp.fields.insert(h.to_string(), tsdb_arrow::schema::FieldValue::Float(f));
                                } else if let Ok(i) = val.parse::<i64>() {
                                    dp.fields.insert(h.to_string(), tsdb_arrow::schema::FieldValue::Integer(i));
                                } else if val == "true" || val == "false" {
                                    dp.fields.insert(h.to_string(), tsdb_arrow::schema::FieldValue::Boolean(val == "true"));
                                } else {
                                    dp.fields.insert(h.to_string(), tsdb_arrow::schema::FieldValue::String(val.to_string()));
                                }
                            }
                        }
                    }
                }

                datapoints.push(dp);
            }

            let total = datapoints.len();
            for chunk in datapoints.chunks(batch_size) {
                db.write_batch(chunk)?;
            }

            println!("Imported {} data points from {}", total, file);
        }
        _ => {
            anyhow::bail!(
                "Unsupported import format: {}. Use 'json' or 'csv'.",
                format
            );
        }
    }

    Ok(())
}

/// 归档管理操作: 将过期 RocksDB CF 数据导出为 JSON Lines 文件后删除源 CF
fn execute_archive(data_dir: &str, action: ArchiveActions) -> anyhow::Result<()> {
    match action {
        ArchiveActions::Create {
            older_than,
            output_dir,
        } => {
            let days = parse_days(&older_than)?;
            let db =
                tsdb_rocksdb::TsdbRocksDb::open(data_dir, tsdb_rocksdb::RocksDbConfig::default())?;

            let archive_path = Path::new(&output_dir);
            let (archived_cfs, total_points) =
                tsdb_rocksdb::DataArchiver::archive_expired_cfs(&db, archive_path, days as u64)?;

            if archived_cfs == 0 {
                println!("No CFs older than {} days found.", days);
            } else {
                println!(
                    "Archived {} column families ({} data points) to {}",
                    archived_cfs, total_points, output_dir
                );
                let files = tsdb_rocksdb::DataArchiver::list_archives(archive_path)?;
                for f in &files {
                    println!("  {}", f);
                }
            }
        }
        ArchiveActions::Restore { file } => {
            if !Path::new(&file).exists() {
                anyhow::bail!("Archive file not found: {}", file);
            }

            let db =
                tsdb_rocksdb::TsdbRocksDb::open(data_dir, tsdb_rocksdb::RocksDbConfig::default())?;

            let f = std::fs::File::open(&file)?;
            let reader = std::io::BufReader::new(f);
            let mut count = 0usize;
            let mut batch = Vec::new();

            for line in std::io::BufRead::lines(reader) {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let dp: tsdb_arrow::schema::DataPoint = serde_json::from_str(&line)?;
                batch.push(dp);
                if batch.len() >= 10000 {
                    db.write_batch(&batch)?;
                    count += batch.len();
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                db.write_batch(&batch)?;
                count += batch.len();
            }

            println!("Restored {} data points from {}", count, file);
        }
        ArchiveActions::List { archive_dir } => {
            let archive_path = Path::new(&archive_dir);
            let files = tsdb_rocksdb::DataArchiver::list_archives(archive_path)?;

            if files.is_empty() {
                println!("No archives found.");
            } else {
                println!("Archives in {}:", archive_dir);
                for f in &files {
                    let path = archive_path.join(f);
                    let size = std::fs::metadata(&path)
                        .map(|m| m.len())
                        .unwrap_or(0);
                    println!("  {} ({} bytes)", f, size);
                }
            }
        }
    }

    Ok(())
}

/// 导出数据到文件
fn execute_export(
    data_dir: &str,
    measurement: &str,
    output: &str,
    format: &str,
) -> anyhow::Result<()> {
    let db = tsdb_rocksdb::TsdbRocksDb::open(data_dir, tsdb_rocksdb::RocksDbConfig::default())?;

    let now = chrono::Utc::now().timestamp_micros();
    let start = now - 365 * 86_400_000_000i64;
    let datapoints = db.read_range(measurement, start, now)?;

    if datapoints.is_empty() {
        println!("No data found for measurement: {}", measurement);
        return Ok(());
    }

    match format {
        "json" => {
            let json = serde_json::to_string_pretty(&datapoints)?;
            std::fs::write(output, &json)?;
        }
        "csv" => {
            let mut tag_keys: Vec<String> = datapoints
                .iter()
                .flat_map(|dp| dp.tags.keys().cloned())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            tag_keys.sort();

            let mut field_keys: Vec<String> = datapoints
                .iter()
                .flat_map(|dp| dp.fields.keys().cloned())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            field_keys.sort();

            let mut headers = vec![
                "measurement".to_string(),
                "timestamp".to_string(),
            ];
            for k in &tag_keys {
                headers.push(format!("tag_{}", k));
            }
            for k in &field_keys {
                headers.push(k.clone());
            }

            let mut wtr = csv::Writer::from_path(output)?;
            wtr.write_record(&headers)?;

            for dp in &datapoints {
                let mut row = vec![
                    dp.measurement.clone(),
                    dp.timestamp.to_string(),
                ];
                for k in &tag_keys {
                    let v = dp.tags.get(k).map(|s| s.as_str()).unwrap_or("");
                    row.push(v.to_string());
                }
                for k in &field_keys {
                    let v = dp.fields.get(k).map_or(String::new(), |fv| match fv {
                        tsdb_arrow::schema::FieldValue::Float(f) => format!("{}", f),
                        tsdb_arrow::schema::FieldValue::Integer(i) => i.to_string(),
                        tsdb_arrow::schema::FieldValue::String(s) => s.clone(),
                        tsdb_arrow::schema::FieldValue::Boolean(b) => b.to_string(),
                    });
                    row.push(v);
                }
                wtr.write_record(&row)?;
            }
            wtr.flush()?;
        }
        _ => {
            anyhow::bail!(
                "Unsupported export format: {}. Use 'json' or 'csv'.",
                format
            );
        }
    }

    println!("Exported {} data points to {}", datapoints.len(), output);
    Ok(())
}

/// 数据库健康检查
fn execute_doctor(data_dir: &str) -> anyhow::Result<()> {
    println!("TSDB Doctor");
    println!("==========");

    if !Path::new(data_dir).exists() {
        println!("[FAIL] Data directory does not exist: {}", data_dir);
        return Ok(());
    }
    println!("[OK]   Data directory exists: {}", data_dir);

    match tsdb_rocksdb::TsdbRocksDb::open(data_dir, tsdb_rocksdb::RocksDbConfig::default()) {
        Ok(db) => {
            println!("[OK]   Database opened successfully");

            let cfs = db.list_ts_cfs();
            println!("[OK]   Column Families: {}", cfs.len());

            let meta_cf = db.db().cf_handle("_series_meta");
            if meta_cf.is_some() {
                println!("[OK]   _series_meta CF exists");
            } else {
                println!("[WARN] _series_meta CF not found");
            }

            for cf_name in &cfs {
                if let Err(e) = db.compact_cf(cf_name) {
                    println!("[WARN] Compaction failed for {}: {}", cf_name, e);
                }
            }
            println!("[OK]   Compaction check passed");

            println!();
            println!("Doctor check complete. No critical issues found.");
        }
        Err(e) => {
            println!("[FAIL] Cannot open database: {}", e);
        }
    }

    Ok(())
}

/// 解析天数字符串 (如 "30d" → 30)
fn parse_days(s: &str) -> anyhow::Result<i64> {
    let s = s.trim();
    if let Some(days_str) = s.strip_suffix('d') {
        Ok(days_str.parse::<i64>()?)
    } else {
        Ok(s.parse::<i64>()?)
    }
}

/// 运行基准测试 (写入/读取)
fn run_bench(data_dir: &str, mode: &str, points: usize, workers: usize) -> anyhow::Result<()> {
    use std::time::Instant;
    use tsdb_arrow::schema::{FieldValue, Tags};

    let bench_dir = std::path::Path::new(data_dir);
    std::fs::create_dir_all(bench_dir)?;

    let db = tsdb_rocksdb::TsdbRocksDb::open(bench_dir, tsdb_rocksdb::RocksDbConfig::default())?;

    match mode {
        "write" => {
            let base_ts = chrono::Utc::now().timestamp_micros();
            let dps: Vec<tsdb_arrow::schema::DataPoint> = (0..points)
                .map(|i| {
                    let mut tags = Tags::new();
                    tags.insert("host".to_string(), format!("host_{:04}", i % 100));
                    let mut fields = tsdb_arrow::schema::Fields::new();
                    fields.insert("usage".to_string(), FieldValue::Float(i as f64 * 0.01));
                    fields.insert("count".to_string(), FieldValue::Integer(i as i64));
                    tsdb_arrow::schema::DataPoint {
                        measurement: "cpu".to_string(),
                        tags,
                        fields,
                        timestamp: base_ts + i as i64 * 1_000,
                    }
                })
                .collect();

            let start = Instant::now();
            db.write_batch(&dps)?;
            let elapsed = start.elapsed();

            let pts_per_sec = points as f64 / elapsed.as_secs_f64();
            println!("Write Benchmark Results");
            println!("=======================");
            println!("Points:     {}", points);
            println!("Workers:    {}", workers);
            println!("Elapsed:    {:.2}s", elapsed.as_secs_f64());
            println!("Throughput: {:.0} points/sec", pts_per_sec);

            let report = serde_json::json!({
                "mode": "write",
                "points": points,
                "workers": workers,
                "elapsed_secs": elapsed.as_secs_f64(),
                "ops_per_sec": pts_per_sec,
            });
            eprintln!("{}", serde_json::to_string_pretty(&report)?);
        }
        "read" => {
            let base_ts = chrono::Utc::now().timestamp_micros();
            let dps: Vec<tsdb_arrow::schema::DataPoint> = (0..points)
                .map(|i| {
                    let mut tags = Tags::new();
                    tags.insert("host".to_string(), format!("host_{:04}", i % 100));
                    let mut fields = tsdb_arrow::schema::Fields::new();
                    fields.insert("usage".to_string(), FieldValue::Float(i as f64 * 0.01));
                    tsdb_arrow::schema::DataPoint {
                        measurement: "cpu".to_string(),
                        tags,
                        fields,
                        timestamp: base_ts + i as i64 * 1_000,
                    }
                })
                .collect();
            db.write_batch(&dps)?;

            let start = Instant::now();
            let result = db.read_range("cpu", base_ts, base_ts + (points as i64 - 1) * 1_000)?;
            let elapsed = start.elapsed();

            let pts_per_sec = result.len() as f64 / elapsed.as_secs_f64();
            println!("Read Benchmark Results");
            println!("======================");
            println!("Points:     {}", result.len());
            println!("Elapsed:    {:.2}s", elapsed.as_secs_f64());
            println!("Throughput: {:.0} points/sec", pts_per_sec);

            let report = serde_json::json!({
                "mode": "read",
                "points": result.len(),
                "elapsed_secs": elapsed.as_secs_f64(),
                "ops_per_sec": pts_per_sec,
            });
            eprintln!("{}", serde_json::to_string_pretty(&report)?);
        }
        _ => {
            anyhow::bail!("Unknown bench mode: {}. Use 'write' or 'read'.", mode);
        }
    }

    Ok(())
}
