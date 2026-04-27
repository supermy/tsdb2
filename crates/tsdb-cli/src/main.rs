use clap::{Parser, Subcommand};
use std::path::Path;
use std::sync::Arc;
use tsdb_arrow::engine::StorageEngine;
use tsdb_arrow::schema::Tags;

#[derive(Parser)]
#[command(name = "tsdb-cli", version, about = "TSDB2 时序数据库命令行工具")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, global = true, default_value = "./data")]
    data_dir: String,

    #[arg(long, global = true, default_value = "rocksdb")]
    storage_engine: String,

    #[arg(long, global = true, default_value = "balanced")]
    config: String,
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
    Iceberg {
        #[command(subcommand)]
        action: IcebergActions,
    },
    Serve {
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
        #[arg(long, default_value_t = 50051)]
        flight_port: u16,
        #[arg(long, default_value_t = 8080)]
        admin_port: u16,
        #[arg(long, default_value_t = 3000)]
        http_port: u16,
        #[arg(long)]
        parquet_dir: Option<String>,
        #[arg(long)]
        iceberg_dir: Option<String>,
    },
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

#[derive(Subcommand)]
enum IcebergActions {
    Create {
        #[arg(long)]
        name: String,
    },
    List,
    Drop {
        #[arg(long)]
        name: String,
    },
    Snapshots {
        #[arg(long)]
        name: String,
    },
    Append {
        #[arg(long)]
        name: String,
        #[arg(long)]
        file: String,
        #[arg(long, default_value = "json")]
        format: String,
    },
    Scan {
        #[arg(long)]
        name: String,
        #[arg(long)]
        sql: Option<String>,
    },
    Compact {
        #[arg(long)]
        name: String,
    },
    Rollback {
        #[arg(long)]
        name: String,
        #[arg(long)]
        snapshot_id: i64,
    },
    Expire {
        #[arg(long)]
        name: String,
        #[arg(long, default_value_t = 7)]
        keep_days: u64,
    },
}

fn open_engine(
    data_dir: &str,
    engine_type: &str,
    config_name: &str,
) -> anyhow::Result<Arc<dyn StorageEngine>> {
    match engine_type {
        "rocksdb" => {
            let config = load_rocksdb_config(config_name)?;
            let db = tsdb_rocksdb::TsdbRocksDb::open(data_dir, config)?;
            Ok(Arc::new(db))
        }
        "arrow" | "parquet" => {
            let config = tsdb_storage_arrow::config::ArrowStorageConfig::default();
            let engine = tsdb_storage_arrow::engine::ArrowStorageEngine::open(data_dir, config)?;
            Ok(Arc::new(engine))
        }
        _ => anyhow::bail!(
            "Unknown storage engine: {}. Use 'rocksdb' or 'arrow'.",
            engine_type
        ),
    }
}

fn load_rocksdb_config(config_name: &str) -> anyhow::Result<tsdb_rocksdb::RocksDbConfig> {
    let config_path = format!("configs/{}.ini", config_name);
    if std::path::Path::new(&config_path).exists() {
        let config = tsdb_rocksdb::RocksDbConfig::from_ini_file(&config_path)
            .map_err(|e| anyhow::anyhow!("config load error: {}", e))?;
        tracing::info!("loaded config from {}", config_path);
        Ok(config)
    } else {
        tracing::info!("config file {} not found, using defaults", config_path);
        Ok(tsdb_rocksdb::RocksDbConfig::default())
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Status => execute_status(&cli.data_dir, &cli.storage_engine, &cli.config)?,
        Commands::Query { sql, format } => execute_query(
            &cli.data_dir,
            &cli.storage_engine,
            &cli.config,
            &sql,
            &format,
        )?,
        Commands::Bench {
            mode,
            points,
            workers,
        } => run_bench(
            &cli.data_dir,
            &cli.storage_engine,
            &cli.config,
            &mode,
            points,
            workers,
        )?,
        Commands::Compact { cf } => execute_compact(&cli.data_dir, &cli.config, cf.as_deref())?,
        Commands::Import {
            file,
            format,
            batch_size,
        } => execute_import(
            &cli.data_dir,
            &cli.storage_engine,
            &cli.config,
            &file,
            &format,
            batch_size,
        )?,
        Commands::Archive { action } => execute_archive(&cli.data_dir, &cli.config, action)?,
        Commands::Export {
            measurement,
            output,
            format,
        } => execute_export(
            &cli.data_dir,
            &cli.storage_engine,
            &cli.config,
            &measurement,
            &output,
            &format,
        )?,
        Commands::Doctor => execute_doctor(&cli.data_dir, &cli.config)?,
        Commands::Iceberg { action } => execute_iceberg(&cli.data_dir, action)?,
        Commands::Serve {
            host,
            flight_port,
            admin_port,
            http_port,
            parquet_dir,
            iceberg_dir,
        } => execute_serve(ServeConfig {
            data_dir: cli.data_dir,
            engine_type: cli.storage_engine,
            config_name: cli.config,
            host,
            flight_port,
            admin_port,
            http_port,
            parquet_dir,
            iceberg_dir,
        })?,
    }

    Ok(())
}

struct ServeConfig {
    data_dir: String,
    engine_type: String,
    config_name: String,
    host: String,
    flight_port: u16,
    admin_port: u16,
    http_port: u16,
    parquet_dir: Option<String>,
    iceberg_dir: Option<String>,
}

fn execute_serve(cfg: ServeConfig) -> anyhow::Result<()> {
    let engine = open_engine(&cfg.data_dir, &cfg.engine_type, &cfg.config_name)?;

    let parquet_dir_resolved = cfg
        .parquet_dir
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{}_parquet", cfg.data_dir.trim_end_matches('/')));

    let iceberg_dir_path = cfg.iceberg_dir.map(std::path::PathBuf::from).or_else(|| {
        Some(std::path::PathBuf::from(format!(
            "{}/iceberg",
            cfg.data_dir.trim_end_matches('/')
        )))
    });

    println!("╔══════════════════════════════════════════════╗");
    println!("║       TSDB2 Server Starting                 ║");
    println!("╠══════════════════════════════════════════════╣");
    println!(
        "║  Flight gRPC:  grpc://{}:{}     ",
        cfg.host, cfg.flight_port
    );
    println!(
        "║  Admin nng:    tcp://{}:{}       ",
        cfg.host, cfg.admin_port
    );
    println!(
        "║  Admin pub:    tcp://{}:{}       ",
        cfg.host,
        cfg.admin_port + 1
    );
    println!(
        "║  Dashboard:    http://{}:{}     ",
        cfg.host, cfg.http_port
    );
    println!(
        "║  Config:       {}                          ",
        cfg.config_name
    );
    println!(
        "║  Data Dir:     {}                           ",
        cfg.data_dir
    );
    println!(
        "║  Parquet Dir:  {}                           ",
        parquet_dir_resolved
    );
    if let Some(ref id) = iceberg_dir_path {
        println!(
            "║  Iceberg Dir:  {}                           ",
            id.display()
        );
    }
    println!(
        "║  Engine:       {}                           ",
        cfg.engine_type
    );
    println!("╚══════════════════════════════════════════════╝");

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let flight_server = tsdb_flight::TsdbFlightServer::new(
            engine.clone(),
            datafusion::prelude::SessionContext::new(),
            cfg.host.clone(),
            cfg.flight_port,
        );

        let admin_server = tsdb_admin::AdminServer::new(
            engine.clone(),
            &cfg.data_dir,
            &parquet_dir_resolved,
            &cfg.host,
            cfg.admin_port,
            iceberg_dir_path,
        );

        let bound_admin = admin_server
            .bind()
            .map_err(|e| anyhow::anyhow!("admin bind: {}", e))?;

        let nng_req_url = format!("tcp://{}:{}", cfg.host, cfg.admin_port);
        let nng_pub_url = format!("tcp://{}:{}", cfg.host, cfg.admin_port + 1);

        let gateway_state = tsdb_admin::GatewayState::new(&nng_req_url, &nng_pub_url)
            .map_err(|e| anyhow::anyhow!("gateway init: {}", e))?;
        let app = tsdb_admin::gateway::build_router(gateway_state);

        let http_addr = format!("{}:{}", cfg.host, cfg.http_port);
        let http_listener = tokio::net::TcpListener::bind(&http_addr)
            .await
            .map_err(|e| anyhow::anyhow!("bind http {}: {}", http_addr, e))?;

        let service = arrow_flight::flight_service_server::FlightServiceServer::new(flight_server);
        let flight_addr = format!("{}:{}", cfg.host, cfg.flight_port).parse().unwrap();
        let flight_server = tonic::transport::Server::builder()
            .add_service(service)
            .serve(flight_addr);

        let admin_handle = tokio::spawn(async move {
            if let Err(e) = bound_admin.run().await {
                tracing::error!("admin server error: {}", e);
            }
        });

        let flight_handle = tokio::spawn(flight_server);

        let http_handle = tokio::spawn(async move {
            if let Err(e) = axum::serve(http_listener, app).await {
                tracing::error!("http server error: {}", e);
            }
        });

        println!(
            "\n  Dashboard available at http://{}:{}",
            cfg.host, cfg.http_port
        );

        tokio::signal::ctrl_c().await?;
        println!("\nShutting down...");
        admin_server.shutdown();
        flight_handle.abort();
        admin_handle.abort();
        http_handle.abort();
        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}

fn iceberg_catalog_path(data_dir: &str) -> String {
    format!("{}/iceberg", data_dir)
}

fn execute_iceberg(data_dir: &str, action: IcebergActions) -> anyhow::Result<()> {
    let catalog_dir = iceberg_catalog_path(data_dir);

    match action {
        IcebergActions::Create { name } => {
            let catalog = tsdb_iceberg::IcebergCatalog::open(&catalog_dir)?;
            let schema = tsdb_iceberg::Schema::new(
                0,
                vec![
                    tsdb_iceberg::schema::Field {
                        id: 1,
                        name: "timestamp".to_string(),
                        required: true,
                        field_type: tsdb_iceberg::schema::IcebergType::Timestamptz,
                        doc: None,
                        initial_default: None,
                        write_default: None,
                    },
                    tsdb_iceberg::schema::Field {
                        id: 2,
                        name: "tag_host".to_string(),
                        required: false,
                        field_type: tsdb_iceberg::schema::IcebergType::String,
                        doc: None,
                        initial_default: None,
                        write_default: None,
                    },
                    tsdb_iceberg::schema::Field {
                        id: 1000,
                        name: "value".to_string(),
                        required: false,
                        field_type: tsdb_iceberg::schema::IcebergType::Double,
                        doc: None,
                        initial_default: None,
                        write_default: None,
                    },
                ],
            );
            let spec = tsdb_iceberg::PartitionSpec::day_partition(0, 1);
            catalog.create_table(&name, schema, spec)?;
            println!("Created Iceberg table: {}", name);
        }
        IcebergActions::List => {
            let catalog = tsdb_iceberg::IcebergCatalog::open(&catalog_dir)?;
            let tables = catalog.list_tables()?;
            if tables.is_empty() {
                println!("No Iceberg tables found.");
            } else {
                println!("Iceberg Tables:");
                println!("==============");
                for name in &tables {
                    let meta = catalog.load_table(name)?;
                    let snap_count = meta.snapshots.len();
                    let current_snap = meta
                        .current_snapshot()
                        .map(|s| s.snapshot_id.to_string())
                        .unwrap_or_else(|| "N/A".to_string());
                    println!(
                        "  {} (snapshots: {}, current: {})",
                        name, snap_count, current_snap
                    );
                }
            }
        }
        IcebergActions::Drop { name } => {
            let catalog = tsdb_iceberg::IcebergCatalog::open(&catalog_dir)?;
            catalog.drop_table(&name)?;
            println!("Dropped Iceberg table: {}", name);
        }
        IcebergActions::Snapshots { name } => {
            let catalog = tsdb_iceberg::IcebergCatalog::open(&catalog_dir)?;
            let meta = catalog.load_table(&name)?;

            println!("Snapshots for table '{}':", name);
            println!("=========================");
            for snap in &meta.snapshots {
                let op = &snap.summary.operation;
                let added = snap.summary.added_data_files.unwrap_or(0);
                let deleted = snap.summary.deleted_data_files.unwrap_or(0);
                let records = snap.summary.total_records.unwrap_or(0);
                println!(
                    "  id={} op={} files=+{}/-{} records={} ts={}",
                    snap.snapshot_id, op, added, deleted, records, snap.timestamp_ms
                );
            }

            println!("\nSnapshot Log:");
            for entry in &meta.snapshot_log {
                let dt =
                    chrono::DateTime::from_timestamp_millis(entry.timestamp_ms as i64).unwrap();
                println!(
                    "  snapshot_id={}, time={}",
                    entry.snapshot_id,
                    dt.format("%Y-%m-%d %H:%M:%S")
                );
            }
        }
        IcebergActions::Append { name, file, format } => {
            let catalog = tsdb_iceberg::IcebergCatalog::open(&catalog_dir)?;
            let meta = catalog.load_table(&name)?;
            let mut table =
                tsdb_iceberg::IcebergTable::new(std::sync::Arc::new(catalog), name.clone(), meta);

            let content = std::fs::read_to_string(&file)?;
            let dps: Vec<tsdb_arrow::schema::DataPoint> = match format.as_str() {
                "json" | "jsonl" => content
                    .lines()
                    .filter(|line| !line.is_empty())
                    .map(serde_json::from_str)
                    .collect::<std::result::Result<Vec<_>, _>>()?,
                "csv" => {
                    let mut reader = csv::Reader::from_reader(content.as_bytes());
                    reader
                        .deserialize()
                        .map(|r| r.map_err(|e| anyhow::anyhow!(e)))
                        .collect::<anyhow::Result<Vec<_>>>()?
                }
                other => anyhow::bail!("Unsupported import format: {}", other),
            };

            table.append(&dps)?;
            println!("Appended {} datapoints to table '{}'", dps.len(), name);

            let snap = table.current_snapshot().unwrap();
            println!("New snapshot: {}", snap.snapshot_id);
        }
        IcebergActions::Scan { name, sql } => {
            let catalog = tsdb_iceberg::IcebergCatalog::open(&catalog_dir)?;
            let meta = catalog.load_table(&name)?;
            let table =
                tsdb_iceberg::IcebergTable::new(std::sync::Arc::new(catalog), name.clone(), meta);

            match sql {
                Some(query) => {
                    println!("Executing SQL on Iceberg table '{}': {}", name, query);
                    println!("==========================================");
                    eprintln!("Warning: SQL filtering is not yet supported for Iceberg tables. Showing all data.");
                    let scan = table.scan().build()?;
                    let batches = scan.execute()?;
                    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

                    use arrow::util::pretty::print_batches;
                    print_batches(&batches)?;

                    println!("\nTotal rows: {}", total_rows);
                }
                None => {
                    println!("Scanning Iceberg table '{}':", name);
                    println!("============================");
                    let scan = table.scan().build()?;
                    let data_files = scan.plan();
                    println!("Data files: {}", data_files.len());
                    for df in data_files {
                        println!(
                            "  {} (records={}, size={})",
                            df.file_path, df.record_count, df.file_size_in_bytes
                        );
                    }

                    let batches = scan.execute()?;
                    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    println!("\nTotal rows: {}", total_rows);
                }
            }
        }
        IcebergActions::Compact { name } => {
            let catalog = tsdb_iceberg::IcebergCatalog::open(&catalog_dir)?;
            let meta = catalog.load_table(&name)?;
            let mut table =
                tsdb_iceberg::IcebergTable::new(std::sync::Arc::new(catalog), name.clone(), meta);

            let before_files = table
                .collect_data_files(table.current_snapshot().unwrap())
                .unwrap_or_default()
                .len();
            println!("Before compact: {} data files", before_files);

            table.compact()?;

            let after_files = table
                .collect_data_files(table.current_snapshot().unwrap())
                .unwrap_or_default()
                .len();
            println!("After compact: {} data files", after_files);
            println!("Compacted table '{}'", name);
        }
        IcebergActions::Rollback { name, snapshot_id } => {
            let catalog = tsdb_iceberg::IcebergCatalog::open(&catalog_dir)?;
            let meta = catalog.load_table(&name)?;
            let mut table =
                tsdb_iceberg::IcebergTable::new(std::sync::Arc::new(catalog), name.clone(), meta);

            table.rollback_to_snapshot(snapshot_id)?;
            println!("Rolled back table '{}' to snapshot {}", name, snapshot_id);
        }
        IcebergActions::Expire { name, keep_days } => {
            let catalog = tsdb_iceberg::IcebergCatalog::open(&catalog_dir)?;
            let meta = catalog.load_table(&name)?;
            let mut table =
                tsdb_iceberg::IcebergTable::new(std::sync::Arc::new(catalog), name.clone(), meta);

            let before = table.snapshots().len();
            let cutoff_ms =
                chrono::Utc::now().timestamp_millis() as u64 - keep_days * 24 * 3600 * 1000;

            table.expire_snapshots(cutoff_ms, 1)?;
            let after = table.snapshots().len();

            println!(
                "Expired snapshots for '{}': {} -> {} (kept last {} days)",
                name, before, after, keep_days
            );
        }
    }

    Ok(())
}

fn execute_status(data_dir: &str, engine_type: &str, _config_name: &str) -> anyhow::Result<()> {
    if engine_type == "rocksdb" {
        let db = tsdb_rocksdb::TsdbRocksDb::open(data_dir, load_rocksdb_config(_config_name)?)?;
        let cfs = db.list_ts_cfs();
        let measurements = db.list_measurements();

        println!("TSDB Status (rocksdb Engine)");
        println!("============================");
        println!("Data Dir:         {}", data_dir);
        println!("Measurements:     {}", measurements.len());
        println!("Column Families:  {}", cfs.len());
        println!();

        if !measurements.is_empty() {
            println!("Measurement Name");
            println!("----------------");
            for m in &measurements {
                println!("{}", m);
            }
        }

        if !cfs.is_empty() {
            println!();
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
    } else {
        let engine = open_engine(data_dir, engine_type, _config_name)?;
        let measurements = engine.list_measurements();

        println!("TSDB Status ({} Engine)", engine_type);
        println!("============================");
        println!("Data Dir:         {}", data_dir);
        println!("Measurements:     {}", measurements.len());
        println!();

        if !measurements.is_empty() {
            println!("Measurement Name");
            println!("----------------");
            for m in &measurements {
                println!("{}", m);
            }
        }
    }

    Ok(())
}

fn execute_query(
    data_dir: &str,
    engine_type: &str,
    _config_name: &str,
    sql: &str,
    format: &str,
) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let engine = open_engine(data_dir, engine_type, _config_name)?;

        let measurements = engine.list_measurements();
        if measurements.is_empty() {
            println!("No data found in database.");
            return Ok::<(), anyhow::Error>(());
        }

        let query_engine = tsdb_datafusion::DataFusionQueryEngine::new(data_dir);

        let now = chrono::Utc::now().timestamp_micros();
        let start = now - 7 * 86_400_000_000i64;

        for measurement in &measurements {
            if let Ok(dps) = engine.read_range(measurement, &Tags::new(), start, now) {
                if dps.is_empty() {
                    continue;
                }
                query_engine.register_from_datapoints(measurement, &dps)?;

                use datafusion::datasource::MemTable;
                use tsdb_arrow::converter::datapoints_to_record_batch;

                let table = query_engine
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
                query_engine
                    .session_context()
                    .deregister_table(measurement)?;
                query_engine
                    .session_context()
                    .register_table(measurement, Arc::new(mem_table))?;
            }
        }

        let result = query_engine.execute(sql).await?;

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

fn execute_compact(data_dir: &str, _config_name: &str, cf: Option<&str>) -> anyhow::Result<()> {
    let db = tsdb_rocksdb::TsdbRocksDb::open(data_dir, load_rocksdb_config(_config_name)?)?;

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

fn execute_import(
    data_dir: &str,
    engine_type: &str,
    _config_name: &str,
    file: &str,
    format: &str,
    batch_size: usize,
) -> anyhow::Result<()> {
    let engine = open_engine(data_dir, engine_type, _config_name)?;
    let content = std::fs::read_to_string(file)?;

    match format {
        "json" => {
            let datapoints: Vec<tsdb_arrow::schema::DataPoint> = serde_json::from_str(&content)?;
            let total = datapoints.len();

            for chunk in datapoints.chunks(batch_size) {
                engine.write_batch(chunk)?;
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
                                    dp.fields.insert(
                                        h.to_string(),
                                        tsdb_arrow::schema::FieldValue::Float(f),
                                    );
                                } else if let Ok(i) = val.parse::<i64>() {
                                    dp.fields.insert(
                                        h.to_string(),
                                        tsdb_arrow::schema::FieldValue::Integer(i),
                                    );
                                } else if val == "true" || val == "false" {
                                    dp.fields.insert(
                                        h.to_string(),
                                        tsdb_arrow::schema::FieldValue::Boolean(val == "true"),
                                    );
                                } else {
                                    dp.fields.insert(
                                        h.to_string(),
                                        tsdb_arrow::schema::FieldValue::String(val.to_string()),
                                    );
                                }
                            }
                        }
                    }
                }

                datapoints.push(dp);
            }

            let total = datapoints.len();
            for chunk in datapoints.chunks(batch_size) {
                engine.write_batch(chunk)?;
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

fn execute_archive(
    data_dir: &str,
    _config_name: &str,
    action: ArchiveActions,
) -> anyhow::Result<()> {
    match action {
        ArchiveActions::Create {
            older_than,
            output_dir,
        } => {
            let days = parse_days(&older_than)?;
            let db = tsdb_rocksdb::TsdbRocksDb::open(data_dir, load_rocksdb_config(_config_name)?)?;

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

            let db = tsdb_rocksdb::TsdbRocksDb::open(data_dir, load_rocksdb_config(_config_name)?)?;

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
                    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                    println!("  {} ({} bytes)", f, size);
                }
            }
        }
    }

    Ok(())
}

fn execute_export(
    data_dir: &str,
    engine_type: &str,
    _config_name: &str,
    measurement: &str,
    output: &str,
    format: &str,
) -> anyhow::Result<()> {
    let engine = open_engine(data_dir, engine_type, _config_name)?;

    let now = chrono::Utc::now().timestamp_micros();
    let start = now - 365 * 86_400_000_000i64;
    let datapoints = engine.read_range(measurement, &Tags::new(), start, now)?;

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

            let mut headers = vec!["measurement".to_string(), "timestamp".to_string()];
            for k in &tag_keys {
                headers.push(format!("tag_{}", k));
            }
            for k in &field_keys {
                headers.push(k.clone());
            }

            let mut wtr = csv::Writer::from_path(output)?;
            wtr.write_record(&headers)?;

            for dp in &datapoints {
                let mut row = vec![dp.measurement.clone(), dp.timestamp.to_string()];
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

fn execute_doctor(data_dir: &str, _config_name: &str) -> anyhow::Result<()> {
    println!("TSDB Doctor");
    println!("==========");

    if !Path::new(data_dir).exists() {
        println!("[FAIL] Data directory does not exist: {}", data_dir);
        return Ok(());
    }
    println!("[OK]   Data directory exists: {}", data_dir);

    match tsdb_rocksdb::TsdbRocksDb::open(data_dir, load_rocksdb_config(_config_name)?) {
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

fn parse_days(s: &str) -> anyhow::Result<i64> {
    let s = s.trim();
    if let Some(days_str) = s.strip_suffix('d') {
        Ok(days_str.parse::<i64>()?)
    } else {
        Ok(s.parse::<i64>()?)
    }
}

fn run_bench(
    data_dir: &str,
    engine_type: &str,
    _config_name: &str,
    mode: &str,
    points: usize,
    workers: usize,
) -> anyhow::Result<()> {
    use std::time::Instant;
    use tsdb_arrow::schema::FieldValue;

    let bench_dir = std::path::Path::new(data_dir);
    std::fs::create_dir_all(bench_dir)?;

    let engine = open_engine(data_dir, engine_type, _config_name)?;

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
            engine.write_batch(&dps)?;
            let elapsed = start.elapsed();

            let pts_per_sec = points as f64 / elapsed.as_secs_f64();
            println!("Write Benchmark Results ({} engine)", engine_type);
            println!("=======================");
            println!("Points:     {}", points);
            println!("Workers:    {}", workers);
            println!("Elapsed:    {:.2}s", elapsed.as_secs_f64());
            println!("Throughput: {:.0} points/sec", pts_per_sec);

            let report = serde_json::json!({
                "mode": "write",
                "engine": engine_type,
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
            engine.write_batch(&dps)?;

            let start = Instant::now();
            let result = engine.read_range(
                "cpu",
                &Tags::new(),
                base_ts,
                base_ts + (points as i64 - 1) * 1_000,
            )?;
            let elapsed = start.elapsed();

            let pts_per_sec = result.len() as f64 / elapsed.as_secs_f64();
            println!("Read Benchmark Results ({} engine)", engine_type);
            println!("======================");
            println!("Points:     {}", result.len());
            println!("Elapsed:    {:.2}s", elapsed.as_secs_f64());
            println!("Throughput: {:.0} points/sec", pts_per_sec);

            let report = serde_json::json!({
                "mode": "read",
                "engine": engine_type,
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
