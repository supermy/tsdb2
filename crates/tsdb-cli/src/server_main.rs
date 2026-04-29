use clap::Parser;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::writer::MakeWriterExt;

#[derive(Parser)]
#[command(name = "tsdb-server", version, about = "TSDB2 时序数据库服务端")]
struct Cli {
    #[arg(long, default_value = "./data")]
    data_dir: String,

    #[arg(long, default_value = "./data_parquet")]
    parquet_dir: String,

    #[arg(long, default_value = "rocksdb")]
    storage_engine: String,

    #[arg(long, default_value = "balanced")]
    config: String,

    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    #[arg(long, default_value_t = 50051)]
    flight_port: u16,

    #[arg(long, default_value_t = 8080)]
    admin_port: u16,

    #[arg(long, default_value_t = 3000)]
    http_port: u16,

    #[arg(long)]
    iceberg_dir: Option<String>,

    #[arg(long, default_value_t = 30)]
    retention_days: u64,

    #[arg(short = 'b', long = "background", help = "以后台守护进程模式运行")]
    background: bool,

    #[arg(long, help = "PID 文件路径 (默认: /tmp/tsdb-server.pid)")]
    pid_file: Option<String>,

    #[arg(long, help = "日志文件目录 (默认: ./logs)")]
    log_dir: Option<String>,

    #[arg(long, default_value = "info", help = "日志级别: trace|debug|info|warn|error")]
    log_level: String,

    #[arg(long, default_value_t = 10, help = "保留的日志文件轮转数量")]
    log_max_files: usize,
}

struct LogGuards {
    _file_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if cli.background {
        daemonize(&cli)?;
    } else {
        let _guards = init_logging(&cli);
        run_server(&cli)?;
    }

    Ok(())
}

fn init_logging(cli: &Cli) -> LogGuards {
    let log_dir = cli.log_dir.as_deref().unwrap_or("logs");
    let log_dir_path = std::path::Path::new(log_dir);
    if let Err(e) = std::fs::create_dir_all(log_dir_path) {
        eprintln!("WARNING: cannot create log dir {}: {}", log_dir_path.display(), e);
    }

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&cli.log_level));

    let file_appender = tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("tsdb-server")
        .filename_suffix("log")
        .max_log_files(cli.log_max_files)
        .build(log_dir_path)
        .unwrap_or_else(|e| {
            eprintln!("WARNING: cannot create RollingFileAppender: {}, falling back to daily", e);
            tracing_appender::rolling::daily(log_dir_path, "tsdb-server.log")
        });

    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    if cli.background {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_ansi(false)
            .with_target(true)
            .with_thread_ids(true)
            .with_writer(non_blocking)
            .init();
    } else {
        let combined = std::io::stderr.and(non_blocking);
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_ansi(false)
            .with_target(true)
            .with_thread_ids(false)
            .with_writer(combined)
            .init();

        tracing::info!("日志文件输出: {}/tsdb-server.*.log", log_dir_path.display());
    }

    tracing::info!("日志初始化完成: level={}, max_files={}", cli.log_level, cli.log_max_files);

    LogGuards {
        _file_guard: Some(guard),
    }
}

fn daemonize(cli: &Cli) -> anyhow::Result<()> {
    let pid_file = cli
        .pid_file
        .as_deref()
        .unwrap_or("/tmp/tsdb-server.pid")
        .to_string();

    if std::path::Path::new(&pid_file).exists() {
        let existing_pid = std::fs::read_to_string(&pid_file)
            .ok()
            .and_then(|s| s.trim().parse::<i32>().ok());
        if let Some(pid) = existing_pid {
            if is_process_running(pid) {
                anyhow::bail!("tsdb-server is already running (PID: {})", pid);
            }
        }
        let _ = std::fs::remove_file(&pid_file);
    }

    let log_dir = cli.log_dir.as_deref().unwrap_or("logs");

    println!("Starting tsdb-server in background...");
    println!("  PID file: {}", pid_file);
    println!("  Log dir:  {}", log_dir);
    println!("  Data dir: {}", cli.data_dir);
    println!("  Engine:   {}", cli.storage_engine);
    println!("  Log level: {}", cli.log_level);

    match unsafe { libc::fork() } {
        -1 => anyhow::bail!("fork failed"),
        0 => {
            if unsafe { libc::setsid() } == -1 {
                std::process::exit(1);
            }

            unsafe { libc::close(0); }

            let _guards = init_logging(cli);

            if let Ok(f) = std::fs::File::create("/dev/null") {
                use std::os::unix::io::AsRawFd;
                unsafe {
                    libc::dup2(f.as_raw_fd(), 1);
                    libc::dup2(f.as_raw_fd(), 2);
                }
            }

            std::fs::write(&pid_file, std::process::id().to_string())
                .unwrap_or_else(|e| tracing::error!("write pid file: {}", e));

            tracing::info!("tsdb-server daemon started (PID: {})", std::process::id());

            if let Err(e) = run_server(cli) {
                tracing::error!("server error: {}", e);
                std::process::exit(1);
            }

            tracing::info!("tsdb-server daemon stopped");
            let _ = std::fs::remove_file(&pid_file);
        }
        pid => {
            std::fs::write(&pid_file, pid.to_string())?;
            println!("  PID:      {}", pid);
            println!("Server started in background mode.");
        }
    }

    Ok(())
}

fn is_process_running(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

fn run_server(cli: &Cli) -> anyhow::Result<()> {
    let engine = open_engine(&cli.data_dir, &cli.storage_engine, &cli.config)?;

    let iceberg_dir_path = cli
        .iceberg_dir
        .as_deref()
        .map(PathBuf::from)
        .or_else(|| Some(PathBuf::from(format!("{}/iceberg", cli.data_dir.trim_end_matches('/')))));

    if !cli.background {
        println!("╔══════════════════════════════════════════════╗");
        println!("║       TSDB2 Server Starting                 ║");
        println!("╠══════════════════════════════════════════════╣");
        println!("║  Flight gRPC:  grpc://{}:{}     ", cli.host, cli.flight_port);
        println!("║  Admin nng:    tcp://{}:{}       ", cli.host, cli.admin_port);
        println!("║  Admin pub:    tcp://{}:{}       ", cli.host, cli.admin_port + 1);
        println!("║  Dashboard:    http://{}:{}     ", cli.host, cli.http_port);
        println!("║  Config:       {}                          ", cli.config);
        println!("║  Data Dir:     {}                           ", cli.data_dir);
        println!("║  Parquet Dir:  {}                           ", cli.parquet_dir);
        if let Some(ref id) = iceberg_dir_path {
            println!("║  Iceberg Dir:  {}                           ", id.display());
        }
        println!("║  Engine:       {}                           ", cli.storage_engine);
        println!("║  Retention:    {} days                      ", cli.retention_days);
        println!("╚══════════════════════════════════════════════╝");
    }

    tracing::info!(
        "tsdb-server starting: engine={}, data={}, parquet={}, flight={}:{}, admin={}:{}, http={}:{}",
        cli.storage_engine, cli.data_dir, cli.parquet_dir,
        cli.host, cli.flight_port, cli.host, cli.admin_port, cli.host, cli.http_port
    );

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let flight_server = tsdb_flight::TsdbFlightServer::new(
            engine.clone(),
            datafusion::prelude::SessionContext::new(),
            cli.host.clone(),
            cli.flight_port,
        );

        let admin_server = tsdb_admin::AdminServer::new(
            engine.clone(),
            &cli.data_dir,
            &cli.parquet_dir,
            &cli.host,
            cli.admin_port,
            iceberg_dir_path,
        );

        let bound_admin = admin_server
            .bind()
            .map_err(|e| anyhow::anyhow!("admin bind: {}", e))?;

        let nng_req_url = format!("tcp://{}:{}", cli.host, cli.admin_port);
        let nng_pub_url = format!("tcp://{}:{}", cli.host, cli.admin_port + 1);

        let gateway_state = tsdb_admin::GatewayState::new(&nng_req_url, &nng_pub_url)
            .map_err(|e| anyhow::anyhow!("gateway init: {}", e))?;
        let app = tsdb_admin::gateway::build_router(gateway_state);

        let http_addr = format!("{}:{}", cli.host, cli.http_port);
        let http_listener = tokio::net::TcpListener::bind(&http_addr)
            .await
            .map_err(|e| anyhow::anyhow!("bind http {}: {}", http_addr, e))?;

        let service = arrow_flight::flight_service_server::FlightServiceServer::new(flight_server);
        let flight_addr = format!("{}:{}", cli.host, cli.flight_port)
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid flight address {}:{}: {}", cli.host, cli.flight_port, e))?;
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

        if !cli.background {
            println!("\n  Dashboard available at http://{}:{}", cli.host, cli.http_port);
        }
        tracing::info!("tsdb-server ready: http://{}:{}", cli.host, cli.http_port);

        tokio::signal::ctrl_c().await?;
        tracing::info!("Shutting down gracefully...");
        if !cli.background {
            println!("\nShutting down gracefully...");
        }
        admin_server.shutdown();

        let shutdown_timeout = tokio::time::Duration::from_secs(5);

        tokio::select! {
            _ = flight_handle => {}
            _ = tokio::time::sleep(shutdown_timeout) => {
                tracing::warn!("flight server did not shut down in time, aborting");
            }
        }

        tokio::select! {
            _ = admin_handle => {}
            _ = tokio::time::sleep(shutdown_timeout) => {
                tracing::warn!("admin server did not shut down in time, aborting");
            }
        }

        tokio::select! {
            _ = http_handle => {}
            _ = tokio::time::sleep(shutdown_timeout) => {
                tracing::warn!("http server did not shut down in time, aborting");
            }
        }

        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}

fn open_engine(
    data_dir: &str,
    engine_type: &str,
    config_name: &str,
) -> anyhow::Result<std::sync::Arc<dyn tsdb_arrow::engine::StorageEngine>> {
    match engine_type {
        "rocksdb" => {
            let config = load_rocksdb_config(config_name)?;
            let db = tsdb_rocksdb::TsdbRocksDb::open(data_dir, config)?;
            Ok(std::sync::Arc::new(db))
        }
        "arrow" | "parquet" => {
            let config = tsdb_storage_arrow::config::ArrowStorageConfig::default();
            let engine = tsdb_storage_arrow::engine::ArrowStorageEngine::open(data_dir, config)?;
            Ok(std::sync::Arc::new(engine))
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
