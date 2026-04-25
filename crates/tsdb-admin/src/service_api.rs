use crate::error::{AdminError, Result};
use crate::protocol::*;
use std::collections::HashMap;
use std::io::{BufRead, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

struct ManagedProcess {
    child: std::process::Child,
    log_path: PathBuf,
    started_at: i64,
}

pub struct ServiceManager {
    services: Arc<RwLock<HashMap<String, ServiceInfo>>>,
    processes: Arc<RwLock<HashMap<String, ManagedProcess>>>,
    config_dir: PathBuf,
}

impl ServiceManager {
    pub fn new(config_dir: impl AsRef<Path>) -> Self {
        Self {
            services: Arc::new(RwLock::new(HashMap::new())),
            processes: Arc::new(RwLock::new(HashMap::new())),
            config_dir: config_dir.as_ref().to_path_buf(),
        }
    }

    pub async fn ensure_default_service(
        &self,
        name: &str,
        port: u16,
        config: &str,
        data_dir: &str,
        engine: &str,
    ) {
        let services = self.services.read().await;
        if services.contains_key(name) {
            return;
        }
        drop(services);
        let mut services = self.services.write().await;
        let info = ServiceInfo {
            name: name.to_string(),
            status: ServiceStatus::Running,
            port,
            config: config.to_string(),
            data_dir: data_dir.to_string(),
            engine: engine.to_string(),
            enable_iceberg: false,
            pid: Some(std::process::id()),
            uptime_secs: Some(0),
        };
        services.insert(name.to_string(), info);
    }

    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    pub async fn list_services(&self) -> Vec<ServiceInfo> {
        let mut services = self.services.write().await;
        self.refresh_statuses(&mut services).await;
        services.values().cloned().collect()
    }

    pub async fn create_service(
        &self,
        name: &str,
        port: u16,
        config: &str,
        data_dir: &str,
        engine: &str,
        enable_iceberg: bool,
    ) -> Result<ServiceInfo> {
        let mut services = self.services.write().await;
        if services.contains_key(name) {
            return Err(AdminError::ServiceAlreadyRunning(name.to_string()));
        }

        let service_config_dir = self.config_dir.join("services").join(name);
        std::fs::create_dir_all(&service_config_dir)
            .map_err(|e| AdminError::Config(format!("create service dir: {}", e)))?;

        let info = ServiceInfo {
            name: name.to_string(),
            status: ServiceStatus::Stopped,
            port,
            config: config.to_string(),
            data_dir: data_dir.to_string(),
            engine: engine.to_string(),
            enable_iceberg,
            pid: None,
            uptime_secs: None,
        };
        services.insert(name.to_string(), info.clone());
        Ok(info)
    }

    pub async fn start_service(&self, name: &str) -> Result<ServiceInfo> {
        let mut services = self.services.write().await;
        let info = services
            .get_mut(name)
            .ok_or_else(|| AdminError::ServiceNotFound(name.to_string()))?;

        if info.status == ServiceStatus::Running {
            return Err(AdminError::ServiceAlreadyRunning(name.to_string()));
        }

        let bin_path = Self::find_tsdb_cli_binary()?;

        let log_dir = self.config_dir.join("logs");
        std::fs::create_dir_all(&log_dir)
            .map_err(|e| AdminError::Config(format!("create log dir: {}", e)))?;
        let log_path = log_dir.join(format!("{}.log", name));
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| AdminError::Config(format!("open log file: {}", e)))?;

        let mut cmd = std::process::Command::new(&bin_path);
        cmd.arg("serve")
            .arg("--data-dir")
            .arg(&info.data_dir)
            .arg("--storage-engine")
            .arg(&info.engine)
            .arg("--config")
            .arg(&info.config)
            .arg("--host")
            .arg("0.0.0.0")
            .arg("--flight-port")
            .arg(info.port.to_string())
            .arg("--admin-port")
            .arg((info.port + 100).to_string())
            .stdout(log_file.try_clone().map_err(|e| AdminError::Config(format!("clone stdout: {}", e)))?)
            .stderr(log_file);

        let child = cmd
            .spawn()
            .map_err(|e| AdminError::Config(format!("spawn process: {}", e)))?;

        let pid = child.id();
        let now = chrono::Utc::now().timestamp_micros();

        {
            let mut processes = self.processes.write().await;
            processes.insert(
                name.to_string(),
                ManagedProcess {
                    child,
                    log_path: log_path.clone(),
                    started_at: now,
                },
            );
        }

        info.status = ServiceStatus::Running;
        info.pid = Some(pid);
        info.uptime_secs = Some(0);

        Ok(info.clone())
    }

    pub async fn stop_service(&self, name: &str) -> Result<ServiceInfo> {
        let mut services = self.services.write().await;
        let info = services
            .get_mut(name)
            .ok_or_else(|| AdminError::ServiceNotFound(name.to_string()))?;

        if info.status != ServiceStatus::Running {
            return Err(AdminError::ServiceNotRunning(name.to_string()));
        }

        {
            let mut processes = self.processes.write().await;
            if let Some(mut proc) = processes.remove(name) {
                Self::kill_child(&mut proc.child).await;
            }
        }

        info.status = ServiceStatus::Stopped;
        info.pid = None;
        info.uptime_secs = None;

        Ok(info.clone())
    }

    pub async fn restart_service(&self, name: &str) -> Result<ServiceInfo> {
        self.stop_service(name).await.ok();
        self.start_service(name).await
    }

    pub async fn get_service(&self, name: &str) -> Result<ServiceInfo> {
        let mut services = self.services.write().await;
        self.refresh_statuses(&mut services).await;
        services
            .get(name)
            .cloned()
            .ok_or_else(|| AdminError::ServiceNotFound(name.to_string()))
    }

    pub async fn delete_service(&self, name: &str) -> Result<()> {
        let mut services = self.services.write().await;
        let info = services
            .get(name)
            .ok_or_else(|| AdminError::ServiceNotFound(name.to_string()))?;
        if info.status == ServiceStatus::Running {
            return Err(AdminError::ServiceAlreadyRunning(name.to_string()));
        }
        services.remove(name);
        Ok(())
    }

    pub async fn apply_config(&self, service_name: &str, profile: &str) -> Result<ServiceInfo> {
        let mut services = self.services.write().await;
        let info = services
            .get_mut(service_name)
            .ok_or_else(|| AdminError::ServiceNotFound(service_name.to_string()))?;

        let was_running = info.status == ServiceStatus::Running;
        info.config = profile.to_string();

        let updated = info.clone();
        drop(services);

        if was_running {
            self.restart_service(service_name).await
        } else {
            Ok(updated)
        }
    }

    pub async fn get_service_logs(&self, name: &str, lines: usize) -> Result<Vec<String>> {
        let processes = self.processes.read().await;
        let log_path = if let Some(proc) = processes.get(name) {
            proc.log_path.clone()
        } else {
            let log_dir = self.config_dir.join("logs");
            log_dir.join(format!("{}.log", name))
        };
        drop(processes);

        if !log_path.exists() {
            return Ok(vec![]);
        }

        let file = std::fs::File::open(&log_path)
            .map_err(|e| AdminError::Config(format!("open log: {}", e)))?;
        let mut reader = std::io::BufReader::new(file);
        let file_size = reader
            .get_ref()
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0);

        if file_size > 1024 * 1024 {
            let seek_pos = file_size - 1024 * 1024;
            reader
                .seek(SeekFrom::Start(seek_pos))
                .map_err(|e| AdminError::Config(format!("seek log: {}", e)))?;
        }

        let all_lines: Vec<String> = reader.lines().map_while(std::result::Result::ok).collect();
        let start = if all_lines.len() > lines {
            all_lines.len() - lines
        } else {
            0
        };
        Ok(all_lines[start..].to_vec())
    }

    pub async fn update_uptime(&self, name: &str) {
        let mut services = self.services.write().await;
        if let Some(info) = services.get_mut(name) {
            if info.status == ServiceStatus::Running {
                let processes = self.processes.read().await;
                if let Some(proc) = processes.get(name) {
                    let now = chrono::Utc::now().timestamp_micros();
                    let elapsed_secs = (now - proc.started_at) / 1_000_000;
                    info.uptime_secs = Some(elapsed_secs as u64);
                }
            }
        }
    }

    async fn refresh_statuses(&self, services: &mut HashMap<String, ServiceInfo>) {
        let mut processes = self.processes.write().await;
        let mut dead_names = Vec::new();

        for (name, proc) in processes.iter_mut() {
            match proc.child.try_wait() {
                Ok(Some(status)) => {
                    if let Some(info) = services.get_mut(name) {
                        info.status = ServiceStatus::Error;
                        info.pid = None;
                        info.uptime_secs = None;
                    }
                    dead_names.push(name.clone());
                    let _ = status;
                }
                Ok(None) => {}
                Err(_) => {
                    if let Some(info) = services.get_mut(name) {
                        info.status = ServiceStatus::Error;
                    }
                    dead_names.push(name.clone());
                }
            }
        }

        for name in dead_names {
            processes.remove(&name);
        }
    }

    async fn kill_child(child: &mut std::process::Child) {
        unsafe {
            libc::kill(child.id() as i32, libc::SIGTERM);
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        match child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                unsafe {
                    libc::kill(child.id() as i32, libc::SIGKILL);
                }
                let _ = child.wait();
            }
            Err(_) => {
                unsafe {
                    libc::kill(child.id() as i32, libc::SIGKILL);
                }
            }
        }
    }

    fn find_tsdb_cli_binary() -> Result<PathBuf> {
        let exe_path = std::env::current_exe()
            .map_err(|e| AdminError::Config(format!("get current exe: {}", e)))?;
        let exe_dir = exe_path
            .parent()
            .ok_or_else(|| AdminError::Config("no parent dir for exe".to_string()))?;

        let tsdb_cli = exe_dir.join("tsdb-cli");
        if tsdb_cli.exists() {
            return Ok(tsdb_cli);
        }

        if let Ok(path) = which::which("tsdb-cli") {
            return Ok(path);
        }

        Err(AdminError::Config(
            "tsdb-cli binary not found in PATH or alongside current executable".to_string(),
        ))
    }
}
