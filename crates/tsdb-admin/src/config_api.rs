use crate::error::{AdminError, Result};
use crate::protocol::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub struct ConfigManager {
    config_dir: PathBuf,
    profiles: HashMap<String, ConfigProfile>,
}

impl ConfigManager {
    pub fn new(config_dir: impl AsRef<Path>) -> Self {
        let dir = config_dir.as_ref().to_path_buf();
        let mut mgr = Self {
            config_dir: dir.clone(),
            profiles: HashMap::new(),
        };
        mgr.load_profiles_from_dir(&dir.join("configs")).ok();
        let exe_dir = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.to_path_buf()));
        if let Some(ref ed) = exe_dir {
            let project_configs = ed.join("configs");
            if project_configs.exists() && project_configs != dir.join("configs") {
                mgr.load_profiles_from_dir(&project_configs).ok();
            }
        }
        let cwd_configs = std::path::PathBuf::from("configs");
        if cwd_configs.exists() && cwd_configs != dir.join("configs") {
            mgr.load_profiles_from_dir(&cwd_configs).ok();
        }
        mgr
    }

    fn load_profiles_from_dir(&mut self, profiles_dir: &Path) -> Result<()> {
        if !profiles_dir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(profiles_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "ini" {
                    if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                        if self.profiles.contains_key(name) {
                            continue;
                        }
                        let content = std::fs::read_to_string(&path)?;
                        let description = Self::extract_description(&content, name);
                        self.profiles.insert(
                            name.to_string(),
                            ConfigProfile {
                                name: name.to_string(),
                                content,
                                description,
                            },
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn extract_description(content: &str, name: &str) -> String {
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('#') && !trimmed.starts_with("#!") {
                let desc = trimmed.trim_start_matches('#').trim();
                if !desc.is_empty() {
                    return desc.to_string();
                }
            }
        }
        format!("{} configuration", name)
    }

    pub fn list_profiles(&self) -> Vec<ConfigProfile> {
        self.profiles.values().cloned().collect()
    }

    pub fn get_profile(&self, name: &str) -> Result<ConfigProfile> {
        self.profiles
            .get(name)
            .cloned()
            .ok_or_else(|| AdminError::Config(format!("profile not found: {}", name)))
    }

    pub fn save_profile(&mut self, name: &str, content: &str) -> Result<ConfigProfile> {
        let path = self.config_dir.join("configs").join(format!("{}.ini", name));
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, content)?;
        let description = Self::extract_description(content, name);
        let profile = ConfigProfile {
            name: name.to_string(),
            content: content.to_string(),
            description,
        };
        self.profiles.insert(name.to_string(), profile.clone());
        Ok(profile)
    }

    pub fn delete_profile(&mut self, name: &str) -> Result<ConfigProfile> {
        let profile = self.profiles
            .remove(name)
            .ok_or_else(|| AdminError::Config(format!("profile not found: {}", name)))?;
        let path = self.config_dir.join("configs").join(format!("{}.ini", name));
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(profile)
    }

    pub fn compare(&self, profile_a: &str, profile_b: &str) -> Result<Vec<ConfigDiff>> {
        let a = self.get_profile(profile_a)?;
        let b = self.get_profile(profile_b)?;
        let map_a = Self::parse_ini_to_map(&a.content);
        let map_b = Self::parse_ini_to_map(&b.content);
        let mut diffs = Vec::new();
        let mut all_keys: Vec<String> = map_a
            .keys()
            .chain(map_b.keys())
            .cloned()
            .collect();
        all_keys.sort();
        all_keys.dedup();
        for key in all_keys {
            let va = map_a.get(&key).cloned().unwrap_or_default();
            let vb = map_b.get(&key).cloned().unwrap_or_default();
            if va != vb {
                diffs.push(ConfigDiff {
                    key,
                    value_a: va,
                    value_b: vb,
                });
            }
        }
        Ok(diffs)
    }

    fn parse_ini_to_map(content: &str) -> HashMap<String, String> {
        let mut map = HashMap::new();
        let mut section = String::new();
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                section = trimmed[1..trimmed.len() - 1].to_string();
            } else if !trimmed.starts_with('#') && !trimmed.is_empty() {
                if let Some((key, value)) = trimmed.split_once('=') {
                    let full_key = if section.is_empty() {
                        key.trim().to_string()
                    } else {
                        format!("{}.{}", section, key.trim())
                    };
                    map.insert(full_key, value.trim().to_string());
                }
            }
        }
        map
    }
}
