pub fn parse_partition_dir(dir_name: &str) -> (String, String) {
    if let Some(rest) = dir_name.strip_prefix("ts_") {
        if let Some(last_underscore) = rest.rfind('_') {
            let measurement = rest[..last_underscore].to_string();
            let date_part = rest[last_underscore + 1..].to_string();
            return (measurement, date_part);
        }
    }
    (dir_name.to_string(), String::new())
}

pub fn validate_path_within_dir(path: &str, base_dir: &std::path::Path) -> Result<(), String> {
    let canonical_base = base_dir
        .canonicalize()
        .map_err(|e| format!("invalid base dir: {}", e))?;
    let target = std::path::Path::new(path);
    let canonical_target = if target.is_absolute() {
        target
            .canonicalize()
            .map_err(|e| format!("invalid path: {}", e))?
    } else {
        canonical_base
            .join(path)
            .canonicalize()
            .map_err(|e| format!("invalid path: {}", e))?
    };
    if !canonical_target.starts_with(&canonical_base) {
        return Err("path traversal not allowed".to_string());
    }
    Ok(())
}

pub fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("name cannot be empty".to_string());
    }
    if name.contains("..") || name.contains('/') || name.contains('\\') {
        return Err("name contains invalid characters".to_string());
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        return Err("name must be alphanumeric, dash, or underscore only".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_partition_dir_valid() {
        let (m, d) = parse_partition_dir("ts_cpu_20260401");
        assert_eq!(m, "cpu");
        assert_eq!(d, "20260401");
    }

    #[test]
    fn test_parse_partition_dir_multi_underscore() {
        let (m, d) = parse_partition_dir("ts_memory_usage_20251231");
        assert_eq!(m, "memory_usage");
        assert_eq!(d, "20251231");
    }

    #[test]
    fn test_parse_partition_dir_no_prefix() {
        let (m, d) = parse_partition_dir("default");
        assert_eq!(m, "default");
        assert_eq!(d, "");
    }

    #[test]
    fn test_parse_partition_dir_only_ts() {
        let (m, d) = parse_partition_dir("ts_something");
        assert_eq!(m, "ts_something");
        assert_eq!(d, "");
    }

    #[test]
    fn test_validate_name_valid() {
        assert!(validate_name("cpu").is_ok());
        assert!(validate_name("memory-usage").is_ok());
        assert!(validate_name("disk_io_01").is_ok());
        assert!(validate_name("a").is_ok());
    }

    #[test]
    fn test_validate_name_empty() {
        assert!(validate_name("").is_err());
    }

    #[test]
    fn test_validate_name_dot_dot() {
        assert!(validate_name("..").is_err());
        assert!(validate_name("foo..bar").is_err());
    }

    #[test]
    fn test_validate_name_slash() {
        assert!(validate_name("foo/bar").is_err());
        assert!(validate_name("/etc/passwd").is_err());
    }

    #[test]
    fn test_validate_name_backslash() {
        assert!(validate_name("foo\\bar").is_err());
    }

    #[test]
    fn test_validate_name_special_chars() {
        assert!(validate_name("foo bar").is_err());
        assert!(validate_name("foo@bar").is_err());
        assert!(validate_name("foo.bar").is_err());
    }

    #[test]
    fn test_validate_path_within_dir_valid() {
        let tmp = tempfile::tempdir().unwrap();
        let file_path = tmp.path().join("test.parquet");
        std::fs::write(&file_path, "data").unwrap();
        let result = validate_path_within_dir(file_path.to_str().unwrap(), tmp.path());
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_path_within_dir_traversal() {
        let tmp = tempfile::tempdir().unwrap();
        let file_path = tmp.path().join("test.parquet");
        std::fs::write(&file_path, "data").unwrap();
        let traversal_path = tmp.path().join("../../etc/passwd");
        let result = validate_path_within_dir(traversal_path.to_str().unwrap(), tmp.path());
        assert!(result.is_err());
    }
}
