use thiserror::Error;

/// RocksDB 时序存储引擎错误类型
#[derive(Debug, Error)]
pub enum TsdbRocksDbError {
    /// 无效的键格式 (长度不足或解析失败)
    #[error("Invalid key: {0}")]
    InvalidKey(String),
    /// 无效的值格式 (类型标签未知或数据截断)
    #[error("Invalid value: {0}")]
    InvalidValue(String),
    /// Column Family 不存在
    #[error("CF not found: {0}")]
    CfNotFound(String),
    /// RocksDB 内部错误
    #[error("RocksDB error: {0}")]
    RocksDB(#[from] rocksdb::Error),
    /// 文件 I/O 错误
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// UTF-8 字符串解析错误
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    /// UTF-8 字符串解码错误
    #[error("UTF-8 decode error: {0}")]
    FromUtf8(#[from] std::string::FromUtf8Error),
    /// 整数类型转换错误
    #[error("TryFromInt error: {0}")]
    TryFromInt(#[from] std::num::TryFromIntError),
    /// Arrow 数据转换错误
    #[error("Arrow conversion error: {0}")]
    ArrowConversion(#[from] tsdb_arrow::error::TsdbArrowError),
    /// JSON 序列化/反序列化错误
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// 统一结果类型
pub type Result<T> = std::result::Result<T, TsdbRocksDbError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_key_construction() {
        let err = TsdbRocksDbError::InvalidKey("key too short".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Invalid key"));
        assert!(msg.contains("key too short"));
    }

    #[test]
    fn test_invalid_value_construction() {
        let err = TsdbRocksDbError::InvalidValue("truncated u16".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Invalid value"));
        assert!(msg.contains("truncated u16"));
    }

    #[test]
    fn test_cf_not_found_construction() {
        let err = TsdbRocksDbError::CfNotFound("ts_cpu_20260101".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("CF not found"));
        assert!(msg.contains("ts_cpu_20260101"));
    }

    #[test]
    fn test_arrow_conversion_error_chain() {
        let arrow_err = tsdb_arrow::error::TsdbArrowError::Schema("test".to_string());
        let err = TsdbRocksDbError::ArrowConversion(arrow_err);
        let msg = format!("{}", err);
        assert!(msg.contains("Arrow conversion error"));
    }

    #[test]
    fn test_error_source_chain() {
        let arrow_err = tsdb_arrow::error::TsdbArrowError::Schema("test".to_string());
        let err: TsdbRocksDbError = arrow_err.into();
        assert!(
            std::error::Error::source(&err).is_some(),
            "ArrowConversion should have a source"
        );

        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: TsdbRocksDbError = io_err.into();
        assert!(
            std::error::Error::source(&err).is_some(),
            "Io should have a source"
        );
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
        let err: TsdbRocksDbError = io_err.into();
        let msg = format!("{}", err);
        assert!(msg.contains("IO error"));
    }

    #[test]
    fn test_error_from_utf8() {
        let bad_bytes: Vec<u8> = vec![0xFF, 0xFE];
        match std::str::from_utf8(&bad_bytes) {
            Err(utf8_err) => {
                let err: TsdbRocksDbError = utf8_err.into();
                let msg = format!("{}", err);
                assert!(msg.contains("UTF-8 error"));
            }
            Ok(_) => panic!("expected UTF-8 error"),
        }
    }

    #[test]
    fn test_error_debug_format() {
        let err = TsdbRocksDbError::InvalidKey("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("InvalidKey"));
    }

    #[test]
    fn test_result_type_ok() {
        let result: Result<i32> = Ok(42);
        assert!(matches!(result, Ok(42)));
    }

    #[test]
    fn test_result_type_err() {
        let result: Result<i32> = Err(TsdbRocksDbError::CfNotFound("test".to_string()));
        assert!(result.is_err());
    }
}
