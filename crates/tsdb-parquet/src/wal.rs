use crate::error::{Result, TsdbParquetError};
use std::io::{BufWriter, Write};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WALEntryType {
    Insert = 0,
    Delete = 1,
    Checkpoint = 2,
}

impl TryFrom<u8> for WALEntryType {
    type Error = TsdbParquetError;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Insert),
            1 => Ok(Self::Delete),
            2 => Ok(Self::Checkpoint),
            _ => Err(TsdbParquetError::Wal(format!(
                "invalid WAL entry type: {}",
                value
            ))),
        }
    }
}

/// WAL 条目
///
/// 编码格式 (小端序):
/// ```text
/// [payload_len: u32 LE][entry_type: u8][sequence: u64 LE][payload: bytes][crc32: u32 LE]
/// ```
#[derive(Debug, Clone)]
pub struct WALEntry {
    /// 条目类型
    pub entry_type: WALEntryType,
    /// 全局递增序列号
    pub sequence: u64,
    /// 负载数据 (序列化的 DataPoint 或 Checkpoint 信息)
    pub payload: Vec<u8>,
}

/// WAL 头部固定大小: 4(payload_len) + 1(entry_type) + 8(sequence) = 13 字节
const WAL_HEADER_SIZE: usize = 4 + 1 + 8;
/// WAL 尾部 CRC32 大小: 4 字节
const WAL_FOOTER_SIZE: usize = 4;

impl WALEntry {
    /// 编码为二进制: header + payload + crc32
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(WAL_HEADER_SIZE + self.payload.len() + WAL_FOOTER_SIZE);

        let payload_len = self.payload.len() as u32;
        buf.extend_from_slice(&payload_len.to_le_bytes());
        buf.push(self.entry_type as u8);
        buf.extend_from_slice(&self.sequence.to_le_bytes());
        buf.extend_from_slice(&self.payload);

        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    /// 从二进制解码, 校验 CRC32
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < WAL_HEADER_SIZE + WAL_FOOTER_SIZE {
            return Err(TsdbParquetError::Wal("WAL entry too short".into()));
        }

        let payload_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let entry_type = WALEntryType::try_from(data[4])?;
        let sequence = u64::from_le_bytes([
            data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[12],
        ]);

        let expected_len = WAL_HEADER_SIZE + payload_len + WAL_FOOTER_SIZE;
        if data.len() < expected_len {
            return Err(TsdbParquetError::Wal(format!(
                "WAL entry truncated: expected {} bytes, got {}",
                expected_len,
                data.len()
            )));
        }

        let payload = data[WAL_HEADER_SIZE..WAL_HEADER_SIZE + payload_len].to_vec();

        let stored_crc = u32::from_le_bytes([
            data[expected_len - 4],
            data[expected_len - 3],
            data[expected_len - 2],
            data[expected_len - 1],
        ]);
        let computed_crc = crc32fast::hash(&data[..expected_len - 4]);

        if stored_crc != computed_crc {
            return Err(TsdbParquetError::Wal(format!(
                "CRC mismatch: stored={}, computed={}",
                stored_crc, computed_crc
            )));
        }

        Ok(Self {
            entry_type,
            sequence,
            payload,
        })
    }
}

/// 预写日志 (Write-Ahead Log)
///
/// 保证写入持久性: 数据先写入 WAL, 再写入内存缓冲区。
/// 崩溃恢复时从 WAL 重放未持久化的条目。
pub struct TsdbWAL {
    path: std::path::PathBuf,
    sequence: std::sync::atomic::AtomicU64,
    writer: std::sync::Mutex<BufWriter<std::fs::File>>,
}

impl TsdbWAL {
    /// 创建新的 WAL 文件
    ///
    /// 自动创建父目录, 序列号从 0 开始。
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| TsdbParquetError::Wal(format!("failed to create WAL dir: {}", e)))?;
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| TsdbParquetError::Wal(format!("failed to open WAL: {}", e)))?;

        Ok(Self {
            path,
            sequence: std::sync::atomic::AtomicU64::new(0),
            writer: std::sync::Mutex::new(BufWriter::new(file)),
        })
    }

    /// 追加条目到 WAL
    ///
    /// 序列号自动递增, 返回分配的序列号。
    pub fn append(&self, entry_type: WALEntryType, payload: &[u8]) -> Result<u64> {
        let seq = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let entry = WALEntry {
            entry_type,
            sequence: seq,
            payload: payload.to_vec(),
        };

        let encoded = entry.encode();

        let mut writer = self.writer.lock().unwrap_or_else(|e| e.into_inner());
        writer
            .write_all(&encoded)
            .map_err(|e| TsdbParquetError::Wal(format!("WAL write failed: {}", e)))?;
        writer
            .flush()
            .map_err(|e| TsdbParquetError::Wal(format!("WAL flush failed: {}", e)))?;

        Ok(seq)
    }

    /// 将 WAL 文件刷盘 (fsync)
    pub fn sync(&self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap_or_else(|e| e.into_inner());
        writer
            .flush()
            .map_err(|e| TsdbParquetError::Wal(format!("WAL flush before sync failed: {}", e)))?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|e| TsdbParquetError::Wal(format!("WAL sync failed: {}", e)))?;
        Ok(())
    }

    /// 从 WAL 文件恢复所有条目
    ///
    /// 逐条解码, CRC 校验失败时停止 (截断尾部损坏数据)。
    pub fn recover(path: impl AsRef<Path>) -> Result<Vec<WALEntry>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let data = std::fs::read(path)
            .map_err(|e| TsdbParquetError::Wal(format!("failed to read WAL: {}", e)))?;

        let mut entries = Vec::new();
        let mut offset = 0;

        while offset < data.len() {
            if offset + WAL_HEADER_SIZE + WAL_FOOTER_SIZE > data.len() {
                break;
            }

            let payload_len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;

            let entry_len = WAL_HEADER_SIZE + payload_len + WAL_FOOTER_SIZE;
            if offset + entry_len > data.len() {
                break;
            }

            match WALEntry::decode(&data[offset..offset + entry_len]) {
                Ok(entry) => {
                    entries.push(entry);
                    offset += entry_len;
                }
                Err(_) => break,
            }
        }

        Ok(entries)
    }

    /// 获取 WAL 文件路径
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_entry_roundtrip() {
        let entry = WALEntry {
            entry_type: WALEntryType::Insert,
            sequence: 42,
            payload: b"test data".to_vec(),
        };

        let encoded = entry.encode();
        let decoded = WALEntry::decode(&encoded).unwrap();

        assert_eq!(decoded.entry_type, WALEntryType::Insert);
        assert_eq!(decoded.sequence, 42);
        assert_eq!(decoded.payload, b"test data");
    }

    #[test]
    fn test_wal_append_recover() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        let wal = TsdbWAL::create(&wal_path).unwrap();

        wal.append(WALEntryType::Insert, b"point1").unwrap();
        wal.append(WALEntryType::Insert, b"point2").unwrap();
        wal.append(WALEntryType::Checkpoint, b"snap").unwrap();
        wal.sync().unwrap();

        let entries = TsdbWAL::recover(&wal_path).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].entry_type, WALEntryType::Insert);
        assert_eq!(entries[0].payload, b"point1");
        assert_eq!(entries[1].sequence, 1);
        assert_eq!(entries[2].entry_type, WALEntryType::Checkpoint);
    }

    #[test]
    fn test_wal_crc_validation() {
        let entry = WALEntry {
            entry_type: WALEntryType::Insert,
            sequence: 0,
            payload: b"data".to_vec(),
        };

        let mut encoded = entry.encode();
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;

        assert!(WALEntry::decode(&encoded).is_err());
    }

    #[test]
    fn test_wal_recover_empty() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("nonexistent.wal");
        let entries = TsdbWAL::recover(&wal_path).unwrap();
        assert!(entries.is_empty());
    }
}
