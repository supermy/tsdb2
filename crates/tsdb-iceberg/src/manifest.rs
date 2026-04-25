use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ManifestEntry {
    pub status: EntryStatus,
    pub snapshot_id: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_sequence_number: Option<u64>,
    pub data_file: DataFile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum EntryStatus {
    Existing = 0,
    Added = 1,
    Deleted = 2,
}

impl TryFrom<u8> for EntryStatus {
    type Error = String;
    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(EntryStatus::Existing),
            1 => Ok(EntryStatus::Added),
            2 => Ok(EntryStatus::Deleted),
            _ => Err(format!("invalid entry status: {}", value)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataFile {
    pub content: DataContentType,
    pub file_path: String,
    pub file_format: String,
    #[serde(default)]
    pub partition: PartitionData,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_sizes: Option<BTreeMap<i32, i64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_counts: Option<BTreeMap<i32, i64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_value_counts: Option<BTreeMap<i32, i64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lower_bounds: Option<BTreeMap<i32, Vec<u8>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_bounds: Option<BTreeMap<i32, Vec<u8>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_offsets: Option<Vec<i64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_order_id: Option<i32>,
}

pub type PartitionData = BTreeMap<i32, serde_json::Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum DataContentType {
    Data = 0,
    PositionDeletes = 1,
    EqualityDeletes = 2,
}

impl DataFile {
    pub fn new_parquet(file_path: String, record_count: i64, file_size_in_bytes: i64) -> Self {
        Self {
            content: DataContentType::Data,
            file_path,
            file_format: "parquet".to_string(),
            partition: PartitionData::new(),
            record_count,
            file_size_in_bytes,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            split_offsets: None,
            sort_order_id: None,
        }
    }

    pub fn lower_bound_i64(&self, field_id: i32) -> Option<i64> {
        self.lower_bounds
            .as_ref()
            .and_then(|b| b.get(&field_id))
            .and_then(|v| {
                if v.len() >= 8 {
                    let bytes: [u8; 8] = v[..8].try_into().ok()?;
                    Some(i64::from_be_bytes(bytes))
                } else {
                    None
                }
            })
    }

    pub fn upper_bound_i64(&self, field_id: i32) -> Option<i64> {
        self.upper_bounds
            .as_ref()
            .and_then(|b| b.get(&field_id))
            .and_then(|v| {
                if v.len() >= 8 {
                    let bytes: [u8; 8] = v[..8].try_into().ok()?;
                    Some(i64::from_be_bytes(bytes))
                } else {
                    None
                }
            })
    }

    pub fn lower_bound_f64(&self, field_id: i32) -> Option<f64> {
        self.lower_bounds
            .as_ref()
            .and_then(|b| b.get(&field_id))
            .and_then(|v| {
                if v.len() >= 8 {
                    let bytes: [u8; 8] = v[..8].try_into().ok()?;
                    Some(f64::from_be_bytes(bytes))
                } else {
                    None
                }
            })
    }

    pub fn upper_bound_f64(&self, field_id: i32) -> Option<f64> {
        self.upper_bounds
            .as_ref()
            .and_then(|b| b.get(&field_id))
            .and_then(|v| {
                if v.len() >= 8 {
                    let bytes: [u8; 8] = v[..8].try_into().ok()?;
                    Some(f64::from_be_bytes(bytes))
                } else {
                    None
                }
            })
    }

    pub fn lower_bound_str(&self, field_id: i32) -> Option<&str> {
        self.lower_bounds
            .as_ref()
            .and_then(|b| b.get(&field_id))
            .and_then(|v| std::str::from_utf8(v).ok())
    }

    pub fn upper_bound_str(&self, field_id: i32) -> Option<&str> {
        self.upper_bounds
            .as_ref()
            .and_then(|b| b.get(&field_id))
            .and_then(|v| std::str::from_utf8(v).ok())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ManifestMeta {
    pub manifest_id: i64,
    pub schema_id: i32,
    pub partition_spec_id: i32,
    #[serde(default)]
    pub added_files_count: i32,
    #[serde(default)]
    pub existing_files_count: i32,
    #[serde(default)]
    pub deleted_files_count: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions_summary: Option<Vec<PartitionFieldSummary>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionFieldSummary {
    #[serde(default)]
    pub contains_null: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contains_nan: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lower_bound: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_bound: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_file_serde_roundtrip() {
        let df = DataFile::new_parquet("/data/test.parquet".to_string(), 1000, 4096);
        let json = serde_json::to_string(&df).unwrap();
        let restored: DataFile = serde_json::from_str(&json).unwrap();
        assert_eq!(df, restored);
    }

    #[test]
    fn test_data_file_bounds_access() {
        let mut df = DataFile::new_parquet("/data/test.parquet".to_string(), 1000, 4096);
        let mut lower = BTreeMap::new();
        lower.insert(1, 1000i64.to_be_bytes().to_vec());
        let mut upper = BTreeMap::new();
        upper.insert(1, 2000i64.to_be_bytes().to_vec());
        df.lower_bounds = Some(lower);
        df.upper_bounds = Some(upper);

        assert_eq!(df.lower_bound_i64(1), Some(1000));
        assert_eq!(df.upper_bound_i64(1), Some(2000));
        assert_eq!(df.lower_bound_i64(999), None);
    }

    #[test]
    fn test_manifest_entry_serde_roundtrip() {
        let entry = ManifestEntry {
            status: EntryStatus::Added,
            snapshot_id: 1,
            sequence_number: Some(1),
            file_sequence_number: Some(1),
            data_file: DataFile::new_parquet("/data/test.parquet".to_string(), 100, 4096),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let restored: ManifestEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry, restored);
    }

    #[test]
    fn test_entry_status_values() {
        assert_eq!(EntryStatus::Existing as u8, 0);
        assert_eq!(EntryStatus::Added as u8, 1);
        assert_eq!(EntryStatus::Deleted as u8, 2);
        assert_eq!(EntryStatus::try_from(0).unwrap(), EntryStatus::Existing);
        assert_eq!(EntryStatus::try_from(1).unwrap(), EntryStatus::Added);
        assert_eq!(EntryStatus::try_from(2).unwrap(), EntryStatus::Deleted);
    }

    #[test]
    fn test_bounds_short_data_returns_none() {
        let mut df = DataFile::new_parquet("/data/test.parquet".to_string(), 1000, 4096);
        let mut lower = BTreeMap::new();
        lower.insert(1, vec![1u8, 2u8, 3u8]);
        df.lower_bounds = Some(lower);
        assert_eq!(df.lower_bound_i64(1), None);
        assert_eq!(df.lower_bound_f64(1), None);
    }

    #[test]
    fn test_bounds_invalid_utf8_returns_none() {
        let mut df = DataFile::new_parquet("/data/test.parquet".to_string(), 1000, 4096);
        let mut lower = BTreeMap::new();
        lower.insert(1, vec![0xFF, 0xFE, 0xFD, 0xFC]);
        df.lower_bounds = Some(lower);
        assert_eq!(df.lower_bound_str(1), None);
    }

    #[test]
    fn test_bounds_f64_access() {
        let mut df = DataFile::new_parquet("/data/test.parquet".to_string(), 1000, 4096);
        let mut lower = BTreeMap::new();
        lower.insert(1, 1.5f64.to_be_bytes().to_vec());
        let mut upper = BTreeMap::new();
        upper.insert(1, 2.78f64.to_be_bytes().to_vec());
        df.lower_bounds = Some(lower);
        df.upper_bounds = Some(upper);
        assert_eq!(df.lower_bound_f64(1), Some(1.5));
        assert_eq!(df.upper_bound_f64(1), Some(2.78));
    }
}
