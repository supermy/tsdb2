use serde::{Deserialize, Serialize};

use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionSpec {
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionField {
    pub source_id: i32,
    pub field_id: i32,
    pub name: String,
    pub transform: Transform,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Transform {
    #[serde(rename = "identity")]
    Identity,
    #[serde(rename = "bucket")]
    Bucket { n: u32 },
    #[serde(rename = "truncate")]
    Truncate { w: u32 },
    #[serde(rename = "year")]
    Year,
    #[serde(rename = "month")]
    Month,
    #[serde(rename = "day")]
    Day,
    #[serde(rename = "hour")]
    Hour,
    #[serde(rename = "void")]
    Void,
}

impl Transform {
    pub fn apply(&self, value: &serde_json::Value) -> Option<serde_json::Value> {
        match self {
            Transform::Identity => Some(value.clone()),
            Transform::Day => {
                if let Some(ts) = value.as_i64() {
                    let secs = ts / 1_000_000;
                    chrono::DateTime::from_timestamp(secs, 0)
                        .map(|dt| serde_json::Value::String(dt.format("%Y-%m-%d").to_string()))
                } else {
                    value
                        .as_str()
                        .map(|s| serde_json::Value::String(s.to_string()))
                }
            }
            Transform::Hour => {
                if let Some(ts) = value.as_i64() {
                    let secs = ts / 1_000_000;
                    chrono::DateTime::from_timestamp(secs, 0)
                        .map(|dt| serde_json::Value::String(dt.format("%Y-%m-%d-%H").to_string()))
                } else {
                    None
                }
            }
            Transform::Month => {
                if let Some(ts) = value.as_i64() {
                    let secs = ts / 1_000_000;
                    chrono::DateTime::from_timestamp(secs, 0)
                        .map(|dt| serde_json::Value::String(dt.format("%Y-%m").to_string()))
                } else {
                    None
                }
            }
            Transform::Year => {
                if let Some(ts) = value.as_i64() {
                    let secs = ts / 1_000_000;
                    chrono::DateTime::from_timestamp(secs, 0)
                        .map(|dt| serde_json::Value::String(dt.format("%Y").to_string()))
                } else {
                    None
                }
            }
            Transform::Bucket { n } => {
                if let Some(s) = value.as_str() {
                    let hash = murmur2_hash(s.as_bytes());
                    Some(serde_json::Value::Number(((hash % *n) as i64).into()))
                } else if let Some(i) = value.as_i64() {
                    let hash = murmur2_hash(&i.to_be_bytes());
                    Some(serde_json::Value::Number(((hash % *n) as i64).into()))
                } else {
                    None
                }
            }
            Transform::Truncate { w } => {
                if let Some(s) = value.as_str() {
                    let truncated: String = s.chars().take(*w as usize).collect();
                    Some(serde_json::Value::String(truncated))
                } else {
                    None
                }
            }
            Transform::Void => None,
        }
    }
}

impl PartitionSpec {
    pub fn new(spec_id: i32, fields: Vec<PartitionField>) -> Self {
        Self { spec_id, fields }
    }

    pub fn unpartitioned(spec_id: i32) -> Self {
        Self {
            spec_id,
            fields: Vec::new(),
        }
    }

    pub fn day_partition(spec_id: i32, timestamp_source_id: i32) -> Self {
        Self {
            spec_id,
            fields: vec![PartitionField {
                source_id: timestamp_source_id,
                field_id: 1000,
                name: "ts_day".to_string(),
                transform: Transform::Day,
            }],
        }
    }

    pub fn partition_path(
        &self,
        values: &std::collections::BTreeMap<i32, serde_json::Value>,
    ) -> String {
        if self.fields.is_empty() {
            return String::new();
        }
        self.fields
            .iter()
            .filter_map(|pf| {
                values.get(&pf.source_id).and_then(|v| {
                    let transformed = pf.transform.apply(v);
                    transformed.map(|tv| format!("{}={}", pf.name, tv))
                })
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    pub fn evolve(&self, changes: Vec<PartitionSpecChange>) -> Self {
        let mut new_fields = self.fields.clone();
        for change in changes {
            match change {
                PartitionSpecChange::AddField {
                    source_id,
                    name,
                    transform,
                } => {
                    let field_id = new_fields.iter().map(|f| f.field_id).max().unwrap_or(999) + 1;
                    new_fields.push(PartitionField {
                        source_id,
                        field_id,
                        name,
                        transform,
                    });
                }
                PartitionSpecChange::RemoveField { field_id } => {
                    new_fields.retain(|f| f.field_id != field_id);
                }
                PartitionSpecChange::RenameField { field_id, new_name } => {
                    if let Some(f) = new_fields.iter_mut().find(|f| f.field_id == field_id) {
                        f.name = new_name;
                    }
                }
            }
        }
        PartitionSpec {
            spec_id: self.spec_id + 1,
            fields: new_fields,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PartitionSpecChange {
    AddField {
        source_id: i32,
        name: String,
        transform: Transform,
    },
    RemoveField {
        field_id: i32,
    },
    RenameField {
        field_id: i32,
        new_name: String,
    },
}

pub fn apply_partition_transform(
    transform: &Transform,
    timestamp_micros: i64,
) -> Result<serde_json::Value> {
    let val = serde_json::Value::Number(timestamp_micros.into());
    transform.apply(&val).ok_or_else(|| {
        crate::error::IcebergError::PartitionSpec(format!(
            "transform {:?} returned None for timestamp {}",
            transform, timestamp_micros
        ))
    })
}

fn murmur2_hash(data: &[u8]) -> u32 {
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;
    let mut hash: u32 = 0;
    let mut i = 0;
    while i + 4 <= data.len() {
        let k = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
        let mut k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        hash = hash.wrapping_mul(M).wrapping_add(k);
        i += 4;
    }
    let remaining = data.len() - i;
    if remaining == 3 {
        hash ^= (data[i + 2] as u32) << 16;
        hash ^= (data[i + 1] as u32) << 8;
        hash ^= data[i] as u32;
        hash = hash.wrapping_mul(M);
    } else if remaining == 2 {
        hash ^= (data[i + 1] as u32) << 8;
        hash ^= data[i] as u32;
        hash = hash.wrapping_mul(M);
    } else if remaining == 1 {
        hash ^= data[i] as u32;
        hash = hash.wrapping_mul(M);
    }
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(M);
    hash ^= hash >> 15;
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_day() {
        let ts = 1_714_089_600_000_000i64;
        let result = Transform::Day.apply(&serde_json::Value::Number(ts.into()));
        assert!(result.is_some());
        let binding = result.unwrap();
        let date_str = binding.as_str().unwrap();
        assert!(date_str.contains("2024"));
    }

    #[test]
    fn test_transform_identity() {
        let val = serde_json::Value::String("test".to_string());
        let result = Transform::Identity.apply(&val);
        assert_eq!(result, Some(val));
    }

    #[test]
    fn test_partition_path() {
        let spec = PartitionSpec::day_partition(0, 1);
        let mut values = std::collections::BTreeMap::new();
        values.insert(
            1,
            serde_json::Value::Number(1_714_089_600_000_000i64.into()),
        );
        let path = spec.partition_path(&values);
        assert!(path.starts_with("ts_day="));
    }

    #[test]
    fn test_unpartitioned() {
        let spec = PartitionSpec::unpartitioned(0);
        assert!(spec.fields.is_empty());
        let path = spec.partition_path(&std::collections::BTreeMap::new());
        assert!(path.is_empty());
    }

    #[test]
    fn test_partition_spec_evolve() {
        let spec = PartitionSpec::unpartitioned(0);
        let evolved = spec.evolve(vec![PartitionSpecChange::AddField {
            source_id: 1,
            name: "ts_day".to_string(),
            transform: Transform::Day,
        }]);
        assert_eq!(evolved.spec_id, 1);
        assert_eq!(evolved.fields.len(), 1);
    }

    #[test]
    fn test_bucket_transform_string() {
        let transform = Transform::Bucket { n: 16 };
        let val = serde_json::Value::String("hello".to_string());
        let result = transform.apply(&val);
        assert!(result.is_some());
        let bucket = result.unwrap().as_i64().unwrap();
        assert!((0..16).contains(&bucket));
    }

    #[test]
    fn test_bucket_transform_integer() {
        let transform = Transform::Bucket { n: 8 };
        let val = serde_json::Value::Number(42i64.into());
        let result = transform.apply(&val);
        assert!(result.is_some());
        let bucket = result.unwrap().as_i64().unwrap();
        assert!((0..8).contains(&bucket));
    }

    #[test]
    fn test_bucket_consistency() {
        let transform = Transform::Bucket { n: 16 };
        let val = serde_json::Value::String("test-key".to_string());
        let r1 = transform.apply(&val);
        let r2 = transform.apply(&val);
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_murmur2_hash_deterministic() {
        let data = b"test-data";
        let h1 = murmur2_hash(data);
        let h2 = murmur2_hash(data);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_transform_hour() {
        let transform = Transform::Hour;
        let val = serde_json::Value::Number(1_714_089_600_000_000i64.into());
        let result = transform.apply(&val);
        assert!(result.is_some());
    }

    #[test]
    fn test_transform_month() {
        let transform = Transform::Month;
        let val = serde_json::Value::Number(1_714_089_600_000_000i64.into());
        let result = transform.apply(&val);
        assert!(result.is_some());
    }

    #[test]
    fn test_transform_year() {
        let transform = Transform::Year;
        let val = serde_json::Value::Number(1_714_089_600_000_000i64.into());
        let result = transform.apply(&val);
        assert!(result.is_some());
    }

    #[test]
    fn test_transform_truncate() {
        let transform = Transform::Truncate { w: 5 };
        let val = serde_json::Value::String("hello world".to_string());
        let result = transform.apply(&val);
        assert_eq!(result, Some(serde_json::Value::String("hello".to_string())));
    }

    #[test]
    fn test_transform_void() {
        let transform = Transform::Void;
        let val = serde_json::Value::Number(42i64.into());
        let result = transform.apply(&val);
        assert_eq!(result, None);
    }

    #[test]
    fn test_transform_hour_non_integer() {
        let transform = Transform::Hour;
        let val = serde_json::Value::String("not-a-number".to_string());
        let result = transform.apply(&val);
        assert_eq!(result, None);
    }

    #[test]
    fn test_murmur2_hash_empty() {
        let h1 = murmur2_hash(b"");
        let h2 = murmur2_hash(b"a");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_murmur2_hash_short_data() {
        let h1 = murmur2_hash(b"a");
        let h2 = murmur2_hash(b"ab");
        let h3 = murmur2_hash(b"abc");
        assert_ne!(h1, h2);
        assert_ne!(h2, h3);
    }
}
