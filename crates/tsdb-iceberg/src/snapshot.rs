use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Snapshot {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub sequence_number: u64,
    pub timestamp_ms: u64,
    pub manifest_list: String,
    pub summary: SnapshotSummary,
    pub schema_id: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SnapshotSummary {
    pub operation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub added_data_files: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_data_files: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub added_records: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_records: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_data_files: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_records: Option<i64>,
    #[serde(default)]
    pub extra: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotOperation {
    Append,
    Replace,
    Overwrite,
    Delete,
}

impl std::fmt::Display for SnapshotOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotOperation::Append => write!(f, "append"),
            SnapshotOperation::Replace => write!(f, "replace"),
            SnapshotOperation::Overwrite => write!(f, "overwrite"),
            SnapshotOperation::Delete => write!(f, "delete"),
        }
    }
}

impl Snapshot {
    pub fn new_append(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        sequence_number: u64,
        schema_id: i32,
    ) -> Self {
        Self {
            snapshot_id,
            parent_snapshot_id,
            sequence_number,
            timestamp_ms: now_ms(),
            manifest_list: format!("snap_{}", snapshot_id),
            summary: SnapshotSummary {
                operation: SnapshotOperation::Append.to_string(),
                added_data_files: None,
                deleted_data_files: None,
                added_records: None,
                deleted_records: None,
                total_data_files: None,
                total_records: None,
                extra: BTreeMap::new(),
            },
            schema_id,
        }
    }

    pub fn new_replace(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        sequence_number: u64,
        schema_id: i32,
    ) -> Self {
        let mut snap =
            Self::new_append(snapshot_id, parent_snapshot_id, sequence_number, schema_id);
        snap.summary.operation = SnapshotOperation::Replace.to_string();
        snap
    }

    pub fn new_overwrite(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        sequence_number: u64,
        schema_id: i32,
    ) -> Self {
        let mut snap =
            Self::new_append(snapshot_id, parent_snapshot_id, sequence_number, schema_id);
        snap.summary.operation = SnapshotOperation::Overwrite.to_string();
        snap
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SnapshotLogEntry {
    pub snapshot_id: i64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Ref {
    pub snapshot_id: i64,
    #[serde(rename = "type")]
    pub ref_type: RefType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_snapshots_to_keep: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_snapshot_age_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ref_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RefType {
    #[serde(rename = "main")]
    Main,
    #[serde(rename = "branch")]
    Branch,
    #[serde(rename = "tag")]
    Tag,
}

use crate::util::now_ms;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_serde_roundtrip() {
        let snap = Snapshot::new_append(1, None, 1, 0);
        let json = serde_json::to_string(&snap).unwrap();
        let restored: Snapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(snap, restored);
    }

    #[test]
    fn test_snapshot_new_append() {
        let snap = Snapshot::new_append(1, None, 1, 0);
        assert_eq!(snap.snapshot_id, 1);
        assert_eq!(snap.summary.operation, "append");
        assert_eq!(snap.schema_id, 0);
    }

    #[test]
    fn test_snapshot_new_replace() {
        let snap = Snapshot::new_replace(2, Some(1), 2, 0);
        assert_eq!(snap.summary.operation, "replace");
        assert_eq!(snap.parent_snapshot_id, Some(1));
    }

    #[test]
    fn test_ref_serde() {
        let r = Ref {
            snapshot_id: 1,
            ref_type: RefType::Main,
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        };
        let json = serde_json::to_string(&r).unwrap();
        let restored: Ref = serde_json::from_str(&json).unwrap();
        assert_eq!(r, restored);
    }
}
