use rocksdb::WriteBatch;

use crate::catalog::{
    IcebergCatalog, TableMetadata, CATALOG_CF, MANIFEST_CF, MANIFEST_LIST_CF, SNAPSHOT_CF,
    TABLE_META_CF,
};
use crate::error::{IcebergError, Result};
use crate::manifest::{ManifestEntry, ManifestMeta};
use crate::snapshot::Snapshot;

pub fn atomic_commit(
    catalog: &IcebergCatalog,
    table_name: &str,
    old_meta: &TableMetadata,
    new_meta: &TableMetadata,
    new_snapshot: Option<&Snapshot>,
    new_manifest_metas: &[ManifestMeta],
    new_entries_by_manifest: &[(i64, Vec<ManifestEntry>)],
) -> Result<()> {
    let current = catalog.load_metadata(table_name)?;
    if current.last_sequence_number != old_meta.last_sequence_number {
        return Err(IcebergError::CommitConflict);
    }

    let mut batch = WriteBatch::default();

    if let Some(snap) = new_snapshot {
        let snap_key = format!("{}\x00snap\x00{}", table_name, snap.snapshot_id);
        let snap_json = serde_json::to_vec(snap)?;
        batch.put_cf(&catalog.cf(SNAPSHOT_CF)?, snap_key.as_bytes(), &snap_json);

        for (seq, mm) in new_manifest_metas.iter().enumerate() {
            let ml_key = format!("{}\x00{}\x00{}", table_name, snap.snapshot_id, seq);
            let mm_json = serde_json::to_vec(mm)?;
            batch.put_cf(&catalog.cf(MANIFEST_LIST_CF)?, ml_key.as_bytes(), &mm_json);
        }
    }

    for (manifest_id, entries) in new_entries_by_manifest {
        for (idx, entry) in entries.iter().enumerate() {
            let me_key = format!("{}\x00{}\x00{}", table_name, manifest_id, idx);
            let entry_json = serde_json::to_vec(entry)?;
            batch.put_cf(&catalog.cf(MANIFEST_CF)?, me_key.as_bytes(), &entry_json);
        }
    }

    let meta_key = format!("{}\x00meta", table_name);
    let meta_json = serde_json::to_vec(new_meta)?;
    batch.put_cf(&catalog.cf(TABLE_META_CF)?, meta_key.as_bytes(), &meta_json);
    batch.put_cf(&catalog.cf(CATALOG_CF)?, table_name.as_bytes(), &meta_json);

    catalog
        .write_batch(batch)
        .map_err(|e| IcebergError::Catalog(format!("write batch failed: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::PartitionSpec;
    use crate::schema::{Field, IcebergType, Schema};
    use crate::snapshot::Snapshot;
    use tempfile::TempDir;

    fn make_test_schema() -> Schema {
        Schema::new(
            0,
            vec![Field {
                id: 1,
                name: "timestamp".to_string(),
                required: true,
                field_type: IcebergType::Timestamptz,
                doc: None,
                initial_default: None,
                write_default: None,
            }],
        )
    }

    #[test]
    fn test_atomic_commit_success() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();

        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        catalog.create_table("cpu", schema, spec).unwrap();

        let old_meta = catalog.load_metadata("cpu").unwrap();
        let mut new_meta = old_meta.clone();
        new_meta.last_sequence_number += 1;
        new_meta.last_updated_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let snap = Snapshot::new_append(1, None, 1, 0);

        atomic_commit(&catalog, "cpu", &old_meta, &new_meta, Some(&snap), &[], &[]).unwrap();

        let loaded = catalog.load_metadata("cpu").unwrap();
        assert_eq!(loaded.last_sequence_number, 1);
    }

    #[test]
    fn test_atomic_commit_conflict() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();

        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        catalog.create_table("cpu", schema, spec).unwrap();

        let old_meta = catalog.load_metadata("cpu").unwrap();

        let mut first_meta = old_meta.clone();
        first_meta.last_sequence_number += 1;
        first_meta.last_updated_ms += 1;

        let snap1 = Snapshot::new_append(1, None, 1, 0);
        atomic_commit(
            &catalog,
            "cpu",
            &old_meta,
            &first_meta,
            Some(&snap1),
            &[],
            &[],
        )
        .unwrap();

        let mut second_meta = old_meta.clone();
        second_meta.last_sequence_number += 2;
        second_meta.last_updated_ms += 2;

        let snap2 = Snapshot::new_append(2, Some(1), 2, 0);
        let result = atomic_commit(
            &catalog,
            "cpu",
            &old_meta,
            &second_meta,
            Some(&snap2),
            &[],
            &[],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_atomic_commit_with_manifest_entries() {
        use crate::manifest::{
            DataContentType, DataFile, EntryStatus, ManifestEntry, ManifestMeta,
        };

        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();

        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        catalog.create_table("cpu", schema, spec).unwrap();

        let old_meta = catalog.load_metadata("cpu").unwrap();
        let mut new_meta = old_meta.clone();
        new_meta.last_sequence_number += 1;
        new_meta.last_updated_ms += 1;

        let snap = Snapshot::new_append(1, None, 1, 0);
        let mm = ManifestMeta {
            manifest_id: 100,
            schema_id: 0,
            partition_spec_id: 0,
            added_files_count: 1,
            existing_files_count: 0,
            deleted_files_count: 0,
            partitions_summary: None,
        };
        let entry = ManifestEntry {
            status: EntryStatus::Added,
            snapshot_id: 1,
            sequence_number: Some(1),
            file_sequence_number: Some(1),
            data_file: DataFile {
                content: DataContentType::Data,
                file_path: "/tmp/test.parquet".to_string(),
                file_format: "parquet".to_string(),
                partition: std::collections::BTreeMap::new(),
                record_count: 10,
                file_size_in_bytes: 1024,
                column_sizes: None,
                value_counts: None,
                null_value_counts: None,
                lower_bounds: None,
                upper_bounds: None,
                split_offsets: None,
                sort_order_id: None,
            },
        };

        atomic_commit(
            &catalog,
            "cpu",
            &old_meta,
            &new_meta,
            Some(&snap),
            &[mm],
            &[(100, vec![entry])],
        )
        .unwrap();

        let loaded = catalog.load_metadata("cpu").unwrap();
        assert_eq!(loaded.last_sequence_number, 1);
    }
}
