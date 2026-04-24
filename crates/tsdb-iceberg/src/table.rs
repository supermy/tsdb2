use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use tsdb_arrow::converter::datapoints_to_record_batch;
use tsdb_arrow::schema::{DataPoint, FieldValue};

use crate::catalog::{IcebergCatalog, TableMetadata};
use crate::commit::atomic_commit;
use crate::error::{IcebergError, Result};
use crate::manifest::{DataFile, EntryStatus, ManifestEntry, ManifestMeta};
use crate::partition::PartitionSpec;
use crate::schema::Schema;
use crate::snapshot::{Ref, RefType, Snapshot, SnapshotLogEntry};
use crate::stats::{
    apply_stats_to_data_file, collect_column_stats, merge_column_stats, ColumnStats,
};

pub struct IcebergTable {
    catalog: Arc<IcebergCatalog>,
    name: String,
    metadata: TableMetadata,
}

impl IcebergTable {
    pub fn new(catalog: Arc<IcebergCatalog>, name: String, metadata: TableMetadata) -> Self {
        Self {
            catalog,
            name,
            metadata,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &Schema {
        self.metadata.current_schema()
    }

    pub fn partition_spec(&self) -> &PartitionSpec {
        self.metadata.current_partition_spec()
    }

    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.metadata.current_snapshot()
    }

    pub fn snapshots(&self) -> &[Snapshot] {
        &self.metadata.snapshots
    }

    pub fn history(&self) -> &[SnapshotLogEntry] {
        &self.metadata.snapshot_log
    }

    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    pub fn append(&mut self, datapoints: &[DataPoint]) -> Result<()> {
        if datapoints.is_empty() {
            return Ok(());
        }

        let schema = self.metadata.current_schema().clone();
        let partition_spec = self.metadata.current_partition_spec().clone();
        let arrow_schema = schema.arrow_schema()?;

        let grouped = self.group_by_partition(datapoints, &partition_spec)?;

        let mut new_data_files = Vec::new();
        let mut added_records: i64 = 0;

        for (partition_data, dps) in grouped {
            let batch = datapoints_to_record_batch(&dps, arrow_schema.clone())?;

            let data_file = self.write_parquet_file(&batch, &partition_data)?;
            added_records += data_file.record_count;
            new_data_files.push(data_file);
        }

        let existing_files = match self.metadata.current_snapshot() {
            Some(snap) => self.collect_data_files(snap)?,
            None => Vec::new(),
        };

        let seq = self.catalog.next_sequence_number();
        let snapshot_id = generate_snapshot_id();
        let parent_snapshot_id = if self.metadata.current_snapshot_id >= 0 {
            Some(self.metadata.current_snapshot_id)
        } else {
            None
        };

        let manifest_id = generate_manifest_id();
        let mut entries = Vec::new();

        for df in &existing_files {
            entries.push(ManifestEntry {
                status: EntryStatus::Existing,
                snapshot_id: self.metadata.current_snapshot_id,
                sequence_number: None,
                file_sequence_number: None,
                data_file: df.clone(),
            });
        }

        for df in &new_data_files {
            entries.push(ManifestEntry {
                status: EntryStatus::Added,
                snapshot_id,
                sequence_number: Some(seq),
                file_sequence_number: Some(seq),
                data_file: df.clone(),
            });
        }

        let manifest_meta = ManifestMeta {
            manifest_id,
            schema_id: schema.schema_id,
            partition_spec_id: partition_spec.spec_id,
            added_files_count: new_data_files.len() as i32,
            existing_files_count: existing_files.len() as i32,
            deleted_files_count: 0,
            partitions_summary: None,
        };

        let mut new_snapshot =
            Snapshot::new_append(snapshot_id, parent_snapshot_id, seq, schema.schema_id);
        new_snapshot.summary.added_data_files = Some(new_data_files.len() as i32);
        new_snapshot.summary.added_records = Some(added_records);
        new_snapshot.summary.deleted_data_files = Some(0);
        new_snapshot.summary.deleted_records = Some(0);
        new_snapshot.summary.total_data_files =
            Some((existing_files.len() + new_data_files.len()) as i32);
        new_snapshot.summary.total_records =
            Some(existing_files.iter().map(|f| f.record_count).sum::<i64>() + added_records);

        let mut new_meta = self.metadata.clone();
        new_meta.last_sequence_number = seq;
        new_meta.last_updated_ms = now_ms();
        new_meta.current_snapshot_id = snapshot_id;
        new_meta.snapshots.push(new_snapshot.clone());
        new_meta.snapshot_log.push(SnapshotLogEntry {
            snapshot_id,
            timestamp_ms: new_snapshot.timestamp_ms,
        });

        atomic_commit(
            &self.catalog,
            &self.name,
            &self.metadata,
            &new_meta,
            Some(&new_snapshot),
            &[manifest_meta],
            &[(manifest_id, entries)],
        )?;

        self.metadata = new_meta;
        Ok(())
    }

    pub fn scan(&self) -> IcebergScanBuilder<'_> {
        IcebergScanBuilder::new(self)
    }

    pub fn scan_at_snapshot(&self, snapshot_id: i64) -> Result<IcebergScanBuilder<'_>> {
        self.metadata
            .snapshot_by_id(snapshot_id)
            .ok_or(IcebergError::SnapshotNotFound(snapshot_id))?;
        Ok(IcebergScanBuilder::new(self).with_snapshot_id(snapshot_id))
    }

    pub fn scan_at_timestamp(&self, timestamp_ms: u64) -> Result<IcebergScanBuilder<'_>> {
        let snap = self
            .metadata
            .snapshot_by_timestamp(timestamp_ms)
            .ok_or_else(|| {
                IcebergError::Catalog(format!(
                    "no snapshot found at or before timestamp {}",
                    timestamp_ms
                ))
            })?;
        Ok(IcebergScanBuilder::new(self).with_snapshot_id(snap.snapshot_id))
    }

    pub fn snapshot_by_id(&self, id: i64) -> Option<&Snapshot> {
        self.metadata.snapshot_by_id(id)
    }

    pub fn snapshot_by_timestamp(&self, ts_ms: u64) -> Option<&Snapshot> {
        self.metadata.snapshot_by_timestamp(ts_ms)
    }

    pub fn snapshot_diff(&self, from_id: i64, to_id: i64) -> Result<SnapshotDiff> {
        let from_snap = self
            .metadata
            .snapshot_by_id(from_id)
            .ok_or(IcebergError::SnapshotNotFound(from_id))?;
        let to_snap = self
            .metadata
            .snapshot_by_id(to_id)
            .ok_or(IcebergError::SnapshotNotFound(to_id))?;

        let from_files = self.collect_data_files(from_snap)?;
        let to_files = self.collect_data_files(to_snap)?;

        let from_paths: std::collections::HashSet<String> =
            from_files.iter().map(|f| f.file_path.clone()).collect();
        let to_paths: std::collections::HashSet<String> =
            to_files.iter().map(|f| f.file_path.clone()).collect();

        let added: Vec<DataFile> = to_files
            .into_iter()
            .filter(|f| !from_paths.contains(&f.file_path))
            .collect();
        let deleted: Vec<DataFile> = from_files
            .into_iter()
            .filter(|f| !to_paths.contains(&f.file_path))
            .collect();

        Ok(SnapshotDiff {
            added_files: added.len() as i32,
            deleted_files: deleted.len() as i32,
            added_records: added.iter().map(|f| f.record_count).sum(),
            deleted_records: deleted.iter().map(|f| f.record_count).sum(),
        })
    }

    pub fn rollback_to_snapshot(&mut self, snapshot_id: i64) -> Result<()> {
        let target = self
            .metadata
            .snapshot_by_id(snapshot_id)
            .ok_or(IcebergError::SnapshotNotFound(snapshot_id))?
            .clone();

        let target_manifests = self
            .catalog
            .load_manifest_list(&self.name, target.snapshot_id)?;

        let seq = self.catalog.next_sequence_number();
        let new_snapshot_id = generate_snapshot_id();

        let mut new_snap = Snapshot::new_replace(
            new_snapshot_id,
            Some(self.metadata.current_snapshot_id),
            seq,
            target.schema_id,
        );
        new_snap.manifest_list = target.manifest_list.clone();

        let mut new_meta = self.metadata.clone();
        new_meta.last_sequence_number = seq;
        new_meta.last_updated_ms = now_ms();
        new_meta.current_snapshot_id = new_snapshot_id;
        new_meta.snapshots.push(new_snap.clone());
        new_meta.snapshot_log.push(SnapshotLogEntry {
            snapshot_id: new_snapshot_id,
            timestamp_ms: new_snap.timestamp_ms,
        });

        atomic_commit(
            &self.catalog,
            &self.name,
            &self.metadata,
            &new_meta,
            Some(&new_snap),
            &target_manifests,
            &[],
        )?;

        self.metadata = new_meta;
        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        let snapshot = match self.metadata.current_snapshot() {
            Some(s) => s.clone(),
            None => return Ok(()),
        };

        let data_files = self.collect_data_files(&snapshot)?;
        if data_files.is_empty() {
            return Ok(());
        }

        let target_size = 128 * 1024 * 1024;
        let small_files: Vec<&DataFile> = data_files
            .iter()
            .filter(|f| f.file_size_in_bytes < target_size as i64)
            .collect();

        if small_files.len() <= 1 {
            return Ok(());
        }

        let small_paths: std::collections::HashSet<String> =
            small_files.iter().map(|f| f.file_path.clone()).collect();

        let other_files: Vec<&DataFile> = data_files
            .iter()
            .filter(|f| !small_paths.contains(&f.file_path))
            .collect();

        let schema = self.metadata.current_schema().clone();
        let arrow_schema = schema.arrow_schema()?;

        let mut by_partition: std::collections::BTreeMap<String, Vec<&DataFile>> =
            std::collections::BTreeMap::new();
        for df in &small_files {
            let key = df
                .partition
                .values()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("/");
            by_partition.entry(key).or_default().push(df);
        }

        let mut compacted_files = Vec::new();
        for (_partition_key, partition_small_files) in by_partition {
            if partition_small_files.len() <= 1 {
                for df in &partition_small_files {
                    compacted_files.push((*df).clone());
                }
                continue;
            }

            let mut all_batches = Vec::new();
            let mut expected_rows: i64 = 0;
            for df in &partition_small_files {
                let batches = self.read_data_file(df)?;
                expected_rows += df.record_count;
                all_batches.extend(batches);
            }

            if all_batches.is_empty() {
                continue;
            }

            let partition_data = partition_small_files[0].partition.clone();
            let mut new_data_file =
                self.write_parquet_file_from_batches(&all_batches, &partition_data, &arrow_schema)?;

            if new_data_file.record_count != expected_rows {
                return Err(IcebergError::Internal(format!(
                    "compact row count mismatch: expected {}, got {}",
                    expected_rows, new_data_file.record_count
                )));
            }

            let mut stats = ColumnStats::default();
            for batch in &all_batches {
                let batch_stats = collect_column_stats(batch, &schema);
                merge_column_stats(&mut stats, &batch_stats);
            }
            apply_stats_to_data_file(&stats, &mut new_data_file);

            compacted_files.push(new_data_file);
        }

        let seq = self.catalog.next_sequence_number();
        let new_snapshot_id = generate_snapshot_id();
        let manifest_id = generate_manifest_id();

        let mut entries = Vec::new();

        for df in &other_files {
            entries.push(ManifestEntry {
                status: EntryStatus::Existing,
                snapshot_id: self.metadata.current_snapshot_id,
                sequence_number: None,
                file_sequence_number: None,
                data_file: (*df).clone(),
            });
        }

        for df in &small_files {
            entries.push(ManifestEntry {
                status: EntryStatus::Deleted,
                snapshot_id: new_snapshot_id,
                sequence_number: Some(seq),
                file_sequence_number: None,
                data_file: (*df).clone(),
            });
        }
        let compacted_count = compacted_files.len() as i32;
        for df in compacted_files {
            entries.push(ManifestEntry {
                status: EntryStatus::Added,
                snapshot_id: new_snapshot_id,
                sequence_number: Some(seq),
                file_sequence_number: Some(seq),
                data_file: df,
            });
        }

        let manifest_meta = ManifestMeta {
            manifest_id,
            schema_id: schema.schema_id,
            partition_spec_id: self.metadata.current_partition_spec().spec_id,
            added_files_count: compacted_count,
            existing_files_count: other_files.len() as i32,
            deleted_files_count: small_files.len() as i32,
            partitions_summary: None,
        };

        let mut new_snap = Snapshot::new_replace(
            new_snapshot_id,
            Some(self.metadata.current_snapshot_id),
            seq,
            schema.schema_id,
        );
        new_snap.summary.added_data_files = Some(compacted_count);
        new_snap.summary.deleted_data_files = Some(small_files.len() as i32);

        let mut new_meta = self.metadata.clone();
        new_meta.last_sequence_number = seq;
        new_meta.last_updated_ms = now_ms();
        new_meta.current_snapshot_id = new_snapshot_id;
        new_meta.snapshots.push(new_snap.clone());
        new_meta.snapshot_log.push(SnapshotLogEntry {
            snapshot_id: new_snapshot_id,
            timestamp_ms: new_snap.timestamp_ms,
        });

        atomic_commit(
            &self.catalog,
            &self.name,
            &self.metadata,
            &new_meta,
            Some(&new_snap),
            &[manifest_meta],
            &[(manifest_id, entries)],
        )?;

        self.metadata = new_meta;
        Ok(())
    }

    pub fn expire_snapshots(&mut self, older_than_ms: u64, min_snapshots: u32) -> Result<()> {
        let total = self.metadata.snapshots.len();
        let keep = min_snapshots as usize;

        if total <= keep {
            return Ok(());
        }

        let protected_ids = self.collect_protected_snapshot_ids();

        let mut to_remove = Vec::new();
        for snap in &self.metadata.snapshots {
            if snap.timestamp_ms < older_than_ms
                && !protected_ids.contains(&snap.snapshot_id)
                && to_remove.len() < total - keep
            {
                to_remove.push(snap.snapshot_id);
            }
        }

        if to_remove.is_empty() {
            return Ok(());
        }

        for snap_id in &to_remove {
            let _ = self.catalog.delete_snapshot_data(&self.name, *snap_id);
        }

        let mut new_meta = self.metadata.clone();
        new_meta
            .snapshots
            .retain(|s| !to_remove.contains(&s.snapshot_id));
        new_meta
            .snapshot_log
            .retain(|e| !to_remove.contains(&e.snapshot_id));
        new_meta.last_updated_ms = now_ms();

        atomic_commit(
            &self.catalog,
            &self.name,
            &self.metadata,
            &new_meta,
            None,
            &[],
            &[],
        )?;

        self.metadata = new_meta;
        Ok(())
    }

    pub fn update_schema(&mut self, changes: Vec<crate::schema::SchemaChange>) -> Result<()> {
        let current = self.metadata.current_schema();
        let new_schema = current.apply_changes(changes)?;

        let mut new_meta = self.metadata.clone();
        new_meta.current_schema_id = new_schema.schema_id;
        new_meta.last_column_id = new_meta.last_column_id.max(new_schema.highest_field_id());
        new_meta.schemas.push(new_schema);
        new_meta.last_updated_ms = now_ms();

        atomic_commit(
            &self.catalog,
            &self.name,
            &self.metadata,
            &new_meta,
            None,
            &[],
            &[],
        )?;

        self.metadata = new_meta;
        Ok(())
    }

    pub fn update_partition_spec(&mut self, new_spec: PartitionSpec) -> Result<()> {
        let mut new_meta = self.metadata.clone();
        new_meta.default_spec_id = new_spec.spec_id;
        new_meta.partition_specs.push(new_spec);
        new_meta.last_updated_ms = now_ms();

        atomic_commit(
            &self.catalog,
            &self.name,
            &self.metadata,
            &new_meta,
            None,
            &[],
            &[],
        )?;

        self.metadata = new_meta;
        Ok(())
    }

    pub fn create_branch(&self, name: &str, snapshot_id: i64) -> Result<()> {
        if self.snapshot_by_id(snapshot_id).is_none() {
            return Err(IcebergError::SnapshotNotFound(snapshot_id));
        }
        let r = Ref {
            snapshot_id,
            ref_type: RefType::Branch,
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        };
        self.catalog.save_ref(&self.name, name, &r)
    }

    pub fn create_tag(&self, name: &str, snapshot_id: i64) -> Result<()> {
        if self.snapshot_by_id(snapshot_id).is_none() {
            return Err(IcebergError::SnapshotNotFound(snapshot_id));
        }
        let r = Ref {
            snapshot_id,
            ref_type: RefType::Tag,
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        };
        self.catalog.save_ref(&self.name, name, &r)
    }

    pub fn read_data_file(&self, data_file: &DataFile) -> Result<Vec<RecordBatch>> {
        let path = PathBuf::from(&data_file.file_path);
        if !path.exists() {
            return Err(IcebergError::Internal(format!(
                "data file not found: {}",
                data_file.file_path
            )));
        }

        let file = File::open(&path)?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?
            .build()?;

        let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(batches)
    }

    #[allow(clippy::type_complexity)]
    fn group_by_partition(
        &self,
        datapoints: &[DataPoint],
        partition_spec: &PartitionSpec,
    ) -> Result<Vec<(BTreeMap<i32, serde_json::Value>, Vec<DataPoint>)>> {
        if partition_spec.fields.is_empty() {
            return Ok(vec![(BTreeMap::new(), datapoints.to_vec())]);
        }

        let mut groups: BTreeMap<String, (BTreeMap<i32, serde_json::Value>, Vec<DataPoint>)> =
            BTreeMap::new();

        for dp in datapoints {
            let mut partition_values = BTreeMap::new();
            for pf in &partition_spec.fields {
                let val = if pf.source_id == 1 {
                    serde_json::Value::Number(dp.timestamp.into())
                } else {
                    let field_name = self
                        .metadata
                        .current_schema()
                        .field_by_id(pf.source_id)
                        .map(|f| f.name.clone())
                        .unwrap_or_default();
                    if let Some(tag_key) = field_name.strip_prefix("tag_") {
                        dp.tags
                            .get(tag_key)
                            .map(|v| serde_json::Value::String(v.clone()))
                            .unwrap_or(serde_json::Value::Null)
                    } else {
                        dp.fields
                            .get(&field_name)
                            .map(|v| match v {
                                FieldValue::Float(f) => serde_json::json!(*f),
                                FieldValue::Integer(i) => serde_json::json!(*i),
                                FieldValue::String(s) => serde_json::json!(s),
                                FieldValue::Boolean(b) => serde_json::json!(*b),
                            })
                            .unwrap_or(serde_json::Value::Null)
                    }
                };
                partition_values.insert(pf.source_id, val);
            }

            let path = partition_spec.partition_path(&partition_values);
            groups
                .entry(path)
                .or_insert_with(|| (partition_values, Vec::new()))
                .1
                .push(dp.clone());
        }

        Ok(groups.into_values().collect())
    }

    fn write_parquet_file(
        &self,
        batch: &RecordBatch,
        partition_data: &BTreeMap<i32, serde_json::Value>,
    ) -> Result<DataFile> {
        let partition_spec = self.metadata.current_partition_spec();
        let partition_path = partition_spec.partition_path(partition_data);

        let data_dir = self.catalog.data_dir(&self.name);
        let dir = if partition_path.is_empty() {
            data_dir
        } else {
            data_dir.join(&partition_path)
        };
        std::fs::create_dir_all(&dir)?;

        let file_uuid = uuid::Uuid::new_v4();
        let file_name = format!("part-{}.parquet", file_uuid);
        let file_path = dir.join(&file_name);

        let file = File::create(&file_path)?;
        let writer_props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::default(),
            ))
            .build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(writer_props))?;
        writer.write(batch)?;
        let _file_meta = writer.finish()?;
        let file_size = std::fs::metadata(&file_path)?.len() as i64;

        let schema = self.metadata.current_schema();
        let stats = collect_column_stats(batch, schema);

        let mut data_file = DataFile::new_parquet(
            file_path.to_string_lossy().to_string(),
            batch.num_rows() as i64,
            file_size,
        );
        data_file.partition = partition_data.clone();
        apply_stats_to_data_file(&stats, &mut data_file);

        Ok(data_file)
    }

    fn write_parquet_file_from_batches(
        &self,
        batches: &[RecordBatch],
        partition_data: &BTreeMap<i32, serde_json::Value>,
        arrow_schema: &arrow::datatypes::SchemaRef,
    ) -> Result<DataFile> {
        let partition_spec = self.metadata.current_partition_spec();
        let partition_path = partition_spec.partition_path(partition_data);

        let data_dir = self.catalog.data_dir(&self.name);
        let dir = if partition_path.is_empty() {
            data_dir
        } else {
            data_dir.join(&partition_path)
        };
        std::fs::create_dir_all(&dir)?;

        let file_uuid = uuid::Uuid::new_v4();
        let file_name = format!("part-{}.parquet", file_uuid);
        let file_path = dir.join(&file_name);

        let file = File::create(&file_path)?;
        let writer_props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::default(),
            ))
            .build();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(writer_props))?;

        let mut total_rows = 0i64;
        for batch in batches {
            total_rows += batch.num_rows() as i64;
            writer.write(batch)?;
        }
        let _file_meta = writer.finish()?;
        let file_size = std::fs::metadata(&file_path)?.len() as i64;

        let mut data_file = DataFile::new_parquet(
            file_path.to_string_lossy().to_string(),
            total_rows,
            file_size,
        );
        data_file.partition = partition_data.clone();

        Ok(data_file)
    }

    pub fn collect_data_files(&self, snapshot: &Snapshot) -> Result<Vec<DataFile>> {
        let manifest_metas = self
            .catalog
            .load_manifest_list(&self.name, snapshot.snapshot_id)?;

        let mut seen = std::collections::HashSet::new();
        let mut data_files = Vec::new();
        for mm in &manifest_metas {
            let entries = self
                .catalog
                .load_manifest_entries(&self.name, mm.manifest_id)?;
            for entry in entries {
                if entry.status != EntryStatus::Deleted
                    && seen.insert(entry.data_file.file_path.clone())
                {
                    data_files.push(entry.data_file);
                }
            }
        }

        Ok(data_files)
    }

    fn collect_protected_snapshot_ids(&self) -> std::collections::HashSet<i64> {
        let mut ids = std::collections::HashSet::new();
        ids.insert(self.metadata.current_snapshot_id);

        for snap in &self.metadata.snapshots {
            if let Some(parent) = snap.parent_snapshot_id {
                ids.insert(parent);
            }
        }

        ids
    }

    pub fn catalog(&self) -> &IcebergCatalog {
        &self.catalog
    }
}

#[derive(Debug)]
pub struct SnapshotDiff {
    pub added_files: i32,
    pub deleted_files: i32,
    pub added_records: i64,
    pub deleted_records: i64,
}

use crate::scan::IcebergScanBuilder;

fn generate_snapshot_id() -> i64 {
    let uuid = uuid::Uuid::new_v4();
    let bytes = uuid.as_bytes();
    let hi = i64::from_be_bytes(bytes[0..8].try_into().expect("8 bytes"));
    hi.abs()
}

fn generate_manifest_id() -> i64 {
    let uuid = uuid::Uuid::new_v4();
    let bytes = uuid.as_bytes();
    let lo = i64::from_be_bytes(bytes[8..16].try_into().expect("8 bytes"));
    lo.abs()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::PartitionSpec;
    use crate::schema::{Field, IcebergType, Schema};
    use tempfile::TempDir;

    fn make_test_schema() -> Schema {
        Schema::new(
            0,
            vec![
                Field {
                    id: 1,
                    name: "timestamp".to_string(),
                    required: true,
                    field_type: IcebergType::Timestamptz,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                Field {
                    id: 2,
                    name: "tag_host".to_string(),
                    required: false,
                    field_type: IcebergType::String,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
                Field {
                    id: 1000,
                    name: "usage".to_string(),
                    required: false,
                    field_type: IcebergType::Double,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
            ],
        )
    }

    fn make_test_table() -> (TempDir, IcebergTable) {
        let dir = TempDir::new().unwrap();
        let catalog = Arc::new(IcebergCatalog::open(dir.path()).unwrap());

        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        catalog.create_table("cpu", schema, spec).unwrap();

        let meta = catalog.load_metadata("cpu").unwrap();
        let table = IcebergTable::new(catalog, "cpu".to_string(), meta);
        (dir, table)
    }

    fn make_test_datapoints(n: usize) -> Vec<DataPoint> {
        (0..n)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.01))
            })
            .collect()
    }

    #[test]
    fn test_table_accessors() {
        let (_dir, table) = make_test_table();
        assert_eq!(table.name(), "cpu");
        assert!(table.current_snapshot().is_none());
        assert_eq!(table.snapshots().len(), 0);
    }

    #[test]
    fn test_append_to_empty_table() {
        let (_dir, mut table) = make_test_table();
        let dps = make_test_datapoints(10);
        table.append(&dps).unwrap();
        assert!(table.current_snapshot().is_some());
        assert_eq!(
            table.current_snapshot().unwrap().summary.operation,
            "append"
        );
    }

    #[test]
    fn test_append_multiple_times() {
        let (_dir, mut table) = make_test_table();
        let dps1 = make_test_datapoints(5);
        let dps2 = make_test_datapoints(5);
        table.append(&dps1).unwrap();
        table.append(&dps2).unwrap();
        assert_eq!(table.snapshots().len(), 2);
    }

    #[test]
    fn test_append_then_read_roundtrip() {
        let (_dir, mut table) = make_test_table();
        let dps = make_test_datapoints(10);
        table.append(&dps).unwrap();

        let snapshot = table.current_snapshot().unwrap();
        let data_files = table.collect_data_files(snapshot).unwrap();
        assert!(!data_files.is_empty());

        let batches = table.read_data_file(&data_files[0]).unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 10);
    }

    #[test]
    fn test_compact() {
        let (_dir, mut table) = make_test_table();

        for _ in 0..3 {
            let dps = make_test_datapoints(5);
            table.append(&dps).unwrap();
        }

        let before_count = table
            .collect_data_files(table.current_snapshot().unwrap())
            .unwrap()
            .len();
        assert!(before_count >= 3);

        table.compact().unwrap();

        let after_count = table
            .collect_data_files(table.current_snapshot().unwrap())
            .unwrap()
            .len();
        assert!(after_count < before_count);
        assert!(after_count >= 1);
    }

    #[test]
    fn test_rollback() {
        let (_dir, mut table) = make_test_table();

        let dps1 = make_test_datapoints(5);
        table.append(&dps1).unwrap();
        let snap1_id = table.current_snapshot().unwrap().snapshot_id;

        let dps2 = make_test_datapoints(5);
        table.append(&dps2).unwrap();

        table.rollback_to_snapshot(snap1_id).unwrap();

        let data_files = table
            .collect_data_files(table.current_snapshot().unwrap())
            .unwrap();
        assert!(!data_files.is_empty());
    }

    #[test]
    fn test_expire_snapshots() {
        let (_dir, mut table) = make_test_table();

        for _ in 0..5 {
            let dps = make_test_datapoints(5);
            table.append(&dps).unwrap();
        }

        let old_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 1_000_000;

        table.expire_snapshots(old_ms, 2).unwrap();
        assert!(table.snapshots().len() >= 2);
    }

    #[test]
    fn test_update_schema() {
        let (_dir, mut table) = make_test_table();
        table
            .update_schema(vec![crate::schema::SchemaChange::AddField {
                parent_id: None,
                name: "idle".to_string(),
                field_type: IcebergType::Double,
                required: false,
                write_default: None,
            }])
            .unwrap();

        assert!(table.schema().field_by_name("idle").is_some());
    }

    #[test]
    fn test_rollback_to_nonexistent_snapshot() {
        let (_dir, mut table) = make_test_table();
        let result = table.rollback_to_snapshot(99999);
        assert!(result.is_err());
    }

    #[test]
    fn test_expire_keeps_min_snapshots() {
        let (_dir, mut table) = make_test_table();

        for _ in 0..5 {
            let dps = make_test_datapoints(5);
            table.append(&dps).unwrap();
        }

        let total = table.snapshots().len();
        table.expire_snapshots(u64::MAX, total as u32).unwrap();
        assert_eq!(table.snapshots().len(), total);
    }

    #[test]
    fn test_schema_evolution_delete_field() {
        let (_dir, mut table) = make_test_table();
        let field_id = table.schema().field_by_name("usage").map(|f| f.id).unwrap();
        table
            .update_schema(vec![crate::schema::SchemaChange::DeleteField { field_id }])
            .unwrap();
        assert!(table.schema().field_by_name("usage").is_none());
    }

    #[test]
    fn test_update_partition_spec() {
        let (_dir, mut table) = make_test_table();
        let new_spec = crate::partition::PartitionSpec::day_partition(1, 1);
        table.update_partition_spec(new_spec).unwrap();
        assert_eq!(table.partition_spec().spec_id, 1);
    }

    #[test]
    fn test_create_branch_and_tag() {
        let (_dir, mut table) = make_test_table();
        let dps = make_test_datapoints(5);
        table.append(&dps).unwrap();
        let snap_id = table.current_snapshot().unwrap().snapshot_id;

        table.create_branch("etl", snap_id).unwrap();
        table.create_tag("v1", snap_id).unwrap();
    }

    #[test]
    fn test_create_branch_invalid_snapshot() {
        let (_dir, table) = make_test_table();
        let result = table.create_branch("bad", 99999);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_tag_invalid_snapshot() {
        let (_dir, table) = make_test_table();
        let result = table.create_tag("bad", 99999);
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_by_id() {
        let (_dir, mut table) = make_test_table();
        let dps = make_test_datapoints(5);
        table.append(&dps).unwrap();
        let snap_id = table.current_snapshot().unwrap().snapshot_id;

        let found = table.snapshot_by_id(snap_id);
        assert!(found.is_some());
        assert_eq!(found.unwrap().snapshot_id, snap_id);

        let not_found = table.snapshot_by_id(99999);
        assert!(not_found.is_none());
    }

    #[test]
    fn test_history() {
        let (_dir, mut table) = make_test_table();
        let dps = make_test_datapoints(5);
        table.append(&dps).unwrap();
        table.append(&dps).unwrap();

        let history = table.history();
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn test_compact_no_snapshot() {
        let (_dir, mut table) = make_test_table();
        let result = table.compact();
        assert!(result.is_ok());
    }

    #[test]
    fn test_append_empty_datapoints() {
        let (_dir, mut table) = make_test_table();
        let result = table.append(&[]);
        assert!(result.is_ok());
    }
}
