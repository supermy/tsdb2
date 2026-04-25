use rocksdb::{BoundColumnFamily, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{IcebergError, Result};
use crate::manifest::{ManifestEntry, ManifestMeta};
use crate::partition::PartitionSpec;
use crate::schema::Schema;
use crate::snapshot::{Ref, RefType, Snapshot, SnapshotLogEntry};

pub const CATALOG_CF: &str = "_catalog";
pub const TABLE_META_CF: &str = "_table_meta";
pub const SNAPSHOT_CF: &str = "_snapshot";
pub const MANIFEST_LIST_CF: &str = "_manifest_list";
pub const MANIFEST_CF: &str = "_manifest";
pub const REFS_CF: &str = "_refs";
pub const SEQ_CF: &str = "_seq";

const CF_NAMES: [&str; 7] = [
    CATALOG_CF,
    TABLE_META_CF,
    SNAPSHOT_CF,
    MANIFEST_LIST_CF,
    MANIFEST_CF,
    REFS_CF,
    SEQ_CF,
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableMetadata {
    pub format_version: u32,
    pub table_uuid: String,
    pub location: String,
    pub last_sequence_number: u64,
    pub last_updated_ms: u64,
    pub last_column_id: i32,
    pub current_schema_id: i32,
    pub schemas: Vec<Schema>,
    pub default_spec_id: i32,
    pub partition_specs: Vec<PartitionSpec>,
    pub current_snapshot_id: i64,
    pub snapshots: Vec<Snapshot>,
    pub snapshot_log: Vec<SnapshotLogEntry>,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

impl TableMetadata {
    pub fn new(location: String, schema: Schema, partition_spec: PartitionSpec) -> Self {
        let last_column_id = schema.highest_field_id();
        let table_uuid = uuid::Uuid::new_v4().to_string();
        Self {
            format_version: 2,
            table_uuid,
            location,
            last_sequence_number: 0,
            last_updated_ms: now_ms(),
            last_column_id,
            current_schema_id: schema.schema_id,
            schemas: vec![schema],
            default_spec_id: partition_spec.spec_id,
            partition_specs: vec![partition_spec],
            current_snapshot_id: -1,
            snapshots: Vec::new(),
            snapshot_log: Vec::new(),
            properties: BTreeMap::new(),
        }
    }

    pub fn current_schema(&self) -> &Schema {
        self.schemas
            .iter()
            .find(|s| s.schema_id == self.current_schema_id)
            .or_else(|| self.schemas.last())
            .expect("table metadata must contain at least one schema")
    }

    pub fn current_partition_spec(&self) -> &PartitionSpec {
        self.partition_specs
            .iter()
            .find(|ps| ps.spec_id == self.default_spec_id)
            .or_else(|| self.partition_specs.last())
            .expect("table metadata must contain at least one partition spec")
    }

    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        if self.current_snapshot_id < 0 {
            return None;
        }
        self.snapshots
            .iter()
            .find(|s| s.snapshot_id == self.current_snapshot_id)
    }

    pub fn snapshot_by_id(&self, id: i64) -> Option<&Snapshot> {
        self.snapshots.iter().find(|s| s.snapshot_id == id)
    }

    pub fn snapshot_by_timestamp(&self, ts_ms: u64) -> Option<&Snapshot> {
        self.snapshots
            .iter()
            .filter(|s| s.timestamp_ms <= ts_ms)
            .max_by_key(|s| s.timestamp_ms)
    }
}

pub struct IcebergCatalog {
    db: DB,
    base_dir: PathBuf,
    seq_counter: AtomicU64,
}

impl std::fmt::Debug for IcebergCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergCatalog")
            .field("base_dir", &self.base_dir)
            .finish()
    }
}

impl IcebergCatalog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let base_dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_dir)?;

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptors: Vec<rocksdb::ColumnFamilyDescriptor> = CF_NAMES
            .iter()
            .map(|&name| rocksdb::ColumnFamilyDescriptor::new(name, Options::default()))
            .collect();

        let db = DB::open_cf_descriptors(&opts, &base_dir, cf_descriptors)?;

        let seq_counter = {
            let seq_cf = db.cf_handle(SEQ_CF).expect("seq cf must exist");
            let saved: u64 = db
                .get_cf(&seq_cf, b"seq_counter")
                .ok()
                .flatten()
                .and_then(|v| serde_json::from_slice(&v).ok())
                .unwrap_or(0);
            AtomicU64::new(saved)
        };

        Ok(Self {
            db,
            base_dir,
            seq_counter,
        })
    }

    pub fn create_table(
        &self,
        name: &str,
        schema: Schema,
        partition_spec: PartitionSpec,
    ) -> Result<()> {
        let catalog_cf = self.cf(CATALOG_CF)?;

        let existing = self.db.get_cf(&catalog_cf, name)?;
        if existing.is_some() {
            return Err(IcebergError::TableAlreadyExists(name.to_string()));
        }

        let table_dir = self.base_dir.join("data").join(name);
        std::fs::create_dir_all(&table_dir)?;

        let metadata = TableMetadata::new(
            table_dir.to_string_lossy().to_string(),
            schema,
            partition_spec,
        );

        let meta_json = serde_json::to_vec(&metadata)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&catalog_cf, name, &meta_json);
        batch.put_cf(&self.cf(TABLE_META_CF)?, meta_key(name), &meta_json);

        let main_ref = Ref {
            snapshot_id: -1,
            ref_type: RefType::Main,
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        };
        let ref_json = serde_json::to_vec(&main_ref)?;
        batch.put_cf(&self.cf(REFS_CF)?, ref_key(name, "main"), &ref_json);

        self.db.write(batch)?;
        Ok(())
    }

    pub fn load_table(&self, name: &str) -> Result<TableMetadata> {
        self.load_metadata(name)
    }

    pub fn drop_table(&self, name: &str) -> Result<()> {
        let catalog_cf = self.cf(CATALOG_CF)?;
        let existing = self.db.get_cf(&catalog_cf, name)?;
        if existing.is_none() {
            return Err(IcebergError::TableNotFound(name.to_string()));
        }

        let mut batch = WriteBatch::default();
        batch.delete_cf(&catalog_cf, name);
        batch.delete_cf(&self.cf(TABLE_META_CF)?, meta_key(name));

        let prefix = format!("{}\x00", name);
        for cf_name in &[SNAPSHOT_CF, MANIFEST_LIST_CF, MANIFEST_CF, REFS_CF] {
            let cf = self.cf(cf_name)?;
            let iter = self.db.prefix_iterator_cf(&cf, prefix.as_bytes());
            for item in iter {
                let (key, _) = item?;
                batch.delete_cf(&cf, &key);
            }
        }

        self.db.write(batch)?;
        Ok(())
    }

    pub fn list_tables(&self) -> Result<Vec<String>> {
        let catalog_cf = self.cf(CATALOG_CF)?;
        let iter = self
            .db
            .iterator_cf(&catalog_cf, rocksdb::IteratorMode::Start);
        let mut tables = Vec::new();
        for item in iter {
            let (key, _) = item?;
            tables.push(String::from_utf8_lossy(&key).to_string());
        }
        Ok(tables)
    }

    pub fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()> {
        let catalog_cf = self.cf(CATALOG_CF)?;

        let existing = self.db.get_cf(&catalog_cf, old_name)?;
        if existing.is_none() {
            return Err(IcebergError::TableNotFound(old_name.to_string()));
        }

        let target = self.db.get_cf(&catalog_cf, new_name)?;
        if target.is_some() {
            return Err(IcebergError::TableAlreadyExists(new_name.to_string()));
        }

        let meta = self.load_metadata(old_name)?;
        let meta_json = serde_json::to_vec(&meta)?;

        let mut batch = WriteBatch::default();
        batch.delete_cf(&catalog_cf, old_name);
        batch.put_cf(&catalog_cf, new_name, &meta_json);
        batch.delete_cf(&self.cf(TABLE_META_CF)?, meta_key(old_name));
        batch.put_cf(&self.cf(TABLE_META_CF)?, meta_key(new_name), &meta_json);

        let old_prefix = format!("{}\x00", old_name);
        let new_prefix = format!("{}\x00", new_name);

        for cf_name in &[SNAPSHOT_CF, MANIFEST_LIST_CF, MANIFEST_CF, REFS_CF, SEQ_CF] {
            let cf = self.cf(cf_name)?;
            let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
            for item in iter {
                let (key, value) = item?;
                let key_str = String::from_utf8_lossy(&key);
                if key_str.starts_with(&old_prefix) {
                    let suffix = &key_str[old_prefix.len()..];
                    let new_key = format!("{}{}", new_prefix, suffix);
                    batch.delete_cf(&cf, &key);
                    batch.put_cf(&cf, new_key, &value);
                }
            }
        }

        self.db.write(batch)?;
        Ok(())
    }

    pub fn load_metadata(&self, table_name: &str) -> Result<TableMetadata> {
        let meta_cf = self.cf(TABLE_META_CF)?;
        let key = meta_key(table_name);
        let bytes = self
            .db
            .get_cf(&meta_cf, &key)?
            .ok_or(IcebergError::TableNotFound(table_name.to_string()))?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub fn save_metadata(&self, table_name: &str, metadata: &TableMetadata) -> Result<()> {
        let meta_cf = self.cf(TABLE_META_CF)?;
        let catalog_cf = self.cf(CATALOG_CF)?;
        let key = meta_key(table_name);
        let meta_json = serde_json::to_vec(metadata)?;

        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, &key, &meta_json);
        batch.put_cf(&catalog_cf, table_name, &meta_json);

        self.db.write(batch)?;
        Ok(())
    }

    pub fn next_sequence_number(&self) -> u64 {
        let seq = self.seq_counter.fetch_add(1, Ordering::SeqCst) + 1;
        if let Ok(seq_cf) = self.cf(SEQ_CF) {
            let val = serde_json::to_vec(&seq).unwrap_or_default();
            let _ = self.db.put_cf(&seq_cf, b"seq_counter", &val);
        }
        seq
    }

    pub fn save_snapshot(&self, table_name: &str, snapshot: &Snapshot) -> Result<()> {
        let cf = self.cf(SNAPSHOT_CF)?;
        let key = snapshot_key(table_name, snapshot.snapshot_id);
        let json = serde_json::to_vec(snapshot)?;
        self.db.put_cf(&cf, &key, &json)?;
        Ok(())
    }

    pub fn load_snapshot(&self, table_name: &str, snapshot_id: i64) -> Result<Snapshot> {
        let cf = self.cf(SNAPSHOT_CF)?;
        let key = snapshot_key(table_name, snapshot_id);
        let bytes = self
            .db
            .get_cf(&cf, &key)?
            .ok_or(IcebergError::SnapshotNotFound(snapshot_id))?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub fn save_manifest_list(
        &self,
        table_name: &str,
        snapshot_id: i64,
        manifests: &[ManifestMeta],
    ) -> Result<()> {
        let cf = self.cf(MANIFEST_LIST_CF)?;
        for (seq, manifest) in manifests.iter().enumerate() {
            let key = manifest_list_key(table_name, snapshot_id, seq as i64);
            let json = serde_json::to_vec(manifest)?;
            self.db.put_cf(&cf, &key, &json)?;
        }
        Ok(())
    }

    pub fn load_manifest_list(
        &self,
        table_name: &str,
        snapshot_id: i64,
    ) -> Result<Vec<ManifestMeta>> {
        let cf = self.cf(MANIFEST_LIST_CF)?;
        let prefix = format!("{}\x00{}\x00", table_name, snapshot_id);
        let iter = self.db.prefix_iterator_cf(&cf, prefix.as_bytes());
        let mut manifests = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            manifests.push(serde_json::from_slice(&value)?);
        }
        Ok(manifests)
    }

    pub fn save_manifest_entries(
        &self,
        table_name: &str,
        manifest_id: i64,
        entries: &[ManifestEntry],
    ) -> Result<()> {
        let cf = self.cf(MANIFEST_CF)?;
        for (idx, entry) in entries.iter().enumerate() {
            let key = manifest_entry_key(table_name, manifest_id, idx as i64);
            let json = serde_json::to_vec(entry)?;
            self.db.put_cf(&cf, &key, &json)?;
        }
        Ok(())
    }

    pub fn load_manifest_entries(
        &self,
        table_name: &str,
        manifest_id: i64,
    ) -> Result<Vec<ManifestEntry>> {
        let cf = self.cf(MANIFEST_CF)?;
        let prefix = format!("{}\x00{}\x00", table_name, manifest_id);
        let iter = self.db.prefix_iterator_cf(&cf, prefix.as_bytes());
        let mut entries = Vec::new();
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            entries.push(serde_json::from_slice(&value)?);
        }
        Ok(entries)
    }

    pub fn save_ref(&self, table_name: &str, ref_name: &str, r: &Ref) -> Result<()> {
        let cf = self.cf(REFS_CF)?;
        let key = ref_key(table_name, ref_name);
        let json = serde_json::to_vec(r)?;
        self.db.put_cf(&cf, &key, &json)?;
        Ok(())
    }

    pub fn load_ref(&self, table_name: &str, ref_name: &str) -> Result<Ref> {
        let cf = self.cf(REFS_CF)?;
        let key = ref_key(table_name, ref_name);
        let bytes = self
            .db
            .get_cf(&cf, &key)?
            .ok_or_else(|| IcebergError::Catalog(format!("ref not found: {}", ref_name)))?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.db.write(batch)?;
        Ok(())
    }

    pub fn delete_snapshot_data(&self, table_name: &str, snapshot_id: i64) -> Result<()> {
        let mut batch = WriteBatch::default();

        let snap_cf = self.cf(SNAPSHOT_CF)?;
        let snap_key = format!("{}\x00snap\x00{}", table_name, snapshot_id);
        batch.delete_cf(&snap_cf, snap_key.as_bytes());

        let ml_cf = self.cf(MANIFEST_LIST_CF)?;
        let ml_prefix = format!("{}\x00{}\x00", table_name, snapshot_id);
        let ml_iter = self.db.prefix_iterator_cf(&ml_cf, ml_prefix.as_bytes());
        for item in ml_iter {
            let (key, value) = item?;
            if !key.starts_with(ml_prefix.as_bytes()) {
                break;
            }
            batch.delete_cf(&ml_cf, &key);

            if let Ok(mm) = serde_json::from_slice::<ManifestMeta>(&value) {
                let m_cf = self.cf(MANIFEST_CF)?;
                let m_prefix = format!("{}\x00{}\x00", table_name, mm.manifest_id);
                let m_iter = self.db.prefix_iterator_cf(&m_cf, m_prefix.as_bytes());
                for m_item in m_iter {
                    let (m_key, _) = m_item?;
                    if !m_key.starts_with(m_prefix.as_bytes()) {
                        break;
                    }
                    batch.delete_cf(&m_cf, &m_key);
                }
            }
        }

        self.db.write(batch)?;
        Ok(())
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn data_dir(&self, table_name: &str) -> PathBuf {
        self.base_dir.join("data").join(table_name)
    }

    pub fn cf(&self, name: &str) -> Result<Arc<BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(name)
            .ok_or(IcebergError::Catalog(format!("CF not found: {}", name)))
    }
}

fn meta_key(table: &str) -> Vec<u8> {
    format!("{}\x00meta", table).into_bytes()
}

fn snapshot_key(table: &str, snapshot_id: i64) -> Vec<u8> {
    format!("{}\x00snap\x00{}", table, snapshot_id).into_bytes()
}

fn manifest_list_key(table: &str, snapshot_id: i64, seq: i64) -> Vec<u8> {
    format!("{}\x00{}\x00{}", table, snapshot_id, seq).into_bytes()
}

fn manifest_entry_key(table: &str, manifest_id: i64, file_idx: i64) -> Vec<u8> {
    format!("{}\x00{}\x00{}", table, manifest_id, file_idx).into_bytes()
}

fn ref_key(table: &str, ref_name: &str) -> Vec<u8> {
    format!("{}\x00{}", table, ref_name).into_bytes()
}

use crate::util::now_ms;

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
                    id: 1000,
                    name: "value".to_string(),
                    required: false,
                    field_type: IcebergType::Double,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
            ],
        )
    }

    #[test]
    fn test_catalog_create_load_table() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();

        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        catalog.create_table("cpu", schema, spec).unwrap();

        let meta = catalog.load_table("cpu").unwrap();
        assert_eq!(meta.format_version, 2);
        assert_eq!(meta.current_snapshot_id, -1);
    }

    #[test]
    fn test_catalog_list_tables() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();

        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        catalog
            .create_table("cpu", schema.clone(), spec.clone())
            .unwrap();
        catalog.create_table("mem", schema, spec).unwrap();

        let tables = catalog.list_tables().unwrap();
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn test_catalog_drop_table() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();

        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        catalog.create_table("cpu", schema, spec).unwrap();
        catalog.drop_table("cpu").unwrap();

        assert!(catalog.load_table("cpu").is_err());
    }

    #[test]
    fn test_catalog_rename_table() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();

        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        catalog.create_table("cpu", schema, spec).unwrap();
        catalog.rename_table("cpu", "memory").unwrap();

        assert!(catalog.load_table("cpu").is_err());
        assert!(catalog.load_table("memory").is_ok());
    }

    #[test]
    fn test_catalog_persistence() {
        let dir = TempDir::new().unwrap();
        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);

        {
            let catalog = IcebergCatalog::open(dir.path()).unwrap();
            catalog.create_table("cpu", schema, spec).unwrap();
        }

        {
            let catalog = IcebergCatalog::open(dir.path()).unwrap();
            let meta = catalog.load_table("cpu").unwrap();
            assert_eq!(meta.format_version, 2);
        }
    }

    #[test]
    fn test_table_metadata_new() {
        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        let meta = TableMetadata::new("/data/cpu".to_string(), schema, spec);
        assert_eq!(meta.current_snapshot_id, -1);
        assert_eq!(meta.schemas.len(), 1);
        assert_eq!(meta.partition_specs.len(), 1);
    }

    #[test]
    fn test_table_metadata_serde_roundtrip() {
        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        let meta = TableMetadata::new("/data/cpu".to_string(), schema, spec);
        let json = serde_json::to_string(&meta).unwrap();
        let restored: TableMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, restored);
    }

    #[test]
    fn test_catalog_create_table_already_exists() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();

        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        catalog
            .create_table("cpu", schema.clone(), spec.clone())
            .unwrap();
        let result = catalog.create_table("cpu", schema, spec);
        assert!(result.is_err());
    }

    #[test]
    fn test_catalog_load_table_not_found() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();
        let result = catalog.load_table("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_catalog_drop_table_not_found() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();
        let result = catalog.drop_table("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_catalog_rename_table_source_not_found() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();
        let result = catalog.rename_table("nonexistent", "new_name");
        assert!(result.is_err());
    }

    #[test]
    fn test_catalog_list_tables_empty() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();
        let tables = catalog.list_tables().unwrap();
        assert!(tables.is_empty());
    }

    #[test]
    fn test_catalog_next_sequence_number() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();

        let seq1 = catalog.next_sequence_number();
        let seq2 = catalog.next_sequence_number();
        let seq3 = catalog.next_sequence_number();

        assert!(seq1 < seq2);
        assert!(seq2 < seq3);
    }

    #[test]
    fn test_catalog_save_and_load_snapshot() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();

        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        catalog.create_table("cpu", schema, spec).unwrap();

        let snap = crate::snapshot::Snapshot::new_append(1, None, 1, 0);
        catalog.save_snapshot("cpu", &snap).unwrap();

        let loaded = catalog.load_snapshot("cpu", 1).unwrap();
        assert_eq!(loaded.snapshot_id, 1);
    }

    #[test]
    fn test_catalog_load_snapshot_not_found() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();

        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        catalog.create_table("cpu", schema, spec).unwrap();

        let result = catalog.load_snapshot("cpu", 999);
        assert!(result.is_err());
    }

    #[test]
    fn test_catalog_save_and_load_ref() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();

        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        catalog.create_table("cpu", schema, spec).unwrap();

        let r = crate::snapshot::Ref {
            snapshot_id: 1,
            ref_type: crate::snapshot::RefType::Branch,
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        };
        catalog.save_ref("cpu", "main", &r).unwrap();

        let loaded = catalog.load_ref("cpu", "main").unwrap();
        assert_eq!(loaded.snapshot_id, 1);
    }

    #[test]
    fn test_catalog_base_dir_and_data_dir() {
        let dir = TempDir::new().unwrap();
        let catalog = IcebergCatalog::open(dir.path()).unwrap();

        assert!(catalog.base_dir().exists());
        assert!(catalog
            .data_dir("test_table")
            .starts_with(catalog.base_dir()));
    }
}
