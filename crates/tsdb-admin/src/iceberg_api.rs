use crate::utils;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tsdb_arrow::schema::{DataPoint, FieldValue};
use tsdb_iceberg::{
    Field, IcebergCatalog, IcebergTable, IcebergType, PartitionField, PartitionSpec, Schema,
    SchemaChange, Transform,
};

pub struct IcebergApi {
    catalog: Arc<IcebergCatalog>,
}

impl IcebergApi {
    pub fn new(iceberg_dir: PathBuf) -> Result<Self, String> {
        std::fs::create_dir_all(&iceberg_dir).map_err(|e| format!("create iceberg dir: {}", e))?;
        let catalog = IcebergCatalog::open(&iceberg_dir)
            .map_err(|e| format!("open iceberg catalog: {}", e))?;
        Ok(Self {
            catalog: Arc::new(catalog),
        })
    }

    pub fn new_with_catalog(catalog: Arc<IcebergCatalog>) -> Result<Self, String> {
        Ok(Self { catalog })
    }

    pub fn list_tables(&self) -> Result<Vec<TableInfo>, String> {
        let names = self
            .catalog
            .list_tables()
            .map_err(|e| format!("list tables: {}", e))?;
        let mut tables = Vec::new();
        for name in &names {
            match self.catalog.load_table(name) {
                Ok(meta) => {
                    let current_snap = meta.current_snapshot();
                    tables.push(TableInfo {
                        name: name.clone(),
                        format_version: meta.format_version,
                        table_uuid: meta.table_uuid.clone(),
                        location: meta.location.clone(),
                        current_snapshot_id: meta.current_snapshot_id,
                        snapshot_count: meta.snapshots.len() as i32,
                        schema_fields: meta.current_schema().fields.len(),
                        partition_spec: format!("{:?}", meta.current_partition_spec().fields),
                        last_updated_ms: meta.last_updated_ms,
                        total_records: current_snap
                            .and_then(|s| s.summary.total_records)
                            .unwrap_or(0),
                        total_data_files: current_snap
                            .and_then(|s| s.summary.total_data_files)
                            .unwrap_or(0),
                    });
                }
                Err(e) => {
                    tracing::warn!("load table {} failed: {}", name, e);
                }
            }
        }
        Ok(tables)
    }

    pub fn create_table(
        &self,
        name: &str,
        schema_def: &SchemaDefinition,
        partition_type: &str,
    ) -> Result<String, String> {
        utils::validate_name(name)?;
        let schema = schema_def.to_schema()?;
        let partition_spec = match partition_type {
            "day" => PartitionSpec::day_partition(1, 1),
            "hour" => PartitionSpec::new(
                1,
                vec![PartitionField {
                    source_id: 1,
                    field_id: 1000,
                    name: "ts_hour".to_string(),
                    transform: Transform::Hour,
                }],
            ),
            "identity" => {
                let field_id = schema
                    .fields
                    .iter()
                    .find(|f| f.name.starts_with("tag_"))
                    .map(|f| f.id)
                    .unwrap_or(2);
                PartitionSpec::new(
                    1,
                    vec![PartitionField {
                        source_id: field_id,
                        field_id: 1000,
                        name: "tag_identity".to_string(),
                        transform: Transform::Identity,
                    }],
                )
            }
            _ => PartitionSpec::unpartitioned(0),
        };

        self.catalog
            .create_table(name, schema, partition_spec)
            .map_err(|e| format!("create table: {}", e))?;
        Ok(format!("table '{}' created", name))
    }

    pub fn drop_table(&self, name: &str) -> Result<String, String> {
        utils::validate_name(name)?;
        self.catalog
            .drop_table(name)
            .map_err(|e| format!("drop table: {}", e))?;
        Ok(format!("table '{}' dropped", name))
    }

    pub fn table_detail(&self, name: &str) -> Result<TableDetail, String> {
        utils::validate_name(name)?;
        let meta = self
            .catalog
            .load_table(name)
            .map_err(|e| format!("load table: {}", e))?;

        let snapshots: Vec<SnapshotInfo> = meta
            .snapshots
            .iter()
            .map(|s| SnapshotInfo {
                snapshot_id: s.snapshot_id,
                parent_snapshot_id: s.parent_snapshot_id,
                sequence_number: s.sequence_number,
                timestamp_ms: s.timestamp_ms,
                operation: s.summary.operation.clone(),
                added_data_files: s.summary.added_data_files.unwrap_or(0),
                deleted_data_files: s.summary.deleted_data_files.unwrap_or(0),
                added_records: s.summary.added_records.unwrap_or(0),
                deleted_records: s.summary.deleted_records.unwrap_or(0),
                total_data_files: s.summary.total_data_files.unwrap_or(0),
                total_records: s.summary.total_records.unwrap_or(0),
            })
            .collect();

        let schema_history: Vec<SchemaInfo> = meta
            .schemas
            .iter()
            .map(|s| SchemaInfo {
                schema_id: s.schema_id,
                fields: s
                    .fields
                    .iter()
                    .map(|f| FieldInfo {
                        id: f.id,
                        name: f.name.clone(),
                        field_type: format!("{:?}", f.field_type),
                        required: f.required,
                        doc: f.doc.clone(),
                    })
                    .collect(),
            })
            .collect();

        let partition_specs: Vec<PartitionSpecInfo> = meta
            .partition_specs
            .iter()
            .map(|ps| PartitionSpecInfo {
                spec_id: ps.spec_id,
                fields: ps
                    .fields
                    .iter()
                    .map(|pf| PartitionFieldInfo {
                        source_id: pf.source_id,
                        field_id: pf.field_id,
                        transform: format!("{:?}", pf.transform),
                    })
                    .collect(),
            })
            .collect();

        let data_files = if let Some(snap) = meta.current_snapshot() {
            let table = IcebergTable::new(self.catalog.clone(), name.to_string(), meta.clone());
            match table.collect_data_files(snap) {
                Ok(files) => files
                    .iter()
                    .map(|df| DataFileInfo {
                        file_path: df.file_path.clone(),
                        file_format: df.file_format.clone(),
                        record_count: df.record_count,
                        file_size_in_bytes: df.file_size_in_bytes,
                        partition: df
                            .partition
                            .iter()
                            .map(|(k, v)| (format!("field_{}", k), v.clone()))
                            .collect(),
                    })
                    .collect(),
                Err(e) => {
                    tracing::warn!("collect_data_files for {}: {}", name, e);
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };

        Ok(TableDetail {
            name: name.to_string(),
            format_version: meta.format_version,
            table_uuid: meta.table_uuid.clone(),
            location: meta.location.clone(),
            current_snapshot_id: meta.current_snapshot_id,
            current_schema_id: meta.current_schema_id,
            default_spec_id: meta.default_spec_id,
            last_sequence_number: meta.last_sequence_number,
            last_updated_ms: meta.last_updated_ms,
            properties: meta.properties.clone(),
            snapshots,
            schema_history,
            partition_specs,
            data_files,
        })
    }

    pub fn append_data(
        &self,
        table_name: &str,
        datapoints_json: &[serde_json::Value],
    ) -> Result<String, String> {
        utils::validate_name(table_name)?;
        let meta = self
            .catalog
            .load_table(table_name)
            .map_err(|e| format!("load table: {}", e))?;
        let mut table = IcebergTable::new(self.catalog.clone(), table_name.to_string(), meta);

        let total_input = datapoints_json.len();
        let datapoints: Vec<DataPoint> = datapoints_json
            .iter()
            .filter_map(serde_json_to_datapoint)
            .collect();

        if datapoints.is_empty() {
            return Err("no valid datapoints".to_string());
        }

        let skipped = total_input - datapoints.len();
        if skipped > 0 {
            tracing::warn!(
                "append_data: {} of {} datapoints skipped due to parse errors",
                skipped,
                total_input
            );
        }

        table
            .append(&datapoints)
            .map_err(|e| format!("append: {}", e))?;

        Ok(format!(
            "appended {} points to '{}'",
            datapoints.len(),
            table_name
        ))
    }

    pub fn scan_table(&self, table_name: &str, limit: Option<usize>) -> Result<ScanResult, String> {
        utils::validate_name(table_name)?;
        let limit = limit.unwrap_or(10000).min(10000);
        let meta = self
            .catalog
            .load_table(table_name)
            .map_err(|e| format!("load table: {}", e))?;
        let table = IcebergTable::new(self.catalog.clone(), table_name.to_string(), meta);

        let scan = table
            .scan()
            .build()
            .map_err(|e| format!("build scan: {}", e))?;

        let batches = scan.execute().map_err(|e| format!("execute scan: {}", e))?;

        let mut columns: Vec<String> = Vec::new();
        let mut rows: Vec<serde_json::Value> = Vec::new();
        let mut total_rows = 0usize;

        for batch in &batches {
            if columns.is_empty() {
                columns = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect();
            }

            for i in 0..batch.num_rows() {
                if total_rows >= limit {
                    break;
                }
                let mut row = serde_json::Map::new();
                for (col_idx, col_name) in columns.iter().enumerate() {
                    if col_idx < batch.num_columns() {
                        let val = arrow_value_to_json(batch.column(col_idx), i);
                        row.insert(col_name.clone(), val);
                    }
                }
                rows.push(serde_json::Value::Object(row));
                total_rows += 1;
            }
            if total_rows >= limit {
                break;
            }
        }

        Ok(ScanResult {
            table: table_name.to_string(),
            columns,
            rows,
            total_rows,
        })
    }

    pub fn snapshots(&self, table_name: &str) -> Result<Vec<SnapshotInfo>, String> {
        utils::validate_name(table_name)?;
        let meta = self
            .catalog
            .load_table(table_name)
            .map_err(|e| format!("load table: {}", e))?;

        Ok(meta
            .snapshots
            .iter()
            .map(|s| SnapshotInfo {
                snapshot_id: s.snapshot_id,
                parent_snapshot_id: s.parent_snapshot_id,
                sequence_number: s.sequence_number,
                timestamp_ms: s.timestamp_ms,
                operation: s.summary.operation.clone(),
                added_data_files: s.summary.added_data_files.unwrap_or(0),
                deleted_data_files: s.summary.deleted_data_files.unwrap_or(0),
                added_records: s.summary.added_records.unwrap_or(0),
                deleted_records: s.summary.deleted_records.unwrap_or(0),
                total_data_files: s.summary.total_data_files.unwrap_or(0),
                total_records: s.summary.total_records.unwrap_or(0),
            })
            .collect())
    }

    pub fn rollback(&self, table_name: &str, snapshot_id: i64) -> Result<String, String> {
        utils::validate_name(table_name)?;
        let meta = self
            .catalog
            .load_table(table_name)
            .map_err(|e| format!("load table: {}", e))?;
        let mut table = IcebergTable::new(self.catalog.clone(), table_name.to_string(), meta);
        table
            .rollback_to_snapshot(snapshot_id)
            .map_err(|e| format!("rollback: {}", e))?;
        Ok(format!(
            "rolled back '{}' to snapshot {}",
            table_name, snapshot_id
        ))
    }

    pub fn compact(&self, table_name: &str) -> Result<String, String> {
        utils::validate_name(table_name)?;
        let meta = self
            .catalog
            .load_table(table_name)
            .map_err(|e| format!("load table: {}", e))?;
        let mut table = IcebergTable::new(self.catalog.clone(), table_name.to_string(), meta);
        table.compact().map_err(|e| format!("compact: {}", e))?;
        Ok(format!("compacted table '{}'", table_name))
    }

    pub fn expire_snapshots(&self, table_name: &str, keep_days: u64) -> Result<String, String> {
        utils::validate_name(table_name)?;
        if keep_days == 0 {
            return Err("keep_days must be at least 1".to_string());
        }
        let meta = self
            .catalog
            .load_table(table_name)
            .map_err(|e| format!("load table: {}", e))?;
        let mut table = IcebergTable::new(self.catalog.clone(), table_name.to_string(), meta);
        let older_than_ms = tsdb_iceberg::util::now_ms() - keep_days * 86_400_000;
        table
            .expire_snapshots(older_than_ms, 2)
            .map_err(|e| format!("expire: {}", e))?;
        Ok(format!(
            "expired snapshots older than {} days for '{}'",
            keep_days, table_name
        ))
    }

    pub fn update_schema(
        &self,
        table_name: &str,
        changes: Vec<SchemaChange>,
    ) -> Result<String, String> {
        utils::validate_name(table_name)?;
        let meta = self
            .catalog
            .load_table(table_name)
            .map_err(|e| format!("load table: {}", e))?;
        let mut table = IcebergTable::new(self.catalog.clone(), table_name.to_string(), meta);
        table
            .update_schema(changes)
            .map_err(|e| format!("update schema: {}", e))?;
        Ok(format!("schema updated for '{}'", table_name))
    }

    pub fn catalog_path(&self) -> String {
        self.catalog.base_dir().to_string_lossy().to_string()
    }
}

fn serde_json_to_datapoint(v: &serde_json::Value) -> Option<DataPoint> {
    let obj = v.as_object()?;
    let measurement = obj.get("measurement")?.as_str()?.to_string();
    let timestamp = obj.get("timestamp")?.as_i64()?;

    let mut dp = DataPoint::new(&measurement, timestamp);

    if let Some(tags) = obj.get("tags").and_then(|t| t.as_object()) {
        for (k, v) in tags {
            if let Some(s) = v.as_str() {
                dp = dp.with_tag(k, s);
            }
        }
    }

    if let Some(fields) = obj.get("fields").and_then(|f| f.as_object()) {
        for (k, v) in fields {
            let fv = if let Some(f) = v.as_f64() {
                FieldValue::Float(f)
            } else if let Some(i) = v.as_i64() {
                FieldValue::Integer(i)
            } else if let Some(s) = v.as_str() {
                FieldValue::String(s.to_string())
            } else if let Some(b) = v.as_bool() {
                FieldValue::Boolean(b)
            } else {
                continue;
            };
            dp = dp.with_field(k, fv);
        }
    }

    Some(dp)
}

fn arrow_value_to_json(col: &arrow::array::ArrayRef, idx: usize) -> serde_json::Value {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if col.is_null(idx) {
        return serde_json::Value::Null;
    }

    match col.data_type() {
        DataType::Boolean => {
            if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                serde_json::Value::Bool(arr.value(idx))
            } else {
                serde_json::Value::Null
            }
        }
        DataType::Int32 => {
            if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                serde_json::json!(arr.value(idx))
            } else {
                serde_json::Value::Null
            }
        }
        DataType::Int64 => {
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                serde_json::json!(arr.value(idx))
            } else {
                serde_json::Value::Null
            }
        }
        DataType::Float32 => {
            if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
                serde_json::json!(arr.value(idx) as f64)
            } else {
                serde_json::Value::Null
            }
        }
        DataType::Float64 => {
            if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                serde_json::json!(arr.value(idx))
            } else {
                serde_json::Value::Null
            }
        }
        DataType::Utf8 => {
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                serde_json::json!(arr.value(idx))
            } else {
                serde_json::Value::Null
            }
        }
        DataType::Timestamp(_, _) => {
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                serde_json::json!(arr.value(idx))
            } else {
                serde_json::Value::Null
            }
        }
        DataType::Date32 => {
            if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                serde_json::json!(arr.value(idx))
            } else {
                serde_json::Value::Null
            }
        }
        _ => serde_json::Value::Null,
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub format_version: u32,
    pub table_uuid: String,
    pub location: String,
    pub current_snapshot_id: i64,
    pub snapshot_count: i32,
    pub schema_fields: usize,
    pub partition_spec: String,
    pub last_updated_ms: u64,
    pub total_records: i64,
    pub total_data_files: i32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableDetail {
    pub name: String,
    pub format_version: u32,
    pub table_uuid: String,
    pub location: String,
    pub current_snapshot_id: i64,
    pub current_schema_id: i32,
    pub default_spec_id: i32,
    pub last_sequence_number: u64,
    pub last_updated_ms: u64,
    pub properties: BTreeMap<String, String>,
    pub snapshots: Vec<SnapshotInfo>,
    pub schema_history: Vec<SchemaInfo>,
    pub partition_specs: Vec<PartitionSpecInfo>,
    pub data_files: Vec<DataFileInfo>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotInfo {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub sequence_number: u64,
    pub timestamp_ms: u64,
    pub operation: String,
    pub added_data_files: i32,
    pub deleted_data_files: i32,
    pub added_records: i64,
    pub deleted_records: i64,
    pub total_data_files: i32,
    pub total_records: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SchemaInfo {
    pub schema_id: i32,
    pub fields: Vec<FieldInfo>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldInfo {
    pub id: i32,
    pub name: String,
    pub field_type: String,
    pub required: bool,
    pub doc: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionSpecInfo {
    pub spec_id: i32,
    pub fields: Vec<PartitionFieldInfo>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionFieldInfo {
    pub source_id: i32,
    pub field_id: i32,
    pub transform: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataFileInfo {
    pub file_path: String,
    pub file_format: String,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub partition: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScanResult {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
    pub total_rows: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SchemaDefinition {
    pub fields: Vec<FieldDefinition>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldDefinition {
    pub name: String,
    pub field_type: String,
    pub required: bool,
}

impl SchemaDefinition {
    pub fn to_schema(&self) -> Result<Schema, String> {
        let mut fields = Vec::new();
        let mut col_id: i32 = 1;

        fields.push(Field {
            id: col_id,
            name: "timestamp".to_string(),
            required: true,
            field_type: IcebergType::Timestamptz,
            doc: None,
            initial_default: None,
            write_default: None,
        });
        col_id += 1;

        for fd in &self.fields {
            if fd.name.starts_with("tag_") {
                fields.push(Field {
                    id: col_id,
                    name: fd.name.clone(),
                    required: fd.required,
                    field_type: IcebergType::String,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                });
                col_id += 1;
            } else {
                let field_type = match fd.field_type.as_str() {
                    "double" | "float" => IcebergType::Double,
                    "long" | "int" => IcebergType::Long,
                    "string" => IcebergType::String,
                    "boolean" | "bool" => IcebergType::Boolean,
                    "timestamp" | "timestamptz" => IcebergType::Timestamptz,
                    _ => IcebergType::String,
                };
                let id = if col_id < 1000 && !fd.name.starts_with("tag_") {
                    let id = 1000;
                    col_id = 1001;
                    id
                } else {
                    let id = col_id;
                    col_id += 1;
                    id
                };
                fields.push(Field {
                    id,
                    name: fd.name.clone(),
                    required: fd.required,
                    field_type,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                });
            }
        }

        Ok(Schema::new(0, fields))
    }
}
