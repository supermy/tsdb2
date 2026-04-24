use arrow::array::*;
use arrow::compute::{filter_record_batch, not as arrow_not};
use arrow::datatypes::{DataType, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use std::path::PathBuf;

use crate::error::{IcebergError, Result};
use crate::manifest::{DataFile, EntryStatus, ManifestMeta};
use crate::schema::Schema;
use crate::snapshot::Snapshot;
use crate::table::IcebergTable;

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    AlwaysTrue,
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    Eq(i32, PredicateValue),
    NotEq(i32, PredicateValue),
    Gt(i32, PredicateValue),
    GtEq(i32, PredicateValue),
    Lt(i32, PredicateValue),
    LtEq(i32, PredicateValue),
    InList(i32, Vec<PredicateValue>),
    IsNull(i32),
    IsNotNull(i32),
}

#[derive(Debug, Clone, PartialEq)]
pub enum PredicateValue {
    Long(i64),
    Double(f64),
    String(String),
    Boolean(bool),
}

pub struct IcebergScanBuilder<'a> {
    table: &'a IcebergTable,
    snapshot_id: Option<i64>,
    predicate: Option<Predicate>,
    projection: Option<Vec<i32>>,
    case_sensitive: bool,
    schema: Schema,
}

impl<'a> IcebergScanBuilder<'a> {
    pub fn new(table: &'a IcebergTable) -> Self {
        let schema = table.schema().clone();
        Self {
            table,
            snapshot_id: None,
            predicate: None,
            projection: None,
            case_sensitive: true,
            schema,
        }
    }

    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    pub fn predicate(mut self, pred: Predicate) -> Self {
        self.predicate = Some(pred);
        self
    }

    pub fn projection(mut self, field_ids: Vec<i32>) -> Self {
        self.projection = Some(field_ids);
        self
    }

    pub fn case_insensitive(mut self) -> Self {
        self.case_sensitive = false;
        self
    }

    pub fn build(self) -> Result<IcebergScan> {
        let snapshot_id = self.snapshot_id.unwrap_or_else(|| {
            self.table
                .current_snapshot()
                .map(|s| s.snapshot_id)
                .unwrap_or(-1)
        });

        if snapshot_id < 0 {
            return Ok(IcebergScan {
                data_files: Vec::new(),
                predicate: self.predicate,
                projection: self.projection,
                schema: self.schema,
            });
        }

        let snapshot = self
            .table
            .snapshot_by_id(snapshot_id)
            .ok_or(IcebergError::SnapshotNotFound(snapshot_id))?
            .clone();

        let data_files = self.plan_files(&snapshot)?;

        Ok(IcebergScan {
            data_files,
            predicate: self.predicate,
            projection: self.projection,
            schema: self.schema,
        })
    }

    fn plan_files(&self, snapshot: &Snapshot) -> Result<Vec<DataFile>> {
        let manifest_metas = self
            .table
            .catalog()
            .load_manifest_list(self.table.name(), snapshot.snapshot_id)?;

        let mut result = Vec::new();

        for mm in &manifest_metas {
            if let Some(ref pred) = self.predicate {
                if !manifest_matches_predicate(mm, pred) {
                    continue;
                }
            }

            let entries = self
                .table
                .catalog()
                .load_manifest_entries(self.table.name(), mm.manifest_id)?;

            for entry in entries {
                if entry.status == EntryStatus::Deleted {
                    continue;
                }

                if let Some(ref pred) = self.predicate {
                    if !file_matches_predicate(&entry.data_file, pred) {
                        continue;
                    }
                }

                result.push(entry.data_file.clone());
            }
        }

        Ok(result)
    }
}

pub struct IcebergScan {
    data_files: Vec<DataFile>,
    predicate: Option<Predicate>,
    projection: Option<Vec<i32>>,
    schema: Schema,
}

impl IcebergScan {
    pub fn plan(&self) -> &[DataFile] {
        &self.data_files
    }

    pub fn execute(&self) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        for df in &self.data_files {
            let path = PathBuf::from(&df.file_path);
            if !path.exists() {
                return Err(IcebergError::Internal(format!(
                    "data file not found: {}",
                    df.file_path
                )));
            }

            let file = std::fs::File::open(&path)?;
            let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;

            let reader = builder.build()?;

            for batch_result in reader {
                let batch = batch_result?;

                if let Some(ref pred) = self.predicate {
                    if let Some(filtered) = row_filter(&batch, pred, &self.schema) {
                        if filtered.num_rows() > 0 {
                            batches.push(filtered);
                        }
                    }
                } else {
                    batches.push(batch);
                }
            }
        }
        Ok(batches)
    }

    pub fn to_record_batches(&self) -> Result<Vec<RecordBatch>> {
        self.execute()
    }

    pub fn data_files(&self) -> &[DataFile] {
        &self.data_files
    }

    pub fn projection(&self) -> Option<&[i32]> {
        self.projection.as_deref()
    }
}

fn manifest_matches_predicate(mm: &ManifestMeta, _pred: &Predicate) -> bool {
    if mm.partitions_summary.is_none() {
        return true;
    }
    true
}

fn file_matches_predicate(df: &DataFile, pred: &Predicate) -> bool {
    match pred {
        Predicate::AlwaysTrue => true,
        Predicate::And(left, right) => {
            file_matches_predicate(df, left) && file_matches_predicate(df, right)
        }
        Predicate::Or(left, right) => {
            file_matches_predicate(df, left) || file_matches_predicate(df, right)
        }
        Predicate::Not(inner) => !file_matches_predicate(df, inner),
        Predicate::Gt(field_id, val) => {
            if let PredicateValue::Long(v) = val {
                if let Some(upper) = df.upper_bound_i64(*field_id) {
                    return upper > *v;
                }
            }
            if let PredicateValue::Double(v) = val {
                if let Some(upper) = df.upper_bound_f64(*field_id) {
                    return upper > *v;
                }
            }
            true
        }
        Predicate::GtEq(field_id, val) => {
            if let PredicateValue::Long(v) = val {
                if let Some(upper) = df.upper_bound_i64(*field_id) {
                    return upper >= *v;
                }
            }
            if let PredicateValue::Double(v) = val {
                if let Some(upper) = df.upper_bound_f64(*field_id) {
                    return upper >= *v;
                }
            }
            true
        }
        Predicate::Lt(field_id, val) => {
            if let PredicateValue::Long(v) = val {
                if let Some(lower) = df.lower_bound_i64(*field_id) {
                    return lower < *v;
                }
            }
            if let PredicateValue::Double(v) = val {
                if let Some(lower) = df.lower_bound_f64(*field_id) {
                    return lower < *v;
                }
            }
            true
        }
        Predicate::LtEq(field_id, val) => {
            if let PredicateValue::Long(v) = val {
                if let Some(lower) = df.lower_bound_i64(*field_id) {
                    return lower <= *v;
                }
            }
            if let PredicateValue::Double(v) = val {
                if let Some(lower) = df.lower_bound_f64(*field_id) {
                    return lower <= *v;
                }
            }
            true
        }
        Predicate::Eq(field_id, val) => {
            if let PredicateValue::Long(v) = val {
                let lower = df.lower_bound_i64(*field_id);
                let upper = df.upper_bound_i64(*field_id);
                if let (Some(l), Some(u)) = (lower, upper) {
                    return l <= *v && u >= *v;
                }
            }
            if let PredicateValue::Double(v) = val {
                let lower = df.lower_bound_f64(*field_id);
                let upper = df.upper_bound_f64(*field_id);
                if let (Some(l), Some(u)) = (lower, upper) {
                    return l <= *v && u >= *v;
                }
            }
            if let PredicateValue::String(v) = val {
                let lower = df.lower_bound_str(*field_id);
                let upper = df.upper_bound_str(*field_id);
                if let (Some(l), Some(u)) = (lower, upper) {
                    return l <= v.as_str() && u >= v.as_str();
                }
            }
            true
        }
        Predicate::NotEq(_, _) => true,
        Predicate::InList(field_id, values) => {
            for v in values {
                if file_matches_predicate(df, &Predicate::Eq(*field_id, v.clone())) {
                    return true;
                }
            }
            false
        }
        Predicate::IsNull(_) => true,
        Predicate::IsNotNull(_) => true,
    }
}

fn row_filter(batch: &RecordBatch, pred: &Predicate, schema: &Schema) -> Option<RecordBatch> {
    let mask = evaluate_predicate(batch, pred, schema)?;
    match filter_record_batch(batch, &mask) {
        Ok(filtered) => Some(filtered),
        Err(_) => Some(batch.clone()),
    }
}

fn evaluate_predicate(
    batch: &RecordBatch,
    pred: &Predicate,
    schema: &Schema,
) -> Option<BooleanArray> {
    match pred {
        Predicate::AlwaysTrue => Some(BooleanArray::from(vec![true; batch.num_rows()])),
        Predicate::And(left, right) => {
            let l = evaluate_predicate(batch, left, schema)?;
            let r = evaluate_predicate(batch, right, schema)?;
            Some(boolean_and(&l, &r))
        }
        Predicate::Or(left, right) => {
            let l = evaluate_predicate(batch, left, schema)?;
            let r = evaluate_predicate(batch, right, schema)?;
            Some(boolean_or(&l, &r))
        }
        Predicate::Not(inner) => {
            let inner_mask = evaluate_predicate(batch, inner, schema)?;
            arrow_not(&inner_mask).ok()
        }
        Predicate::Eq(field_id, val) => {
            let col_idx = find_column_index(batch, schema, *field_id)?;
            compare_eq(batch.column(col_idx), val)
        }
        Predicate::NotEq(field_id, val) => {
            let col_idx = find_column_index(batch, schema, *field_id)?;
            let eq_mask = compare_eq(batch.column(col_idx), val)?;
            arrow_not(&eq_mask).ok()
        }
        Predicate::Gt(field_id, val) => {
            let col_idx = find_column_index(batch, schema, *field_id)?;
            compare_gt(batch.column(col_idx), val)
        }
        Predicate::GtEq(field_id, val) => {
            let col_idx = find_column_index(batch, schema, *field_id)?;
            compare_gteq(batch.column(col_idx), val)
        }
        Predicate::Lt(field_id, val) => {
            let col_idx = find_column_index(batch, schema, *field_id)?;
            compare_lt(batch.column(col_idx), val)
        }
        Predicate::LtEq(field_id, val) => {
            let col_idx = find_column_index(batch, schema, *field_id)?;
            compare_lteq(batch.column(col_idx), val)
        }
        Predicate::IsNull(field_id) => {
            let col_idx = find_column_index(batch, schema, *field_id)?;
            is_null_mask(batch.column(col_idx))
        }
        Predicate::IsNotNull(field_id) => {
            let col_idx = find_column_index(batch, schema, *field_id)?;
            let null_mask = is_null_mask(batch.column(col_idx))?;
            arrow_not(&null_mask).ok()
        }
        Predicate::InList(field_id, values) => {
            let col_idx = find_column_index(batch, schema, *field_id)?;
            let mut combined = BooleanArray::from(vec![false; batch.num_rows()]);
            for v in values {
                if let Some(eq) = compare_eq(batch.column(col_idx), v) {
                    combined = boolean_or(&combined, &eq);
                }
            }
            Some(combined)
        }
    }
}

fn find_column_index(batch: &RecordBatch, schema: &Schema, field_id: i32) -> Option<usize> {
    let field = schema.field_by_id(field_id)?;
    batch.schema().index_of(&field.name).ok()
}

fn boolean_and(left: &BooleanArray, right: &BooleanArray) -> BooleanArray {
    left.iter()
        .zip(right.iter())
        .map(|(l, r)| match (l, r) {
            (Some(true), Some(true)) => Some(true),
            (Some(false), _) | (_, Some(false)) => Some(false),
            _ => None,
        })
        .collect()
}

fn boolean_or(left: &BooleanArray, right: &BooleanArray) -> BooleanArray {
    left.iter()
        .zip(right.iter())
        .map(|(l, r)| match (l, r) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        })
        .collect()
}

fn extract_i64_values(col: &ArrayRef) -> Option<Vec<Option<i64>>> {
    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>()?;
            Some(arr.iter().collect())
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>()?;
            Some(arr.iter().map(|v| v.map(|x| x as i64)).collect())
        }
        DataType::Timestamp(_, _) => {
            let arr = col.as_primitive::<TimestampMicrosecondType>();
            Some(
                (0..arr.len())
                    .map(|i| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect(),
            )
        }
        DataType::Date32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>()?;
            Some(arr.iter().map(|v| v.map(|x| x as i64)).collect())
        }
        _ => None,
    }
}

fn extract_f64_values(col: &ArrayRef) -> Option<Vec<Option<f64>>> {
    match col.data_type() {
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>()?;
            Some(arr.iter().collect())
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>()?;
            Some(arr.iter().map(|v| v.map(|x| x as f64)).collect())
        }
        _ => None,
    }
}

fn compare_eq(col: &ArrayRef, val: &PredicateValue) -> Option<BooleanArray> {
    match val {
        PredicateValue::Long(v) => {
            let values = extract_i64_values(col)?;
            Some(BooleanArray::from(
                values
                    .iter()
                    .map(|x| x.map(|x| x == *v))
                    .collect::<Vec<_>>(),
            ))
        }
        PredicateValue::Double(v) => {
            let values = extract_f64_values(col)?;
            Some(BooleanArray::from(
                values
                    .iter()
                    .map(|x| x.map(|x| x == *v))
                    .collect::<Vec<_>>(),
            ))
        }
        PredicateValue::String(v) => {
            let arr = col.as_any().downcast_ref::<StringArray>()?;
            Some(arr.iter().map(|x| x.map(|x| x == v.as_str())).collect())
        }
        PredicateValue::Boolean(v) => {
            let arr = col.as_any().downcast_ref::<BooleanArray>()?;
            Some(arr.iter().map(|x| x.map(|x| x == *v)).collect())
        }
    }
}

fn compare_gt(col: &ArrayRef, val: &PredicateValue) -> Option<BooleanArray> {
    match val {
        PredicateValue::Long(v) => {
            let values = extract_i64_values(col)?;
            Some(BooleanArray::from(
                values.iter().map(|x| x.map(|x| x > *v)).collect::<Vec<_>>(),
            ))
        }
        PredicateValue::Double(v) => {
            let values = extract_f64_values(col)?;
            Some(BooleanArray::from(
                values.iter().map(|x| x.map(|x| x > *v)).collect::<Vec<_>>(),
            ))
        }
        PredicateValue::String(v) => {
            let arr = col.as_any().downcast_ref::<StringArray>()?;
            Some(arr.iter().map(|x| x.map(|x| x > v.as_str())).collect())
        }
        PredicateValue::Boolean(_) => None,
    }
}

fn compare_gteq(col: &ArrayRef, val: &PredicateValue) -> Option<BooleanArray> {
    match val {
        PredicateValue::Long(v) => {
            let values = extract_i64_values(col)?;
            Some(BooleanArray::from(
                values
                    .iter()
                    .map(|x| x.map(|x| x >= *v))
                    .collect::<Vec<_>>(),
            ))
        }
        PredicateValue::Double(v) => {
            let values = extract_f64_values(col)?;
            Some(BooleanArray::from(
                values
                    .iter()
                    .map(|x| x.map(|x| x >= *v))
                    .collect::<Vec<_>>(),
            ))
        }
        PredicateValue::String(v) => {
            let arr = col.as_any().downcast_ref::<StringArray>()?;
            Some(arr.iter().map(|x| x.map(|x| x >= v.as_str())).collect())
        }
        PredicateValue::Boolean(_) => None,
    }
}

fn compare_lt(col: &ArrayRef, val: &PredicateValue) -> Option<BooleanArray> {
    match val {
        PredicateValue::Long(v) => {
            let values = extract_i64_values(col)?;
            Some(BooleanArray::from(
                values.iter().map(|x| x.map(|x| x < *v)).collect::<Vec<_>>(),
            ))
        }
        PredicateValue::Double(v) => {
            let values = extract_f64_values(col)?;
            Some(BooleanArray::from(
                values.iter().map(|x| x.map(|x| x < *v)).collect::<Vec<_>>(),
            ))
        }
        PredicateValue::String(v) => {
            let arr = col.as_any().downcast_ref::<StringArray>()?;
            Some(arr.iter().map(|x| x.map(|x| x < v.as_str())).collect())
        }
        PredicateValue::Boolean(_) => None,
    }
}

fn compare_lteq(col: &ArrayRef, val: &PredicateValue) -> Option<BooleanArray> {
    match val {
        PredicateValue::Long(v) => {
            let values = extract_i64_values(col)?;
            Some(BooleanArray::from(
                values
                    .iter()
                    .map(|x| x.map(|x| x <= *v))
                    .collect::<Vec<_>>(),
            ))
        }
        PredicateValue::Double(v) => {
            let values = extract_f64_values(col)?;
            Some(BooleanArray::from(
                values
                    .iter()
                    .map(|x| x.map(|x| x <= *v))
                    .collect::<Vec<_>>(),
            ))
        }
        PredicateValue::String(v) => {
            let arr = col.as_any().downcast_ref::<StringArray>()?;
            Some(arr.iter().map(|x| x.map(|x| x <= v.as_str())).collect())
        }
        PredicateValue::Boolean(_) => None,
    }
}

fn is_null_mask(col: &ArrayRef) -> Option<BooleanArray> {
    Some(BooleanArray::from(
        (0..col.len()).map(|i| col.is_null(i)).collect::<Vec<_>>(),
    ))
}

pub fn filters_to_predicate(
    filters: &[datafusion::logical_expr::Expr],
    schema: &Schema,
) -> Option<Predicate> {
    if filters.is_empty() {
        return None;
    }

    let mut preds: Vec<Predicate> = Vec::new();

    for filter in filters {
        if let Some(p) = expr_to_predicate(filter, schema) {
            preds.push(p);
        }
    }

    if preds.is_empty() {
        return None;
    }

    Some(
        preds
            .into_iter()
            .reduce(|acc, p| Predicate::And(Box::new(acc), Box::new(p)))
            .expect("preds is non-empty"),
    )
}

fn expr_to_predicate(expr: &datafusion::logical_expr::Expr, schema: &Schema) -> Option<Predicate> {
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (field_id, val) = extract_field_and_value(left, right, schema)?;
            match op {
                Operator::Eq => Some(Predicate::Eq(field_id, val)),
                Operator::NotEq => Some(Predicate::NotEq(field_id, val)),
                Operator::Gt => Some(Predicate::Gt(field_id, val)),
                Operator::GtEq => Some(Predicate::GtEq(field_id, val)),
                Operator::Lt => Some(Predicate::Lt(field_id, val)),
                Operator::LtEq => Some(Predicate::LtEq(field_id, val)),
                Operator::And => {
                    let left_p = expr_to_predicate(left, schema)?;
                    let right_p = expr_to_predicate(right, schema)?;
                    Some(Predicate::And(Box::new(left_p), Box::new(right_p)))
                }
                Operator::Or => {
                    let left_p = expr_to_predicate(left, schema)?;
                    let right_p = expr_to_predicate(right, schema)?;
                    Some(Predicate::Or(Box::new(left_p), Box::new(right_p)))
                }
                _ => None,
            }
        }
        Expr::Not(inner) => {
            let p = expr_to_predicate(inner, schema)?;
            Some(Predicate::Not(Box::new(p)))
        }
        _ => None,
    }
}

fn extract_field_and_value(
    left: &datafusion::logical_expr::Expr,
    right: &datafusion::logical_expr::Expr,
    schema: &Schema,
) -> Option<(i32, PredicateValue)> {
    use datafusion::logical_expr::Expr;

    match (left, right) {
        (Expr::Column(col), Expr::Literal(scalar)) => {
            let field_id = column_name_to_field_id(schema, &col.name)?;
            let val = scalar_to_predicate_value(scalar)?;
            Some((field_id, val))
        }
        (Expr::Literal(scalar), Expr::Column(col)) => {
            let field_id = column_name_to_field_id(schema, &col.name)?;
            let val = scalar_to_predicate_value(scalar)?;
            Some((field_id, val))
        }
        _ => None,
    }
}

fn column_name_to_field_id(schema: &Schema, name: &str) -> Option<i32> {
    schema.field_by_name(name).map(|f| f.id)
}

fn scalar_to_predicate_value(scalar: &datafusion::scalar::ScalarValue) -> Option<PredicateValue> {
    use datafusion::scalar::ScalarValue;
    match scalar {
        ScalarValue::Int64(Some(v)) => Some(PredicateValue::Long(*v)),
        ScalarValue::Int32(Some(v)) => Some(PredicateValue::Long(*v as i64)),
        ScalarValue::Float64(Some(v)) => Some(PredicateValue::Double(*v)),
        ScalarValue::Float32(Some(v)) => Some(PredicateValue::Double(*v as f64)),
        ScalarValue::Utf8(Some(v)) => Some(PredicateValue::String(v.clone())),
        ScalarValue::Boolean(Some(v)) => Some(PredicateValue::Boolean(*v)),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(PredicateValue::Long(*v)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::IcebergCatalog;
    use crate::partition::PartitionSpec;
    use crate::schema::{Field, IcebergType, Schema};
    use crate::table::IcebergTable;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tsdb_arrow::schema::{DataPoint, FieldValue};

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

    fn setup_table_with_data() -> (TempDir, IcebergTable) {
        let dir = TempDir::new().unwrap();
        let catalog = Arc::new(IcebergCatalog::open(dir.path()).unwrap());
        let schema = make_test_schema();
        let spec = PartitionSpec::unpartitioned(0);
        catalog.create_table("cpu", schema, spec).unwrap();
        let meta = catalog.load_metadata("cpu").unwrap();
        let mut table = IcebergTable::new(catalog, "cpu".to_string(), meta);

        let dps: Vec<DataPoint> = (0..10)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.01))
            })
            .collect();
        table.append(&dps).unwrap();

        (dir, table)
    }

    #[test]
    fn test_scan_builder_default() {
        let (_dir, table) = setup_table_with_data();
        let scan = table.scan().build().unwrap();
        assert!(!scan.plan().is_empty());
    }

    #[test]
    fn test_scan_execute_full() {
        let (_dir, table) = setup_table_with_data();
        let scan = table.scan().build().unwrap();
        let batches = scan.execute().unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 10);
    }

    #[test]
    fn test_scan_with_predicate() {
        let (_dir, table) = setup_table_with_data();
        let scan = table
            .scan()
            .predicate(Predicate::Gt(1, PredicateValue::Long(5_000_000)))
            .build()
            .unwrap();
        assert!(!scan.plan().is_empty());
    }

    #[test]
    fn test_scan_empty_result() {
        let (_dir, table) = setup_table_with_data();
        let scan = table
            .scan()
            .predicate(Predicate::Gt(1, PredicateValue::Long(999_999_999_999)))
            .build()
            .unwrap();
        assert!(scan.plan().is_empty());
    }

    #[test]
    fn test_file_matches_predicate() {
        let mut df = crate::manifest::DataFile::new_parquet("/test.parquet".to_string(), 100, 4096);
        let mut lower = BTreeMap::new();
        lower.insert(1, 1000i64.to_be_bytes().to_vec());
        let mut upper = BTreeMap::new();
        upper.insert(1, 2000i64.to_be_bytes().to_vec());
        df.lower_bounds = Some(lower);
        df.upper_bounds = Some(upper);

        assert!(file_matches_predicate(
            &df,
            &Predicate::Gt(1, PredicateValue::Long(500))
        ));
        assert!(!file_matches_predicate(
            &df,
            &Predicate::Gt(1, PredicateValue::Long(3000))
        ));
        assert!(file_matches_predicate(
            &df,
            &Predicate::Eq(1, PredicateValue::Long(1500))
        ));
    }

    #[test]
    fn test_file_matches_predicate_gteq_lt_lteq() {
        let mut df = crate::manifest::DataFile::new_parquet("/test.parquet".to_string(), 100, 4096);
        let mut lower = BTreeMap::new();
        lower.insert(1, 1000i64.to_be_bytes().to_vec());
        let mut upper = BTreeMap::new();
        upper.insert(1, 2000i64.to_be_bytes().to_vec());
        df.lower_bounds = Some(lower);
        df.upper_bounds = Some(upper);

        assert!(file_matches_predicate(
            &df,
            &Predicate::GtEq(1, PredicateValue::Long(1000))
        ));
        assert!(!file_matches_predicate(
            &df,
            &Predicate::GtEq(1, PredicateValue::Long(2001))
        ));
        assert!(file_matches_predicate(
            &df,
            &Predicate::Lt(1, PredicateValue::Long(3000))
        ));
        assert!(!file_matches_predicate(
            &df,
            &Predicate::Lt(1, PredicateValue::Long(1000))
        ));
        assert!(file_matches_predicate(
            &df,
            &Predicate::LtEq(1, PredicateValue::Long(2000))
        ));
        assert!(!file_matches_predicate(
            &df,
            &Predicate::LtEq(1, PredicateValue::Long(999))
        ));
    }

    #[test]
    fn test_file_matches_predicate_and_or_not() {
        let mut df = crate::manifest::DataFile::new_parquet("/test.parquet".to_string(), 100, 4096);
        let mut lower = BTreeMap::new();
        lower.insert(1, 1000i64.to_be_bytes().to_vec());
        let mut upper = BTreeMap::new();
        upper.insert(1, 2000i64.to_be_bytes().to_vec());
        df.lower_bounds = Some(lower);
        df.upper_bounds = Some(upper);

        let and_pred = Predicate::And(
            Box::new(Predicate::Gt(1, PredicateValue::Long(500))),
            Box::new(Predicate::Lt(1, PredicateValue::Long(3000))),
        );
        assert!(file_matches_predicate(&df, &and_pred));

        let and_false = Predicate::And(
            Box::new(Predicate::Gt(1, PredicateValue::Long(3000))),
            Box::new(Predicate::Lt(1, PredicateValue::Long(4000))),
        );
        assert!(!file_matches_predicate(&df, &and_false));

        let or_pred = Predicate::Or(
            Box::new(Predicate::Lt(1, PredicateValue::Long(500))),
            Box::new(Predicate::Gt(1, PredicateValue::Long(1500))),
        );
        assert!(file_matches_predicate(&df, &or_pred));

        let not_pred = Predicate::Not(Box::new(Predicate::Gt(1, PredicateValue::Long(3000))));
        assert!(file_matches_predicate(&df, &not_pred));
    }

    #[test]
    fn test_file_matches_predicate_noteq_isnull_always_true() {
        let mut df = crate::manifest::DataFile::new_parquet("/test.parquet".to_string(), 100, 4096);
        let mut lower = BTreeMap::new();
        lower.insert(1, 1000i64.to_be_bytes().to_vec());
        let mut upper = BTreeMap::new();
        upper.insert(1, 2000i64.to_be_bytes().to_vec());
        df.lower_bounds = Some(lower);
        df.upper_bounds = Some(upper);

        assert!(file_matches_predicate(
            &df,
            &Predicate::NotEq(1, PredicateValue::Long(1500))
        ));
        assert!(file_matches_predicate(&df, &Predicate::IsNull(1)));
        assert!(file_matches_predicate(&df, &Predicate::IsNotNull(1)));
        assert!(file_matches_predicate(&df, &Predicate::AlwaysTrue));
    }

    #[test]
    fn test_scan_with_projection() {
        let (_dir, table) = setup_table_with_data();
        let scan = table.scan().projection(vec![1]).build().unwrap();
        let batches = scan.execute().unwrap();
        for batch in &batches {
            assert!(batch.num_columns() >= 1);
        }
    }
}
