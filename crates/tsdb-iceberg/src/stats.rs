use arrow::array::*;
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::manifest::DataFile;
use crate::schema::Schema;

#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    pub column_sizes: BTreeMap<i32, i64>,
    pub value_counts: BTreeMap<i32, i64>,
    pub null_value_counts: BTreeMap<i32, i64>,
    pub lower_bounds: BTreeMap<i32, Vec<u8>>,
    pub upper_bounds: BTreeMap<i32, Vec<u8>>,
}

pub fn collect_column_stats(batch: &RecordBatch, schema: &Schema) -> ColumnStats {
    let mut stats = ColumnStats::default();

    for field in &schema.fields {
        let col_idx = match batch.schema().index_of(&field.name) {
            Ok(idx) => idx,
            Err(_) => continue,
        };
        let col = batch.column(col_idx);
        let len = col.len();

        stats
            .column_sizes
            .insert(field.id, col.get_buffer_memory_size() as i64);
        stats.value_counts.insert(field.id, len as i64);

        let null_count = col.null_count() as i64;
        stats.null_value_counts.insert(field.id, null_count);

        if let (Some(lb), Some(ub)) = (
            compute_lower_bound(col, &field.field_type),
            compute_upper_bound(col, &field.field_type),
        ) {
            stats.lower_bounds.insert(field.id, lb);
            stats.upper_bounds.insert(field.id, ub);
        }
    }

    stats
}

fn compute_lower_bound(
    col: &Arc<dyn Array>,
    field_type: &crate::schema::IcebergType,
) -> Option<Vec<u8>> {
    if col.is_empty() {
        return None;
    }
    match field_type {
        crate::schema::IcebergType::Long => {
            let arr = col.as_any().downcast_ref::<Int64Array>()?;
            let min = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .min()?;
            Some(min.to_be_bytes().to_vec())
        }
        crate::schema::IcebergType::Int => {
            let arr = col.as_any().downcast_ref::<Int32Array>()?;
            let min = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .min()?;
            Some(min.to_be_bytes().to_vec())
        }
        crate::schema::IcebergType::Double => {
            let arr = col.as_any().downcast_ref::<Float64Array>()?;
            let min = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .reduce(f64::min)?;
            Some(min.to_be_bytes().to_vec())
        }
        crate::schema::IcebergType::Float => {
            let arr = col.as_any().downcast_ref::<Float32Array>()?;
            let min = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .reduce(f32::min)?;
            Some(min.to_be_bytes().to_vec())
        }
        crate::schema::IcebergType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>()?;
            let has_false = (0..arr.len()).any(|i| !arr.is_null(i) && !arr.value(i));
            Some(if has_false { vec![0] } else { vec![1] })
        }
        crate::schema::IcebergType::String => {
            let arr = col.as_any().downcast_ref::<StringArray>()?;
            let min = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .min()?;
            Some(min.as_bytes().to_vec())
        }
        crate::schema::IcebergType::Timestamp => {
            let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
            let min = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .min()?;
            Some(min.to_be_bytes().to_vec())
        }
        crate::schema::IcebergType::Timestamptz => {
            let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
            let min = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .min()?;
            Some(min.to_be_bytes().to_vec())
        }
        _ => None,
    }
}

fn compute_upper_bound(
    col: &Arc<dyn Array>,
    field_type: &crate::schema::IcebergType,
) -> Option<Vec<u8>> {
    if col.is_empty() {
        return None;
    }
    match field_type {
        crate::schema::IcebergType::Long => {
            let arr = col.as_any().downcast_ref::<Int64Array>()?;
            let max = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .max()?;
            Some(max.to_be_bytes().to_vec())
        }
        crate::schema::IcebergType::Int => {
            let arr = col.as_any().downcast_ref::<Int32Array>()?;
            let max = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .max()?;
            Some(max.to_be_bytes().to_vec())
        }
        crate::schema::IcebergType::Double => {
            let arr = col.as_any().downcast_ref::<Float64Array>()?;
            let max = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .reduce(f64::max)?;
            Some(max.to_be_bytes().to_vec())
        }
        crate::schema::IcebergType::Float => {
            let arr = col.as_any().downcast_ref::<Float32Array>()?;
            let max = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .reduce(f32::max)?;
            Some(max.to_be_bytes().to_vec())
        }
        crate::schema::IcebergType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>()?;
            let has_true = (0..arr.len()).any(|i| !arr.is_null(i) && arr.value(i));
            Some(if has_true { vec![1] } else { vec![0] })
        }
        crate::schema::IcebergType::String => {
            let arr = col.as_any().downcast_ref::<StringArray>()?;
            let max = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .max()?;
            Some(max.as_bytes().to_vec())
        }
        crate::schema::IcebergType::Timestamp => {
            let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
            let max = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .max()?;
            Some(max.to_be_bytes().to_vec())
        }
        crate::schema::IcebergType::Timestamptz => {
            let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
            let max = (0..arr.len())
                .filter(|&i| !arr.is_null(i))
                .map(|i| arr.value(i))
                .max()?;
            Some(max.to_be_bytes().to_vec())
        }
        _ => None,
    }
}

pub fn apply_stats_to_data_file(stats: &ColumnStats, data_file: &mut DataFile) {
    if !stats.column_sizes.is_empty() {
        data_file.column_sizes = Some(stats.column_sizes.clone());
    }
    if !stats.value_counts.is_empty() {
        data_file.value_counts = Some(stats.value_counts.clone());
    }
    if !stats.null_value_counts.is_empty() {
        data_file.null_value_counts = Some(stats.null_value_counts.clone());
    }
    if !stats.lower_bounds.is_empty() {
        data_file.lower_bounds = Some(stats.lower_bounds.clone());
    }
    if !stats.upper_bounds.is_empty() {
        data_file.upper_bounds = Some(stats.upper_bounds.clone());
    }
}

pub fn merge_column_stats(base: &mut ColumnStats, other: &ColumnStats) {
    for (&field_id, &size) in &other.column_sizes {
        *base.column_sizes.entry(field_id).or_insert(0) += size;
    }
    for (&field_id, &count) in &other.value_counts {
        *base.value_counts.entry(field_id).or_insert(0) += count;
    }
    for (&field_id, &count) in &other.null_value_counts {
        *base.null_value_counts.entry(field_id).or_insert(0) += count;
    }
    for (&field_id, lb) in &other.lower_bounds {
        use std::collections::btree_map::Entry;
        match base.lower_bounds.entry(field_id) {
            Entry::Vacant(e) => {
                e.insert(lb.clone());
            }
            Entry::Occupied(mut e) => {
                if lb.as_slice() < e.get().as_slice() {
                    *e.get_mut() = lb.clone();
                }
            }
        }
    }
    for (&field_id, ub) in &other.upper_bounds {
        use std::collections::btree_map::Entry;
        match base.upper_bounds.entry(field_id) {
            Entry::Vacant(e) => {
                e.insert(ub.clone());
            }
            Entry::Occupied(mut e) => {
                if ub.as_slice() > e.get().as_slice() {
                    *e.get_mut() = ub.clone();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Field, IcebergType, Schema};
    use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit};
    use std::sync::Arc;

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
    fn test_collect_stats_numeric() {
        let schema = make_test_schema();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            ArrowField::new("value", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(
                    TimestampMicrosecondArray::from(vec![1000, 2000, 3000]).with_timezone("UTC"),
                ),
                Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0), None])),
            ],
        )
        .unwrap();

        let stats = collect_column_stats(&batch, &schema);
        assert_eq!(*stats.value_counts.get(&1).unwrap(), 3);
        assert_eq!(*stats.null_value_counts.get(&1000).unwrap(), 1);
        assert!(stats.lower_bounds.contains_key(&1000));
        assert!(stats.upper_bounds.contains_key(&1000));
    }

    #[test]
    fn test_collect_stats_empty_batch() {
        let schema = make_test_schema();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            ArrowField::new("value", DataType::Float64, true),
        ]));

        let batch = RecordBatch::new_empty(arrow_schema);
        let stats = collect_column_stats(&batch, &schema);
        assert!(stats.lower_bounds.is_empty());
    }

    #[test]
    fn test_merge_column_stats_basic() {
        let mut base = ColumnStats::default();
        base.column_sizes.insert(1, 100);
        base.value_counts.insert(1, 50);
        base.lower_bounds.insert(1, 10i64.to_be_bytes().to_vec());
        base.upper_bounds.insert(1, 100i64.to_be_bytes().to_vec());

        let mut other = ColumnStats::default();
        other.column_sizes.insert(1, 50);
        other.value_counts.insert(1, 30);
        other.lower_bounds.insert(1, 5i64.to_be_bytes().to_vec());
        other.upper_bounds.insert(1, 200i64.to_be_bytes().to_vec());

        merge_column_stats(&mut base, &other);

        assert_eq!(*base.column_sizes.get(&1).unwrap(), 150);
        assert_eq!(*base.value_counts.get(&1).unwrap(), 80);
        assert_eq!(
            i64::from_be_bytes(base.lower_bounds[&1][..8].try_into().unwrap()),
            5
        );
        assert_eq!(
            i64::from_be_bytes(base.upper_bounds[&1][..8].try_into().unwrap()),
            200
        );
    }

    #[test]
    fn test_merge_column_stats_new_field_id() {
        let mut base = ColumnStats::default();
        base.value_counts.insert(1, 10);

        let mut other = ColumnStats::default();
        other.value_counts.insert(2, 20);

        merge_column_stats(&mut base, &other);

        assert_eq!(*base.value_counts.get(&1).unwrap(), 10);
        assert_eq!(*base.value_counts.get(&2).unwrap(), 20);
    }

    #[test]
    fn test_merge_column_stats_lower_bounds_no_update() {
        let mut base = ColumnStats::default();
        base.lower_bounds.insert(1, 5i64.to_be_bytes().to_vec());

        let mut other = ColumnStats::default();
        other.lower_bounds.insert(1, 10i64.to_be_bytes().to_vec());

        merge_column_stats(&mut base, &other);

        assert_eq!(
            i64::from_be_bytes(base.lower_bounds[&1][..8].try_into().unwrap()),
            5
        );
    }

    #[test]
    fn test_merge_column_stats_upper_bounds_no_update() {
        let mut base = ColumnStats::default();
        base.upper_bounds.insert(1, 200i64.to_be_bytes().to_vec());

        let mut other = ColumnStats::default();
        other.upper_bounds.insert(1, 100i64.to_be_bytes().to_vec());

        merge_column_stats(&mut base, &other);

        assert_eq!(
            i64::from_be_bytes(base.upper_bounds[&1][..8].try_into().unwrap()),
            200
        );
    }

    #[test]
    fn test_apply_stats_to_data_file() {
        let mut stats = ColumnStats::default();
        stats.column_sizes.insert(1, 100);
        stats.value_counts.insert(1, 50);
        stats.null_value_counts.insert(1, 2);
        stats.lower_bounds.insert(1, 10i64.to_be_bytes().to_vec());
        stats.upper_bounds.insert(1, 100i64.to_be_bytes().to_vec());

        let mut df =
            crate::manifest::DataFile::new_parquet("/tmp/test.parquet".to_string(), 50, 1024);
        apply_stats_to_data_file(&stats, &mut df);

        assert!(df.column_sizes.is_some());
        assert!(df.value_counts.is_some());
        assert!(df.null_value_counts.is_some());
        assert!(df.lower_bounds.is_some());
        assert!(df.upper_bounds.is_some());
    }

    #[test]
    fn test_apply_stats_to_data_file_empty() {
        let stats = ColumnStats::default();
        let mut df =
            crate::manifest::DataFile::new_parquet("/tmp/test.parquet".to_string(), 50, 1024);
        apply_stats_to_data_file(&stats, &mut df);

        assert!(df.column_sizes.is_none());
        assert!(df.value_counts.is_none());
    }

    #[test]
    fn test_collect_stats_all_null_column() {
        let schema = make_test_schema();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            ArrowField::new("value", DataType::Float64, true),
        ]));

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(
                    TimestampMicrosecondArray::from(vec![1000, 2000, 3000]).with_timezone("UTC"),
                ),
                Arc::new(Float64Array::from(vec![None, None, None])),
            ],
        )
        .unwrap();

        let stats = collect_column_stats(&batch, &schema);
        assert_eq!(*stats.null_value_counts.get(&1000).unwrap(), 3);
        assert!(!stats.lower_bounds.contains_key(&1000));
    }
}
