use crate::error::{Result, TsdbArrowError};
use crate::schema::{DataPoint, FieldValue, Fields, Tags};
use arrow::array::*;
use arrow::datatypes::{DataType, SchemaRef};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

/// 计算标签集合的哈希值
///
/// 使用 DefaultHasher 对标签的键值对依次哈希,
/// 结果用于 RocksDB 键的前缀部分, 将同一 series 的数据聚集存储。
pub fn compute_tags_hash(tags: &Tags) -> u64 {
    let mut hasher = DefaultHasher::new();
    for (k, v) in tags {
        Hash::hash(&k, &mut hasher);
        Hash::hash(&v, &mut hasher);
    }
    hasher.finish()
}

/// 将 DataPoint 列表转换为 Arrow RecordBatch
///
/// 根据 schema 中的列定义, 逐列构建 Arrow 数组。
/// 支持两种 schema 模式:
/// - **扩展模式**: 包含 timestamp, measurement, tags_hash, tag_keys, tag_values + 字段列
/// - **紧凑模式**: 包含 timestamp, tag_{key}... + 字段列 (标签展开为独立列)
///
/// 当 datapoints 为空时返回空 RecordBatch。
pub fn datapoints_to_record_batch(
    datapoints: &[DataPoint],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    if datapoints.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let arr = build_column_array(field.name(), field.data_type(), datapoints)?;
        columns.push(arr);
    }

    RecordBatch::try_new(schema, columns).map_err(TsdbArrowError::Arrow)
}

/// 根据列名分发到对应的数组构建函数
fn build_column_array(
    name: &str,
    dtype: &DataType,
    datapoints: &[DataPoint],
) -> Result<Arc<dyn Array>> {
    match name {
        "timestamp" => build_timestamp_array(dtype, datapoints),
        "measurement" => build_measurement_array(datapoints),
        "tags_hash" => build_tags_hash_array(datapoints),
        "tag_keys" => build_tag_keys_array(datapoints),
        "tag_values" => build_tag_values_array(datapoints),
        _ => {
            if let Some(prefix) = name.strip_prefix("tag_") {
                build_tag_column_array(prefix, datapoints)
            } else {
                build_field_column_array(name, dtype, datapoints)
            }
        }
    }
}

fn build_timestamp_array(dtype: &DataType, datapoints: &[DataPoint]) -> Result<Arc<dyn Array>> {
    let tz = match dtype {
        DataType::Timestamp(_, Some(tz)) => Some(tz.clone()),
        _ => None,
    };
    let mut builder = TimestampMicrosecondBuilder::new();
    for dp in datapoints {
        builder.append_value(dp.timestamp);
    }
    let mut arr = builder.finish();
    if let Some(tz) = tz {
        arr = arr.with_timezone(tz);
    }
    Ok(Arc::new(arr))
}

/// 构建 measurement 列 (Utf8)
fn build_measurement_array(datapoints: &[DataPoint]) -> Result<Arc<dyn Array>> {
    let mut builder = StringBuilder::new();
    for dp in datapoints {
        builder.append_value(&dp.measurement);
    }
    Ok(Arc::new(builder.finish()))
}

/// 构建 tags_hash 列 (UInt64)
fn build_tags_hash_array(datapoints: &[DataPoint]) -> Result<Arc<dyn Array>> {
    let mut builder = UInt64Builder::new();
    for dp in datapoints {
        let hash = compute_tags_hash(&dp.tags);
        builder.append_value(hash);
    }
    Ok(Arc::new(builder.finish()))
}

/// 构建 tag_keys 列 (List<Utf8>) — 扩展模式专用
fn build_tag_keys_array(datapoints: &[DataPoint]) -> Result<Arc<dyn Array>> {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for dp in datapoints {
        for key in dp.tags.keys() {
            builder.values().append_value(key);
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

/// 构建 tag_values 列 (List<Utf8>) — 扩展模式专用
fn build_tag_values_array(datapoints: &[DataPoint]) -> Result<Arc<dyn Array>> {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for dp in datapoints {
        for val in dp.tags.values() {
            builder.values().append_value(val);
        }
        builder.append(true);
    }
    Ok(Arc::new(builder.finish()))
}

/// 构建单个标签列 (Utf8, nullable) — 紧凑模式专用
///
/// 列名格式为 `tag_{key}`, 缺少该标签的数据点对应位置为 null。
fn build_tag_column_array(tag_key: &str, datapoints: &[DataPoint]) -> Result<Arc<dyn Array>> {
    let mut builder = StringBuilder::new();
    for dp in datapoints {
        match dp.tags.get(tag_key) {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// 构建字段列, 根据 Arrow DataType 选择对应构建器
///
/// 支持类型: Float64, Int64, Utf8, Boolean
/// 字段缺失时填充 null; 类型不匹配时尝试隐式转换 (如 Int64→Float64)。
fn build_field_column_array(
    field_name: &str,
    dtype: &DataType,
    datapoints: &[DataPoint],
) -> Result<Arc<dyn Array>> {
    match dtype {
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for dp in datapoints {
                match dp.fields.get(field_name) {
                    Some(FieldValue::Float(v)) => builder.append_value(*v),
                    Some(FieldValue::Integer(v)) => builder.append_value(*v as f64),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::new();
            for dp in datapoints {
                match dp.fields.get(field_name) {
                    Some(FieldValue::Integer(v)) => builder.append_value(*v),
                    Some(FieldValue::Float(v)) => builder.append_value(*v as i64),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for dp in datapoints {
                match dp.fields.get(field_name) {
                    Some(FieldValue::String(v)) => builder.append_value(v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for dp in datapoints {
                match dp.fields.get(field_name) {
                    Some(FieldValue::Boolean(v)) => builder.append_value(*v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(TsdbArrowError::Conversion(format!(
            "unsupported field type for column '{}': {:?}",
            field_name, other
        ))),
    }
}

/// 将 Arrow RecordBatch 反向转换为 DataPoint 列表
///
/// 逐行解析 RecordBatch:
/// 1. 从 timestamp 列读取微秒时间戳
/// 2. 从 measurement 列或 schema metadata 读取指标名
/// 3. 从 `tag_{key}` 列读取标签 (紧凑模式)
/// 4. 从字段列读取字段值
///
/// 跳过 timestamp, measurement, tags_hash, tag_keys, tag_values 等元数据列。
pub fn record_batch_to_datapoints(batch: &RecordBatch) -> Result<Vec<DataPoint>> {
    let schema = batch.schema();
    let row_count = batch.num_rows();

    let timestamp_col = schema
        .index_of("timestamp")
        .map_err(|_| TsdbArrowError::Conversion("timestamp column not found".into()))?;

    let ts_array = batch
        .column(timestamp_col)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            TsdbArrowError::Conversion("timestamp column is not TimestampMicrosecond".into())
        })?;

    let measurement_col = schema
        .index_of("measurement")
        .ok()
        .map(|idx| batch.column(idx).as_any().downcast_ref::<StringArray>());

    let mut results = Vec::with_capacity(row_count);

    for i in 0..row_count {
        let timestamp = ts_array.value(i);

        let measurement = if let Some(Some(col)) = measurement_col {
            col.value(i).to_string()
        } else {
            schema
                .metadata()
                .get("measurement")
                .cloned()
                .unwrap_or_default()
        };

        let mut tags = Tags::new();
        let mut fields = Fields::new();

        for (idx, field) in schema.fields().iter().enumerate() {
            let name = field.name();
            if name == "timestamp" || name == "measurement" {
                continue;
            }

            if name == "tags_hash" || name == "tag_keys" || name == "tag_values" {
                continue;
            }

            if let Some(tag_key) = name.strip_prefix("tag_") {
                if field
                    .metadata()
                    .get("tsdb_role")
                    .map(|r| r == "tag")
                    .unwrap_or(false)
                    || (field.metadata().is_empty() && name.starts_with("tag_"))
                {
                    if let Some(col) = batch.column(idx).as_any().downcast_ref::<StringArray>() {
                        if !col.is_null(i) {
                            tags.insert(tag_key.to_string(), col.value(i).to_string());
                        }
                    }
                    continue;
                }
            }

            let col = batch.column(idx);
            if col.is_null(i) {
                continue;
            }

            let fv = extract_field_value(col, i, field.data_type())?;
            fields.insert(name.clone(), fv);
        }

        results.push(DataPoint {
            measurement,
            tags,
            fields,
            timestamp,
        });
    }

    Ok(results)
}

/// 从 Arrow 数组中提取单个字段值
fn extract_field_value(col: &Arc<dyn Array>, index: usize, dtype: &DataType) -> Result<FieldValue> {
    match dtype {
        DataType::Float64 => {
            let arr = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| TsdbArrowError::Conversion("failed to downcast Float64".into()))?;
            Ok(FieldValue::Float(arr.value(index)))
        }
        DataType::Int64 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| TsdbArrowError::Conversion("failed to downcast Int64".into()))?;
            Ok(FieldValue::Integer(arr.value(index)))
        }
        DataType::Utf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| TsdbArrowError::Conversion("failed to downcast Utf8".into()))?;
            Ok(FieldValue::String(arr.value(index).to_string()))
        }
        DataType::Boolean => {
            let arr = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| TsdbArrowError::Conversion("failed to downcast Boolean".into()))?;
            Ok(FieldValue::Boolean(arr.value(index)))
        }
        _ => Err(TsdbArrowError::Conversion(format!(
            "unsupported data type: {:?}",
            dtype
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{tsdb_schema, TsdbSchemaBuilder};
    use arrow::datatypes::DataType;

    fn make_test_datapoints() -> Vec<DataPoint> {
        vec![
            DataPoint::new("cpu", 1_000_000)
                .with_tag("host", "server01")
                .with_tag("region", "us-west")
                .with_field("usage", FieldValue::Float(0.5))
                .with_field("idle", FieldValue::Float(0.5))
                .with_field("count", FieldValue::Integer(100)),
            DataPoint::new("cpu", 2_000_000)
                .with_tag("host", "server02")
                .with_tag("region", "us-east")
                .with_field("usage", FieldValue::Float(0.8))
                .with_field("idle", FieldValue::Float(0.2))
                .with_field("count", FieldValue::Integer(200)),
        ]
    }

    #[test]
    fn test_datapoints_to_record_batch_compact() {
        let dps = make_test_datapoints();
        let schema = TsdbSchemaBuilder::new("cpu")
            .compact()
            .with_tag_key("host")
            .with_tag_key("region")
            .with_float_field("usage")
            .with_float_field("idle")
            .with_int_field("count")
            .build();

        let batch = datapoints_to_record_batch(&dps, schema).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let ts_col = batch
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts_col.value(0), 1_000_000);
        assert_eq!(ts_col.value(1), 2_000_000);

        let host_col = batch
            .column_by_name("tag_host")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(host_col.value(0), "server01");
        assert_eq!(host_col.value(1), "server02");

        let usage_col = batch
            .column_by_name("usage")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(usage_col.value(0), 0.5);
        assert_eq!(usage_col.value(1), 0.8);
    }

    #[test]
    fn test_roundtrip_compact() {
        let dps = make_test_datapoints();
        let schema = TsdbSchemaBuilder::new("cpu")
            .compact()
            .with_tag_key("host")
            .with_tag_key("region")
            .with_float_field("usage")
            .with_float_field("idle")
            .with_int_field("count")
            .build();

        let batch = datapoints_to_record_batch(&dps, schema.clone()).unwrap();
        let restored = record_batch_to_datapoints(&batch).unwrap();

        assert_eq!(restored.len(), 2);
        assert_eq!(restored[0].timestamp, 1_000_000);
        assert_eq!(restored[0].measurement, "cpu");
        assert_eq!(restored[0].tags.get("host").unwrap(), "server01");
        assert_eq!(
            restored[0].fields.get("usage").unwrap(),
            &FieldValue::Float(0.5)
        );
        assert_eq!(
            restored[0].fields.get("count").unwrap(),
            &FieldValue::Integer(100)
        );
    }

    #[test]
    fn test_datapoints_to_record_batch_extended() {
        let dps = make_test_datapoints();
        let schema = tsdb_schema(
            "cpu",
            &[
                ("usage".into(), DataType::Float64),
                ("idle".into(), DataType::Float64),
                ("count".into(), DataType::Int64),
            ],
        );

        let batch = datapoints_to_record_batch(&dps, schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(batch.column_by_name("tags_hash").is_some());
        assert!(batch.column_by_name("tag_keys").is_some());
    }

    #[test]
    fn test_empty_datapoints() {
        let schema = TsdbSchemaBuilder::new("cpu")
            .compact()
            .with_tag_key("host")
            .with_float_field("usage")
            .build();

        let batch = datapoints_to_record_batch(&[], schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_null_fields() {
        let dps = vec![
            DataPoint::new("cpu", 1_000_000)
                .with_tag("host", "s1")
                .with_field("usage", FieldValue::Float(0.5)),
            DataPoint::new("cpu", 2_000_000).with_tag("host", "s2"),
        ];

        let schema = TsdbSchemaBuilder::new("cpu")
            .compact()
            .with_tag_key("host")
            .with_float_field("usage")
            .build();

        let batch = datapoints_to_record_batch(&dps, schema).unwrap();
        let usage_col = batch
            .column_by_name("usage")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(usage_col.value(0), 0.5);
        assert!(usage_col.is_null(1));
    }
}
