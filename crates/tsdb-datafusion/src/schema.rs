use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;
use tsdb_arrow::schema::FieldValue;

/// 根据度量名、标签键和字段类型构建 Arrow Schema
///
/// 生成的 Schema 包含以下列:
/// - `timestamp`: Timestamp(Microsecond) — 时间戳列 (非空)
/// - `tag_{key}`: Utf8 — 每个标签键对应一列 (可空)
/// - `{field_name}`: 对应数据类型 — 每个字段对应一列 (可空)
///
/// Schema 的 metadata 中包含 `measurement` 键，记录度量名。
///
/// # 参数
/// - `measurement` - 度量名 (如 "cpu", "memory")
/// - `tag_keys` - 标签键列表
/// - `field_types` - 字段名与示例值的列表 (用于推断类型)
///
/// # 返回
/// 构建好的 SchemaRef (Arc<Schema>)
pub fn measurement_to_schema(
    measurement: &str,
    tag_keys: &[String],
    field_types: &[(String, FieldValue)],
) -> SchemaRef {
    let mut fields = vec![Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    )];

    for key in tag_keys {
        fields.push(Field::new(format!("tag_{}", key), DataType::Utf8, true));
    }

    for (name, value) in field_types {
        let dtype = match value {
            FieldValue::Float(_) => DataType::Float64,
            FieldValue::Integer(_) => DataType::Int64,
            FieldValue::String(_) => DataType::Utf8,
            FieldValue::Boolean(_) => DataType::Boolean,
        };
        fields.push(Field::new(name.clone(), dtype, true));
    }

    let mut metadata = HashMap::new();
    metadata.insert("measurement".to_string(), measurement.to_string());
    let schema = Schema::new(fields).with_metadata(metadata);

    Arc::new(schema)
}

/// 将 Arrow Schema 转换为 SQL CREATE TABLE 语句
///
/// 生成的 SQL 格式: `CREATE TABLE IF NOT EXISTS t (timestamp, col1, col2, ...)`
///
/// # 参数
/// - `schema` - Arrow Schema
///
/// # 返回
/// SQL DDL 语句字符串
pub fn schema_to_sql(schema: &Schema) -> String {
    let mut cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    cols.insert(0, "timestamp".to_string());
    format!("CREATE TABLE IF NOT EXISTS t ({})", cols.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_measurement_to_schema() {
        let tag_keys = vec!["host".to_string(), "region".to_string()];
        let field_types = vec![
            ("usage".to_string(), FieldValue::Float(0.5)),
            ("count".to_string(), FieldValue::Integer(42)),
        ];

        let schema = measurement_to_schema("cpu", &tag_keys, &field_types);
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert!(field_names.contains(&"timestamp"));
        assert!(field_names.contains(&"tag_host"));
        assert!(field_names.contains(&"tag_region"));
        assert!(field_names.contains(&"usage"));
        assert!(field_names.contains(&"count"));

        let measurement = schema.metadata().get("measurement").unwrap();
        assert_eq!(measurement, "cpu");
    }

    #[test]
    fn test_schema_to_sql() {
        let tag_keys = vec!["host".to_string()];
        let field_types = vec![("usage".to_string(), FieldValue::Float(0.5))];
        let schema = measurement_to_schema("cpu", &tag_keys, &field_types);

        let sql = schema_to_sql(&schema);
        assert!(sql.contains("timestamp"));
        assert!(sql.contains("CREATE TABLE"));
    }
}
