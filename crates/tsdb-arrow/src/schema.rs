use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;

/// 创建扩展模式的时序 Schema
///
/// 列结构:
/// - `timestamp`: Timestamp(Microsecond) — 微秒时间戳
/// - `measurement`: Utf8 — 指标名
/// - `tags_hash`: UInt64 — 标签哈希
/// - `tag_keys`: List<Utf8> — 标签键列表
/// - `tag_values`: List<Utf8> — 标签值列表
/// - 字段列: 根据 `field_types` 动态添加
///
/// Schema metadata 中包含 `measurement` 键。
pub fn tsdb_schema(measurement: &str, field_types: &[(String, DataType)]) -> SchemaRef {
    let mut fields = vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("measurement", DataType::Utf8, false),
        Field::new("tags_hash", DataType::UInt64, false),
        Field::new(
            "tag_keys",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "tag_values",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ];

    for (name, dtype) in field_types {
        fields.push(Field::new(name.clone(), dtype.clone(), true));
    }

    let mut metadata = HashMap::new();
    metadata.insert("measurement".to_string(), measurement.to_string());
    let schema = Schema::new(fields).with_metadata(metadata);

    Arc::new(schema)
}

/// 创建紧凑模式的时序 Schema
///
/// 列结构:
/// - `timestamp`: Timestamp(Microsecond) — 微秒时间戳
/// - `tag_{key}`: Utf8 (nullable) — 每个标签键展开为独立列
/// - 字段列: 根据 `field_types` 动态添加
///
/// 紧凑模式适合 Parquet 存储, 避免了 List 类型的开销。
pub fn compact_tsdb_schema(
    measurement: &str,
    tag_keys: &[String],
    field_types: &[(String, DataType)],
) -> SchemaRef {
    let mut fields = vec![Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    )];

    for key in tag_keys {
        fields.push(Field::new(format!("tag_{}", key), DataType::Utf8, true));
    }

    for (name, dtype) in field_types {
        fields.push(Field::new(name.clone(), dtype.clone(), true));
    }

    let mut metadata = HashMap::new();
    metadata.insert("measurement".to_string(), measurement.to_string());
    let schema = Schema::new(fields).with_metadata(metadata);

    Arc::new(schema)
}

/// 时序 Schema 构建器
///
/// 支持链式调用构建扩展模式或紧凑模式的 Schema。
///
/// # 示例
/// ```ignore
/// let schema = TsdbSchemaBuilder::new("cpu")
///     .compact()
///     .with_tag_key("host")
///     .with_float_field("usage")
///     .build();
/// ```
pub struct TsdbSchemaBuilder {
    measurement: String,
    tag_keys: Vec<String>,
    field_types: Vec<(String, DataType)>,
    compact: bool,
}

impl TsdbSchemaBuilder {
    /// 创建构建器, 指定 measurement 名称
    pub fn new(measurement: impl Into<String>) -> Self {
        Self {
            measurement: measurement.into(),
            tag_keys: Vec::new(),
            field_types: Vec::new(),
            compact: false,
        }
    }

    /// 添加标签键
    pub fn with_tag_key(mut self, key: impl Into<String>) -> Self {
        self.tag_keys.push(key.into());
        self
    }

    /// 添加 Float64 字段
    pub fn with_float_field(mut self, name: impl Into<String>) -> Self {
        self.field_types.push((name.into(), DataType::Float64));
        self
    }

    /// 添加 Int64 字段
    pub fn with_int_field(mut self, name: impl Into<String>) -> Self {
        self.field_types.push((name.into(), DataType::Int64));
        self
    }

    /// 添加 Utf8 字段
    pub fn with_string_field(mut self, name: impl Into<String>) -> Self {
        self.field_types.push((name.into(), DataType::Utf8));
        self
    }

    /// 添加 Boolean 字段
    pub fn with_bool_field(mut self, name: impl Into<String>) -> Self {
        self.field_types.push((name.into(), DataType::Boolean));
        self
    }

    /// 切换为紧凑模式 (标签展开为独立列)
    pub fn compact(mut self) -> Self {
        self.compact = true;
        self
    }

    /// 构建 Schema
    pub fn build(self) -> SchemaRef {
        if self.compact {
            compact_tsdb_schema(&self.measurement, &self.tag_keys, &self.field_types)
        } else {
            tsdb_schema(&self.measurement, &self.field_types)
        }
    }
}

/// 根据字段值推断 Arrow DataType
pub fn field_value_to_data_type(value: &FieldValue) -> DataType {
    match value {
        FieldValue::Float(_) => DataType::Float64,
        FieldValue::Integer(_) => DataType::Int64,
        FieldValue::String(_) => DataType::Utf8,
        FieldValue::Boolean(_) => DataType::Boolean,
    }
}

use std::collections::BTreeMap;

/// 标签集合: 有序键值对 (BTreeMap 保证序列化顺序一致)
pub type Tags = BTreeMap<String, String>;
/// 字段集合: 有序键值对
pub type Fields = BTreeMap<String, FieldValue>;

/// 字段值枚举
///
/// 支持四种时序数据常见类型, 与 Arrow 类型一一对应:
/// - Float → Float64
/// - Integer → Int64
/// - String → Utf8
/// - Boolean → Boolean
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum FieldValue {
    Float(f64),
    Integer(i64),
    String(String),
    Boolean(bool),
}

impl<'de> serde::Deserialize<'de> for FieldValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        struct FieldValueVisitor;

        impl<'de> Visitor<'de> for FieldValueVisitor {
            type Value = FieldValue;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a FieldValue (plain value or tagged object)")
            }

            fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<FieldValue, E> {
                Ok(FieldValue::Float(v))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<FieldValue, E> {
                Ok(FieldValue::Integer(v))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<FieldValue, E> {
                Ok(FieldValue::Integer(v as i64))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<FieldValue, E> {
                Ok(FieldValue::String(v.to_string()))
            }

            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<FieldValue, E> {
                Ok(FieldValue::Boolean(v))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<FieldValue, A::Error> {
                let (key, value): (String, serde_json::Value) = map.next_entry()?.ok_or_else(|| {
                    de::Error::custom("empty map is not a valid FieldValue")
                })?;
                match key.as_str() {
                    "Float" => value.as_f64().map(FieldValue::Float).ok_or_else(|| {
                        de::Error::custom("invalid Float value")
                    }),
                    "Integer" => value.as_i64().map(FieldValue::Integer).ok_or_else(|| {
                        de::Error::custom("invalid Integer value")
                    }),
                    "String" => value.as_str().map(|s| FieldValue::String(s.to_string())).ok_or_else(|| {
                        de::Error::custom("invalid String value")
                    }),
                    "Boolean" => value.as_bool().map(FieldValue::Boolean).ok_or_else(|| {
                        de::Error::custom("invalid Boolean value")
                    }),
                    other => Err(de::Error::custom(format!("unknown FieldValue variant: {}", other))),
                }
            }
        }

        deserializer.deserialize_any(FieldValueVisitor)
    }
}

impl FieldValue {
    /// 尝试转换为 f64, Integer 会隐式转换
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FieldValue::Float(v) => Some(*v),
            FieldValue::Integer(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// 尝试转换为 i64, Float 会截断小数部分
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            FieldValue::Integer(v) => Some(*v),
            FieldValue::Float(v) => Some(*v as i64),
            _ => None,
        }
    }

    /// 尝试获取字符串引用
    pub fn as_str(&self) -> Option<&str> {
        match self {
            FieldValue::String(v) => Some(v),
            _ => None,
        }
    }

    /// 尝试获取布尔值
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            FieldValue::Boolean(v) => Some(*v),
            _ => None,
        }
    }
}

/// 时序数据点
///
/// 一个 DataPoint 代表某时刻某指标的一条记录,
/// 包含指标名、标签集合、字段集合和微秒级时间戳。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataPoint {
    /// 指标名 (如 "cpu", "memory")
    pub measurement: String,
    /// 标签键值对 (用于标识 series)
    pub tags: Tags,
    /// 字段键值对 (实际数据值)
    pub fields: Fields,
    /// 微秒级时间戳
    pub timestamp: i64,
}

impl DataPoint {
    /// 创建新的数据点
    pub fn new(measurement: impl Into<String>, timestamp: i64) -> Self {
        Self {
            measurement: measurement.into(),
            tags: Tags::new(),
            fields: Fields::new(),
            timestamp,
        }
    }

    /// 添加标签 (链式调用)
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// 添加字段 (链式调用)
    pub fn with_field(mut self, key: impl Into<String>, value: FieldValue) -> Self {
        self.fields.insert(key.into(), value);
        self
    }

    /// 生成 series key: `measurement,tag1=val1,tag2=val2` (标签排序)
    pub fn series_key(&self) -> String {
        let mut parts: Vec<String> = self
            .tags
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        parts.sort();
        format!("{},{}", self.measurement, parts.join(","))
    }
}

/// 字段类型枚举 (无值, 仅表示类型信息)
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FieldType {
    Float,
    Integer,
    String,
    Boolean,
}

impl From<&FieldValue> for FieldType {
    fn from(value: &FieldValue) -> Self {
        match value {
            FieldValue::Float(_) => FieldType::Float,
            FieldValue::Integer(_) => FieldType::Integer,
            FieldValue::String(_) => FieldType::String,
            FieldValue::Boolean(_) => FieldType::Boolean,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_value_as_f64() {
        assert_eq!(FieldValue::Float(3.14).as_f64(), Some(3.14));
        assert_eq!(FieldValue::Integer(42).as_f64(), Some(42.0));
        assert_eq!(FieldValue::String("hello".into()).as_f64(), None);
        assert_eq!(FieldValue::Boolean(true).as_f64(), None);
    }

    #[test]
    fn test_field_value_as_i64() {
        assert_eq!(FieldValue::Integer(42).as_i64(), Some(42));
        assert_eq!(FieldValue::Float(3.14).as_i64(), Some(3));
        assert_eq!(FieldValue::String("hello".into()).as_i64(), None);
        assert_eq!(FieldValue::Boolean(false).as_i64(), None);
    }

    #[test]
    fn test_field_value_as_str() {
        assert_eq!(FieldValue::String("hello".into()).as_str(), Some("hello"));
        assert_eq!(FieldValue::Float(3.14).as_str(), None);
        assert_eq!(FieldValue::Integer(42).as_str(), None);
        assert_eq!(FieldValue::Boolean(true).as_str(), None);
    }

    #[test]
    fn test_field_value_as_bool() {
        assert_eq!(FieldValue::Boolean(true).as_bool(), Some(true));
        assert_eq!(FieldValue::Boolean(false).as_bool(), Some(false));
        assert_eq!(FieldValue::Float(3.14).as_bool(), None);
        assert_eq!(FieldValue::Integer(42).as_bool(), None);
    }

    #[test]
    fn test_field_value_serde_roundtrip_float() {
        let original = FieldValue::Float(3.14);
        let json = serde_json::to_string(&original).unwrap();
        let restored: FieldValue = serde_json::from_str(&json).unwrap();
        assert!(matches!(restored, FieldValue::Float(v) if (v - 3.14).abs() < f64::EPSILON));
    }

    #[test]
    fn test_field_value_serde_roundtrip_integer() {
        let original = FieldValue::Integer(42);
        let json = serde_json::to_string(&original).unwrap();
        let restored: FieldValue = serde_json::from_str(&json).unwrap();
        assert!(matches!(restored, FieldValue::Integer(42)));
    }

    #[test]
    fn test_field_value_serde_roundtrip_string() {
        let original = FieldValue::String("hello".into());
        let json = serde_json::to_string(&original).unwrap();
        let restored: FieldValue = serde_json::from_str(&json).unwrap();
        assert!(matches!(restored, FieldValue::String(s) if s == "hello"));
    }

    #[test]
    fn test_field_value_serde_roundtrip_boolean() {
        let original = FieldValue::Boolean(true);
        let json = serde_json::to_string(&original).unwrap();
        let restored: FieldValue = serde_json::from_str(&json).unwrap();
        assert!(matches!(restored, FieldValue::Boolean(true)));
    }

    #[test]
    fn test_field_value_deserialize_tagged_object() {
        let json = r#"{"Float": 2.71}"#;
        let fv: FieldValue = serde_json::from_str(json).unwrap();
        assert!(matches!(fv, FieldValue::Float(v) if (v - 2.71).abs() < f64::EPSILON));

        let json = r#"{"Integer": 99}"#;
        let fv: FieldValue = serde_json::from_str(json).unwrap();
        assert!(matches!(fv, FieldValue::Integer(99)));

        let json = r#"{"String": "test"}"#;
        let fv: FieldValue = serde_json::from_str(json).unwrap();
        assert!(matches!(fv, FieldValue::String(s) if s == "test"));

        let json = r#"{"Boolean": true}"#;
        let fv: FieldValue = serde_json::from_str(json).unwrap();
        assert!(matches!(fv, FieldValue::Boolean(true)));
    }

    #[test]
    fn test_field_value_deserialize_unknown_variant() {
        let json = r#"{"Unknown": 42}"#;
        let result: Result<FieldValue, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_field_type_from_field_value() {
        assert_eq!(FieldType::from(&FieldValue::Float(0.0)), FieldType::Float);
        assert_eq!(FieldType::from(&FieldValue::Integer(0)), FieldType::Integer);
        assert_eq!(FieldType::from(&FieldValue::String("".into())), FieldType::String);
        assert_eq!(FieldType::from(&FieldValue::Boolean(false)), FieldType::Boolean);
    }

    #[test]
    fn test_series_key_with_tags() {
        let dp = DataPoint::new("cpu", 1000)
            .with_tag("host", "server01")
            .with_tag("region", "us-west");
        let key = dp.series_key();
        assert_eq!(key, "cpu,host=server01,region=us-west");
    }

    #[test]
    fn test_series_key_no_tags() {
        let dp = DataPoint::new("cpu", 1000);
        let key = dp.series_key();
        assert_eq!(key, "cpu,");
    }

    #[test]
    fn test_series_key_single_tag() {
        let dp = DataPoint::new("mem", 2000)
            .with_tag("host", "srv1");
        let key = dp.series_key();
        assert_eq!(key, "mem,host=srv1");
    }

    #[test]
    fn test_schema_builder_compact_all_types() {
        let schema = TsdbSchemaBuilder::new("test")
            .with_tag_key("host")
            .with_float_field("f1")
            .with_int_field("i1")
            .with_string_field("s1")
            .with_bool_field("b1")
            .compact()
            .build();

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"timestamp"));
        assert!(field_names.contains(&"tag_host"));
        assert!(field_names.contains(&"f1"));
        assert!(field_names.contains(&"i1"));
        assert!(field_names.contains(&"s1"));
        assert!(field_names.contains(&"b1"));
    }

    #[test]
    fn test_schema_builder_extended_mode() {
        let schema = TsdbSchemaBuilder::new("test")
            .with_tag_key("region")
            .with_float_field("value")
            .build();

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"timestamp"));
        assert!(field_names.contains(&"tags_hash"));
        assert!(field_names.contains(&"tag_keys"));
        assert!(field_names.contains(&"tag_values"));
        assert!(field_names.contains(&"value"));
    }

    #[test]
    fn test_compact_tsdb_schema() {
        let schema = compact_tsdb_schema(
            "cpu",
            &["host".to_string(), "region".to_string()],
            &[
                ("usage".to_string(), arrow::datatypes::DataType::Float64),
                ("count".to_string(), arrow::datatypes::DataType::Int64),
            ],
        );
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"tag_host"));
        assert!(field_names.contains(&"tag_region"));
        assert!(field_names.contains(&"usage"));
        assert!(field_names.contains(&"count"));
    }

    #[test]
    fn test_field_value_to_data_type() {
        assert_eq!(field_value_to_data_type(&FieldValue::Float(0.0)), arrow::datatypes::DataType::Float64);
        assert_eq!(field_value_to_data_type(&FieldValue::Integer(0)), arrow::datatypes::DataType::Int64);
        assert_eq!(field_value_to_data_type(&FieldValue::String("".into())), arrow::datatypes::DataType::Utf8);
        assert_eq!(field_value_to_data_type(&FieldValue::Boolean(false)), arrow::datatypes::DataType::Boolean);
    }
}
