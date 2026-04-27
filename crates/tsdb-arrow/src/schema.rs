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

pub fn compact_tsdb_schema_from_datapoints(datapoints: &[DataPoint]) -> SchemaRef {
    if datapoints.is_empty() {
        return compact_tsdb_schema("unknown", &[], &[]);
    }

    let measurement = &datapoints[0].measurement;

    let mut tag_key_set = std::collections::BTreeSet::new();
    let mut field_type_map: std::collections::BTreeMap<String, DataType> =
        std::collections::BTreeMap::new();

    for dp in datapoints {
        for k in dp.tags.keys() {
            tag_key_set.insert(k.clone());
        }
        for (k, v) in &dp.fields {
            field_type_map
                .entry(k.clone())
                .or_insert_with(|| field_value_to_data_type(v));
        }
    }

    let tag_keys: Vec<String> = tag_key_set.into_iter().collect();
    let field_types: Vec<(String, DataType)> = field_type_map.into_iter().collect();

    compact_tsdb_schema(measurement, &tag_keys, &field_types)
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
use std::ops::{Deref, DerefMut};

/// 标签集合: 有序键值对 (BTreeMap 保证序列化顺序一致)
///
/// 封装 BTreeMap 提供验证和领域语义。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Tags(BTreeMap<String, String>);

impl Tags {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.0.insert(key.into(), value.into());
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.0.get(key)
    }

    pub fn keys(&self) -> std::collections::btree_map::Keys<'_, String, String> {
        self.0.keys()
    }

    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, String, String> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
}

impl Default for Tags {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for Tags {
    type Target = BTreeMap<String, String>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Tags {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<BTreeMap<String, String>> for Tags {
    fn from(map: BTreeMap<String, String>) -> Self {
        Self(map)
    }
}

impl FromIterator<(String, String)> for Tags {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl IntoIterator for Tags {
    type Item = (String, String);
    type IntoIter = std::collections::btree_map::IntoIter<String, String>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Tags {
    type Item = (&'a String, &'a String);
    type IntoIter = std::collections::btree_map::Iter<'a, String, String>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// 字段集合: 有序键值对
///
/// 封装 BTreeMap 提供验证和领域语义。
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Fields(BTreeMap<String, FieldValue>);

impl Fields {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn insert(&mut self, key: impl Into<String>, value: FieldValue) {
        self.0.insert(key.into(), value);
    }

    pub fn get(&self, key: &str) -> Option<&FieldValue> {
        self.0.get(key)
    }

    pub fn keys(&self) -> std::collections::btree_map::Keys<'_, String, FieldValue> {
        self.0.keys()
    }

    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, String, FieldValue> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
}

impl Default for Fields {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for Fields {
    type Target = BTreeMap<String, FieldValue>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Fields {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<BTreeMap<String, FieldValue>> for Fields {
    fn from(map: BTreeMap<String, FieldValue>) -> Self {
        Self(map)
    }
}

impl FromIterator<(String, FieldValue)> for Fields {
    fn from_iter<I: IntoIterator<Item = (String, FieldValue)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl IntoIterator for Fields {
    type Item = (String, FieldValue);
    type IntoIter = std::collections::btree_map::IntoIter<String, FieldValue>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Fields {
    type Item = (&'a String, &'a FieldValue);
    type IntoIter = std::collections::btree_map::Iter<'a, String, FieldValue>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

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
                if v > i64::MAX as u64 {
                    Err(de::Error::custom("u64 value overflows i64 range"))
                } else {
                    Ok(FieldValue::Integer(v as i64))
                }
            }

            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<FieldValue, E> {
                Ok(FieldValue::String(v.to_string()))
            }

            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<FieldValue, E> {
                Ok(FieldValue::Boolean(v))
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<FieldValue, A::Error> {
                let (key, value): (String, serde_json::Value) = map
                    .next_entry()?
                    .ok_or_else(|| de::Error::custom("empty map is not a valid FieldValue"))?;
                match key.as_str() {
                    "Float" => value
                        .as_f64()
                        .map(FieldValue::Float)
                        .ok_or_else(|| de::Error::custom("invalid Float value")),
                    "Integer" => value
                        .as_i64()
                        .map(FieldValue::Integer)
                        .ok_or_else(|| de::Error::custom("invalid Integer value")),
                    "String" => value
                        .as_str()
                        .map(|s| FieldValue::String(s.to_string()))
                        .ok_or_else(|| de::Error::custom("invalid String value")),
                    "Boolean" => value
                        .as_bool()
                        .map(FieldValue::Boolean)
                        .ok_or_else(|| de::Error::custom("invalid Boolean value")),
                    other => Err(de::Error::custom(format!(
                        "unknown FieldValue variant: {}",
                        other
                    ))),
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

    /// 验证数据点的完整性
    ///
    /// 规则:
    /// - measurement 不能为空
    /// - 至少有一个字段
    /// - 标签键不能为空字符串
    /// - 字段键不能为空字符串
    /// - NaN/Inf 浮点值不允许
    pub fn validate(&self) -> std::result::Result<(), DataPointError> {
        if self.measurement.is_empty() || self.measurement.trim().is_empty() {
            return Err(DataPointError::EmptyMeasurement);
        }
        if self.fields.is_empty() {
            return Err(DataPointError::NoFields);
        }
        for key in self.tags.keys() {
            if key.is_empty() {
                return Err(DataPointError::EmptyTagKey);
            }
        }
        for (key, value) in &self.fields {
            if key.is_empty() {
                return Err(DataPointError::EmptyFieldKey);
            }
            if let FieldValue::Float(f) = value {
                if f.is_nan() || f.is_infinite() {
                    return Err(DataPointError::InvalidFloatValue {
                        field: key.clone(),
                        value: *f,
                    });
                }
            }
        }
        Ok(())
    }
}

/// 数据点验证错误
#[derive(Debug, Clone, PartialEq)]
pub enum DataPointError {
    EmptyMeasurement,
    NoFields,
    EmptyTagKey,
    EmptyFieldKey,
    InvalidFloatValue { field: String, value: f64 },
}

impl std::fmt::Display for DataPointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataPointError::EmptyMeasurement => write!(f, "measurement name cannot be empty"),
            DataPointError::NoFields => write!(f, "data point must have at least one field"),
            DataPointError::EmptyTagKey => write!(f, "tag key cannot be empty string"),
            DataPointError::EmptyFieldKey => write!(f, "field key cannot be empty string"),
            DataPointError::InvalidFloatValue { field, value } => {
                write!(f, "field '{}' has invalid float value: {}", field, value)
            }
        }
    }
}

impl std::error::Error for DataPointError {}

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
        assert_eq!(FieldValue::Float(2.78).as_f64(), Some(2.78));
        assert_eq!(FieldValue::Integer(42).as_f64(), Some(42.0));
        assert_eq!(FieldValue::String("hello".into()).as_f64(), None);
        assert_eq!(FieldValue::Boolean(true).as_f64(), None);
    }

    #[test]
    fn test_field_value_as_i64() {
        assert_eq!(FieldValue::Integer(42).as_i64(), Some(42));
        assert_eq!(FieldValue::Float(2.78).as_i64(), Some(2));
        assert_eq!(FieldValue::String("hello".into()).as_i64(), None);
        assert_eq!(FieldValue::Boolean(false).as_i64(), None);
    }

    #[test]
    fn test_field_value_as_str() {
        assert_eq!(FieldValue::String("hello".into()).as_str(), Some("hello"));
        assert_eq!(FieldValue::Float(2.78).as_str(), None);
        assert_eq!(FieldValue::Integer(42).as_str(), None);
        assert_eq!(FieldValue::Boolean(true).as_str(), None);
    }

    #[test]
    fn test_field_value_as_bool() {
        assert_eq!(FieldValue::Boolean(true).as_bool(), Some(true));
        assert_eq!(FieldValue::Boolean(false).as_bool(), Some(false));
        assert_eq!(FieldValue::Float(2.78).as_bool(), None);
        assert_eq!(FieldValue::Integer(42).as_bool(), None);
    }

    #[test]
    fn test_field_value_serde_roundtrip_float() {
        let original = FieldValue::Float(2.78);
        let json = serde_json::to_string(&original).unwrap();
        let restored: FieldValue = serde_json::from_str(&json).unwrap();
        assert!(matches!(restored, FieldValue::Float(v) if (v - 2.78).abs() < f64::EPSILON));
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
        assert_eq!(
            FieldType::from(&FieldValue::String("".into())),
            FieldType::String
        );
        assert_eq!(
            FieldType::from(&FieldValue::Boolean(false)),
            FieldType::Boolean
        );
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
        let dp = DataPoint::new("mem", 2000).with_tag("host", "srv1");
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
        assert_eq!(
            field_value_to_data_type(&FieldValue::Float(0.0)),
            arrow::datatypes::DataType::Float64
        );
        assert_eq!(
            field_value_to_data_type(&FieldValue::Integer(0)),
            arrow::datatypes::DataType::Int64
        );
        assert_eq!(
            field_value_to_data_type(&FieldValue::String("".into())),
            arrow::datatypes::DataType::Utf8
        );
        assert_eq!(
            field_value_to_data_type(&FieldValue::Boolean(false)),
            arrow::datatypes::DataType::Boolean
        );
    }

    #[test]
    fn test_validate_valid_datapoint() {
        let dp = DataPoint::new("cpu", 1000)
            .with_tag("host", "server01")
            .with_field("usage", FieldValue::Float(0.5));
        assert!(dp.validate().is_ok());
    }

    #[test]
    fn test_validate_empty_measurement() {
        let dp = DataPoint::new("", 1000).with_field("value", FieldValue::Float(1.0));
        assert_eq!(dp.validate().unwrap_err(), DataPointError::EmptyMeasurement);
    }

    #[test]
    fn test_validate_whitespace_measurement() {
        let dp = DataPoint::new("  ", 1000).with_field("value", FieldValue::Float(1.0));
        assert_eq!(dp.validate().unwrap_err(), DataPointError::EmptyMeasurement);
    }

    #[test]
    fn test_validate_no_fields() {
        let dp = DataPoint::new("cpu", 1000);
        assert_eq!(dp.validate().unwrap_err(), DataPointError::NoFields);
    }

    #[test]
    fn test_validate_nan_float() {
        let dp = DataPoint::new("cpu", 1000).with_field("value", FieldValue::Float(f64::NAN));
        let err = dp.validate().unwrap_err();
        assert!(matches!(err, DataPointError::InvalidFloatValue { .. }));
    }

    #[test]
    fn test_validate_infinite_float() {
        let dp = DataPoint::new("cpu", 1000).with_field("value", FieldValue::Float(f64::INFINITY));
        let err = dp.validate().unwrap_err();
        assert!(matches!(err, DataPointError::InvalidFloatValue { .. }));
    }

    #[test]
    fn test_validate_no_tags_is_valid() {
        let dp = DataPoint::new("cpu", 1000).with_field("usage", FieldValue::Float(0.5));
        assert!(dp.validate().is_ok());
    }

    #[test]
    fn test_validate_multiple_fields() {
        let dp = DataPoint::new("cpu", 1000)
            .with_tag("host", "s1")
            .with_field("usage", FieldValue::Float(0.5))
            .with_field("count", FieldValue::Integer(42));
        assert!(dp.validate().is_ok());
    }

    #[test]
    fn test_field_value_u64_overflow_rejected() {
        let original = FieldValue::Integer(i64::MAX);
        let json = serde_json::to_string(&original).unwrap();
        let overflow_json = json.replace(&i64::MAX.to_string(), &u64::MAX.to_string());
        let result: Result<FieldValue, _> = serde_json::from_str(&overflow_json);
        assert!(result.is_err(), "u64 overflow should be rejected");
    }

    #[test]
    fn test_field_value_large_i64_roundtrip() {
        let original = FieldValue::Integer(i64::MAX);
        let json = serde_json::to_string(&original).unwrap();
        let result: Result<FieldValue, _> = serde_json::from_str(&json);
        assert!(result.is_ok());
        if let FieldValue::Integer(v) = result.unwrap() {
            assert_eq!(v, i64::MAX);
        } else {
            panic!("expected Integer");
        }
    }

    #[test]
    fn test_field_value_negative_i64_roundtrip() {
        let original = FieldValue::Integer(i64::MIN);
        let json = serde_json::to_string(&original).unwrap();
        let result: Result<FieldValue, _> = serde_json::from_str(&json);
        assert!(result.is_ok());
        if let FieldValue::Integer(v) = result.unwrap() {
            assert_eq!(v, i64::MIN);
        } else {
            panic!("expected Integer");
        }
    }

    #[test]
    fn test_datapoint_with_many_tags() {
        let mut dp = DataPoint::new("cpu", 1000).with_field("usage", FieldValue::Float(0.5));
        for i in 0..20 {
            dp = dp.with_tag(format!("tag_{}", i), format!("val_{}", i));
        }
        assert_eq!(dp.tags.len(), 20);
        assert!(dp.validate().is_ok());
    }

    #[test]
    fn test_datapoint_with_many_fields() {
        let mut dp = DataPoint::new("cpu", 1000).with_tag("host", "s1");
        for i in 0..20 {
            dp = dp.with_field(format!("field_{}", i), FieldValue::Float(i as f64));
        }
        assert_eq!(dp.fields.len(), 20);
        assert!(dp.validate().is_ok());
    }

    #[test]
    fn test_series_key_deterministic() {
        let dp1 = DataPoint::new("cpu", 1000)
            .with_tag("a", "1")
            .with_tag("b", "2")
            .with_field("v", FieldValue::Float(1.0));

        let dp2 = DataPoint::new("cpu", 1000)
            .with_tag("b", "2")
            .with_tag("a", "1")
            .with_field("v", FieldValue::Float(1.0));

        let key1 = dp1.series_key();
        let key2 = dp2.series_key();
        assert_eq!(key1, key2, "series_key should be order-independent");
    }
}
