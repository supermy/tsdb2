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
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum FieldValue {
    Float(f64),
    Integer(i64),
    String(String),
    Boolean(bool),
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
