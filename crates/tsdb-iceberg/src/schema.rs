use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::{IcebergError, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Schema {
    pub schema_id: i32,
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Field {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: IcebergType,
    pub doc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_default: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_default: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "value")]
pub enum IcebergType {
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "int")]
    Int,
    #[serde(rename = "long")]
    Long,
    #[serde(rename = "float")]
    Float,
    #[serde(rename = "double")]
    Double,
    #[serde(rename = "decimal")]
    Decimal { precision: u8, scale: u8 },
    #[serde(rename = "date")]
    Date,
    #[serde(rename = "time")]
    Time,
    #[serde(rename = "timestamp")]
    Timestamp,
    #[serde(rename = "timestamptz")]
    Timestamptz,
    #[serde(rename = "string")]
    String,
    #[serde(rename = "uuid")]
    Uuid,
    #[serde(rename = "binary")]
    Binary,
    #[serde(rename = "struct")]
    Struct { fields: Vec<Field> },
    #[serde(rename = "list")]
    List {
        element_id: i32,
        element_required: bool,
        element: Box<IcebergType>,
    },
    #[serde(rename = "map")]
    Map {
        key_id: i32,
        key: Box<IcebergType>,
        value_id: i32,
        value_required: bool,
        value: Box<IcebergType>,
    },
}

impl Schema {
    pub fn new(schema_id: i32, fields: Vec<Field>) -> Self {
        Self { schema_id, fields }
    }

    pub fn field_by_id(&self, id: i32) -> Option<&Field> {
        self.fields.iter().find(|f| f.id == id)
    }

    pub fn field_by_name(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    pub fn arrow_schema(&self) -> Result<SchemaRef> {
        let arrow_fields: Vec<ArrowField> = self
            .fields
            .iter()
            .map(|f| {
                let dt = iceberg_type_to_arrow(&f.field_type)?;
                Ok(ArrowField::new(&f.name, dt, !f.required))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(ArrowSchema::new(arrow_fields)))
    }

    pub fn from_datapoint_schema(
        _measurement: &str,
        tags: &BTreeMap<String, String>,
        fields: &BTreeMap<String, tsdb_arrow::schema::FieldValue>,
    ) -> Self {
        let mut iceberg_fields = Vec::new();
        let mut col_id: i32 = 1;

        iceberg_fields.push(Field {
            id: col_id,
            name: "timestamp".to_string(),
            required: true,
            field_type: IcebergType::Timestamptz,
            doc: None,
            initial_default: None,
            write_default: None,
        });
        col_id += 1;

        for key in tags.keys() {
            iceberg_fields.push(Field {
                id: col_id,
                name: format!("tag_{}", key),
                required: false,
                field_type: IcebergType::String,
                doc: Some(format!("tag: {}", key)),
                initial_default: None,
                write_default: None,
            });
            col_id += 1;
        }

        col_id = 1000;
        for (name, value) in fields {
            let field_type = match value {
                tsdb_arrow::schema::FieldValue::Float(_) => IcebergType::Double,
                tsdb_arrow::schema::FieldValue::Integer(_) => IcebergType::Long,
                tsdb_arrow::schema::FieldValue::String(_) => IcebergType::String,
                tsdb_arrow::schema::FieldValue::Boolean(_) => IcebergType::Boolean,
            };
            iceberg_fields.push(Field {
                id: col_id,
                name: name.clone(),
                required: false,
                field_type,
                doc: None,
                initial_default: None,
                write_default: None,
            });
            col_id += 1;
        }

        Self {
            schema_id: 0,
            fields: iceberg_fields,
        }
    }

    pub fn highest_field_id(&self) -> i32 {
        self.fields.iter().map(|f| f.id).max().unwrap_or(0)
    }
}

fn iceberg_type_to_arrow(itype: &IcebergType) -> Result<DataType> {
    match itype {
        IcebergType::Boolean => Ok(DataType::Boolean),
        IcebergType::Int => Ok(DataType::Int32),
        IcebergType::Long => Ok(DataType::Int64),
        IcebergType::Float => Ok(DataType::Float32),
        IcebergType::Double => Ok(DataType::Float64),
        IcebergType::Decimal { precision, scale } => {
            Ok(DataType::Decimal128(*precision, *scale as i8))
        }
        IcebergType::Date => Ok(DataType::Date32),
        IcebergType::Time => Ok(DataType::Time64(TimeUnit::Microsecond)),
        IcebergType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        IcebergType::Timestamptz => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        IcebergType::String => Ok(DataType::Utf8),
        IcebergType::Uuid => Ok(DataType::FixedSizeBinary(16)),
        IcebergType::Binary => Ok(DataType::Binary),
        IcebergType::Struct { fields } => {
            let arrow_fields: Vec<ArrowField> = fields
                .iter()
                .map(|f| {
                    let dt = iceberg_type_to_arrow(&f.field_type)?;
                    Ok(ArrowField::new(&f.name, dt, !f.required))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(DataType::Struct(arrow_fields.into()))
        }
        IcebergType::List {
            element_id: _,
            element_required,
            element,
        } => {
            let dt = iceberg_type_to_arrow(element)?;
            Ok(DataType::List(Arc::new(ArrowField::new(
                "item",
                dt,
                !element_required,
            ))))
        }
        IcebergType::Map {
            key_id: _,
            key,
            value_id: _,
            value_required,
            value,
        } => {
            let key_dt = iceberg_type_to_arrow(key)?;
            let val_dt = iceberg_type_to_arrow(value)?;
            Ok(DataType::Map(
                Arc::new(ArrowField::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            ArrowField::new("key", key_dt, false),
                            ArrowField::new("value", val_dt, !value_required),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SchemaChange {
    AddField {
        parent_id: Option<i32>,
        name: String,
        #[serde(rename = "type")]
        field_type: IcebergType,
        required: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        write_default: Option<serde_json::Value>,
    },
    DeleteField {
        field_id: i32,
    },
    RenameField {
        field_id: i32,
        new_name: String,
    },
    PromoteType {
        field_id: i32,
        new_type: IcebergType,
    },
    MoveField {
        field_id: i32,
        new_position: usize,
    },
}

impl Schema {
    pub fn apply_change(&self, change: SchemaChange) -> Result<Schema> {
        let mut new_fields = self.fields.clone();
        match change {
            SchemaChange::AddField {
                parent_id: _,
                name,
                field_type,
                required,
                write_default,
            } => {
                let next_id = self.highest_field_id() + 1;
                new_fields.push(Field {
                    id: next_id,
                    name,
                    required,
                    field_type,
                    doc: None,
                    initial_default: None,
                    write_default,
                });
            }
            SchemaChange::DeleteField { field_id } => {
                new_fields.retain(|f| f.id != field_id);
            }
            SchemaChange::RenameField { field_id, new_name } => {
                if let Some(f) = new_fields.iter_mut().find(|f| f.id == field_id) {
                    f.name = new_name;
                }
            }
            SchemaChange::PromoteType { field_id, new_type } => {
                if let Some(f) = new_fields.iter_mut().find(|f| f.id == field_id) {
                    match (&f.field_type, &new_type) {
                        (IcebergType::Int, IcebergType::Long) => f.field_type = new_type,
                        (IcebergType::Float, IcebergType::Double) => f.field_type = new_type,
                        _ => {
                            return Err(IcebergError::Schema(format!(
                                "cannot promote {:?} to {:?}",
                                f.field_type, new_type
                            )))
                        }
                    }
                }
            }
            SchemaChange::MoveField {
                field_id,
                new_position,
            } => {
                if let Some(idx) = new_fields.iter().position(|f| f.id == field_id) {
                    let field = new_fields.remove(idx);
                    let pos = new_position.min(new_fields.len());
                    new_fields.insert(pos, field);
                }
            }
        }
        Ok(Schema {
            schema_id: self.schema_id + 1,
            fields: new_fields,
        })
    }

    pub fn apply_changes(&self, changes: Vec<SchemaChange>) -> Result<Schema> {
        let mut schema = self.clone();
        for change in changes {
            schema = schema.apply_change(change)?;
        }
        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tsdb_arrow::schema::FieldValue;

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
                    id: 3,
                    name: "count".to_string(),
                    required: false,
                    field_type: IcebergType::Int,
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

    #[test]
    fn test_schema_serde_roundtrip() {
        let schema = make_test_schema();
        let json = serde_json::to_string(&schema).unwrap();
        let restored: Schema = serde_json::from_str(&json).unwrap();
        assert_eq!(schema, restored);
    }

    #[test]
    fn test_schema_to_arrow() {
        let schema = make_test_schema();
        let arrow = schema.arrow_schema().unwrap();
        assert_eq!(arrow.fields().len(), 4);
        assert_eq!(arrow.field(0).name(), "timestamp");
        assert_eq!(arrow.field(1).name(), "tag_host");
        assert_eq!(arrow.field(2).name(), "count");
        assert_eq!(arrow.field(3).name(), "usage");
    }

    #[test]
    fn test_field_lookup() {
        let schema = make_test_schema();
        assert!(schema.field_by_id(1).is_some());
        assert!(schema.field_by_id(999).is_none());
        assert!(schema.field_by_name("tag_host").is_some());
        assert!(schema.field_by_name("nonexistent").is_none());
    }

    #[test]
    fn test_schema_from_datapoint() {
        let mut tags = BTreeMap::new();
        tags.insert("host".to_string(), "s1".to_string());
        let mut fields = BTreeMap::new();
        fields.insert("usage".to_string(), FieldValue::Float(0.5));

        let schema = Schema::from_datapoint_schema("cpu", &tags, &fields);
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.field_by_name("timestamp").unwrap().id, 1);
        assert_eq!(schema.field_by_name("tag_host").unwrap().id, 2);
        assert_eq!(schema.field_by_name("usage").unwrap().id, 1000);
    }

    #[test]
    fn test_apply_add_field() {
        let schema = make_test_schema();
        let new = schema
            .apply_change(SchemaChange::AddField {
                parent_id: None,
                name: "idle".to_string(),
                field_type: IcebergType::Double,
                required: false,
                write_default: None,
            })
            .unwrap();
        assert_eq!(new.schema_id, 1);
        assert_eq!(new.fields.len(), 5);
        assert!(new.field_by_name("idle").is_some());
    }

    #[test]
    fn test_apply_delete_field() {
        let schema = make_test_schema();
        let new = schema
            .apply_change(SchemaChange::DeleteField { field_id: 2 })
            .unwrap();
        assert_eq!(new.fields.len(), 3);
        assert!(new.field_by_name("tag_host").is_none());
    }

    #[test]
    fn test_apply_rename_field() {
        let schema = make_test_schema();
        let new = schema
            .apply_change(SchemaChange::RenameField {
                field_id: 2,
                new_name: "tag_server".to_string(),
            })
            .unwrap();
        assert!(new.field_by_name("tag_server").is_some());
    }

    #[test]
    fn test_apply_promote_type() {
        let schema = make_test_schema();
        let new = schema
            .apply_change(SchemaChange::PromoteType {
                field_id: 3,
                new_type: IcebergType::Long,
            })
            .unwrap();
        assert_eq!(new.field_by_id(3).unwrap().field_type, IcebergType::Long);
    }

    #[test]
    fn test_highest_field_id() {
        let schema = make_test_schema();
        assert_eq!(schema.highest_field_id(), 1000);
    }
}
