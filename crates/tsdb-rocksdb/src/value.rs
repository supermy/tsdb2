use crate::error::Result;
use crate::error::TsdbRocksDbError;
use tsdb_arrow::schema::FieldValue;

/// 字段类型标签: Float64
const FIELD_TYPE_FLOAT64: u8 = 0;
/// 字段类型标签: Int64
const FIELD_TYPE_INT64: u8 = 1;
/// 字段类型标签: Utf8 字符串
const FIELD_TYPE_UTF8: u8 = 2;
/// 字段类型标签: Boolean
const FIELD_TYPE_BOOLEAN: u8 = 3;

/// 将 u16 编码为大端序并追加到缓冲区
fn encode_u16(buf: &mut Vec<u8>, val: u16) {
    buf.extend_from_slice(&val.to_be_bytes());
}

/// 从字节数组的大端序中解码 u16，推进游标
fn decode_u16(data: &[u8], cursor: &mut usize) -> Result<u16> {
    if *cursor + 2 > data.len() {
        return Err(TsdbRocksDbError::InvalidValue("truncated u16".to_string()));
    }
    let val = u16::from_be_bytes([data[*cursor], data[*cursor + 1]]);
    *cursor += 2;
    Ok(val)
}

/// 编码字符串: [长度: u16 BE][utf8 字节]
fn encode_str(buf: &mut Vec<u8>, s: &str) {
    encode_u16(buf, s.len() as u16);
    buf.extend_from_slice(s.as_bytes());
}

/// 解码字符串，推进游标
fn decode_str(data: &[u8], cursor: &mut usize) -> Result<String> {
    let len = decode_u16(data, cursor)? as usize;
    if *cursor + len > data.len() {
        return Err(TsdbRocksDbError::InvalidValue(
            "truncated string".to_string(),
        ));
    }
    let s = String::from_utf8(data[*cursor..*cursor + len].to_vec())?;
    *cursor += len;
    Ok(s)
}

/// 编码单个字段值
///
/// 编码格式: [type_tag: u8][value_bytes]
/// - Float64:  [0x00][8 bytes f64 BE]
/// - Int64:    [0x01][8 bytes i64 BE]
/// - Utf8:     [0x02][len: u16][utf8 bytes]
/// - Boolean:  [0x03][1 byte: 0x00=false, 0x01=true]
fn encode_field_value(buf: &mut Vec<u8>, fv: &FieldValue) {
    match fv {
        FieldValue::Float(v) => {
            buf.push(FIELD_TYPE_FLOAT64);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        FieldValue::Integer(v) => {
            buf.push(FIELD_TYPE_INT64);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        FieldValue::String(v) => {
            buf.push(FIELD_TYPE_UTF8);
            encode_u16(buf, v.len() as u32 as u16);
            buf.extend_from_slice(v.as_bytes());
        }
        FieldValue::Boolean(v) => {
            buf.push(FIELD_TYPE_BOOLEAN);
            buf.push(if *v { 1 } else { 0 });
        }
    }
}

/// 解码单个字段值，推进游标
fn decode_field_value(data: &[u8], cursor: &mut usize) -> Result<FieldValue> {
    if *cursor >= data.len() {
        return Err(TsdbRocksDbError::InvalidValue(
            "truncated field type".to_string(),
        ));
    }
    let field_type = data[*cursor];
    *cursor += 1;
    match field_type {
        FIELD_TYPE_FLOAT64 => {
            if *cursor + 8 > data.len() {
                return Err(TsdbRocksDbError::InvalidValue("truncated f64".to_string()));
            }
            let val = f64::from_be_bytes(
                data[*cursor..*cursor + 8]
                    .try_into()
                    .map_err(|_| TsdbRocksDbError::InvalidValue("f64 parse".into()))?,
            );
            *cursor += 8;
            Ok(FieldValue::Float(val))
        }
        FIELD_TYPE_INT64 => {
            if *cursor + 8 > data.len() {
                return Err(TsdbRocksDbError::InvalidValue("truncated i64".to_string()));
            }
            let val = i64::from_be_bytes(
                data[*cursor..*cursor + 8]
                    .try_into()
                    .map_err(|_| TsdbRocksDbError::InvalidValue("i64 parse".into()))?,
            );
            *cursor += 8;
            Ok(FieldValue::Integer(val))
        }
        FIELD_TYPE_UTF8 => {
            let s = decode_str(data, cursor)?;
            Ok(FieldValue::String(s))
        }
        FIELD_TYPE_BOOLEAN => {
            if *cursor >= data.len() {
                return Err(TsdbRocksDbError::InvalidValue("truncated bool".to_string()));
            }
            let val = data[*cursor] != 0;
            *cursor += 1;
            Ok(FieldValue::Boolean(val))
        }
        _ => Err(TsdbRocksDbError::InvalidValue(format!(
            "unknown field type: {}",
            field_type
        ))),
    }
}

/// 将字段集合编码为紧凑二进制格式
///
/// 编码格式:
/// ```text
/// [num_fields: u16]
/// [field_1: [name_len: u16][name_bytes][type_tag: u8][value_bytes]]
/// [field_2: ...]
/// ...
/// ```
pub fn encode_fields(fields: &tsdb_arrow::schema::Fields) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    encode_u16(&mut buf, fields.len() as u16);
    for (name, value) in fields.iter() {
        encode_str(&mut buf, name);
        encode_field_value(&mut buf, value);
    }
    buf
}

/// 从紧凑二进制格式解码字段集合
pub fn decode_fields(data: &[u8]) -> Result<tsdb_arrow::schema::Fields> {
    let mut cursor = 0;
    let num_fields = decode_u16(data, &mut cursor)?;
    let mut fields = tsdb_arrow::schema::Fields::new();
    for _ in 0..num_fields {
        let name = decode_str(data, &mut cursor)?;
        let value = decode_field_value(data, &mut cursor)?;
        fields.insert(name, value);
    }
    Ok(fields)
}

/// 列投影解码: 只解码指定字段名的字段, 跳过不需要的列
///
/// 比全量 decode_fields + 过滤更高效: 跳过不需要的字段值解码,
/// 减少内存分配和 CPU 开销。适用于只需要部分字段的查询场景。
pub fn decode_fields_projection(
    data: &[u8],
    projection: &std::collections::HashSet<&str>,
) -> Result<tsdb_arrow::schema::Fields> {
    let mut cursor = 0;
    let num_fields = decode_u16(data, &mut cursor)?;
    let mut fields = tsdb_arrow::schema::Fields::new();
    for _ in 0..num_fields {
        let name = decode_str(data, &mut cursor)?;
        if projection.contains(name.as_str()) {
            let value = decode_field_value(data, &mut cursor)?;
            fields.insert(name, value);
        } else {
            skip_field_value(data, &mut cursor)?;
        }
    }
    Ok(fields)
}

/// 跳过单个字段值 (不解码), 推进游标
fn skip_field_value(data: &[u8], cursor: &mut usize) -> Result<()> {
    if *cursor >= data.len() {
        return Err(TsdbRocksDbError::InvalidValue(
            "truncated field type".to_string(),
        ));
    }
    let field_type = data[*cursor];
    *cursor += 1;
    match field_type {
        FIELD_TYPE_FLOAT64 | FIELD_TYPE_INT64 => {
            *cursor += 8;
        }
        FIELD_TYPE_UTF8 => {
            let len = decode_u16(data, cursor)? as usize;
            *cursor += len;
        }
        FIELD_TYPE_BOOLEAN => {
            *cursor += 1;
        }
        _ => {
            return Err(TsdbRocksDbError::InvalidValue(format!(
                "unknown field type: {}",
                field_type
            )))
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_fields(pairs: &[(&str, FieldValue)]) -> tsdb_arrow::schema::Fields {
        let mut fields = tsdb_arrow::schema::Fields::new();
        for (name, value) in pairs {
            fields.insert(name.to_string(), value.clone());
        }
        fields
    }

    #[test]
    fn test_value_encode_decode_roundtrip_float() {
        let fields = make_fields(&[
            ("usage", FieldValue::Float(2.71)),
            ("count", FieldValue::Integer(42)),
        ]);
        let encoded = encode_fields(&fields);
        let decoded = decode_fields(&encoded).unwrap();
        assert_eq!(decoded.get("usage").unwrap(), &FieldValue::Float(2.71));
        assert_eq!(decoded.get("count").unwrap(), &FieldValue::Integer(42));
    }

    #[test]
    fn test_value_encode_decode_all_types() {
        let fields = make_fields(&[
            ("f", FieldValue::Float(-1.5)),
            ("i", FieldValue::Integer(i64::MAX)),
            ("s", FieldValue::String("hello world".to_string())),
            ("b", FieldValue::Boolean(true)),
        ]);
        let encoded = encode_fields(&fields);
        let decoded = decode_fields(&encoded).unwrap();

        assert_eq!(*decoded.get("f").unwrap(), FieldValue::Float(-1.5));
        assert_eq!(*decoded.get("i").unwrap(), FieldValue::Integer(i64::MAX));
        assert_eq!(
            *decoded.get("s").unwrap(),
            FieldValue::String("hello world".to_string())
        );
        assert_eq!(*decoded.get("b").unwrap(), FieldValue::Boolean(true));
    }

    #[test]
    fn test_value_empty_fields() {
        let fields = tsdb_arrow::schema::Fields::new();
        let encoded = encode_fields(&fields);
        let decoded = decode_fields(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_value_single_field() {
        let fields = make_fields(&[("temp", FieldValue::Float(36.6))]);
        let encoded = encode_fields(&fields);
        let decoded = decode_fields(&encoded).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(*decoded.get("temp").unwrap(), FieldValue::Float(36.6));
    }

    #[test]
    fn test_value_unicode_field_name_and_value() {
        let fields = make_fields(&[("温度", FieldValue::String("北京".to_string()))]);
        let encoded = encode_fields(&fields);
        let decoded = decode_fields(&encoded).unwrap();
        assert_eq!(
            *decoded.get("温度").unwrap(),
            FieldValue::String("北京".to_string())
        );
    }

    #[test]
    fn test_value_many_fields() {
        let mut fields = tsdb_arrow::schema::Fields::new();
        for i in 0..100 {
            fields.insert(format!("field_{}", i), FieldValue::Float(i as f64 * 0.1));
        }
        let encoded = encode_fields(&fields);
        let decoded = decode_fields(&encoded).unwrap();
        assert_eq!(decoded.len(), 100);
        for i in 0..100 {
            assert_eq!(
                *decoded.get(&format!("field_{}", i)).unwrap(),
                FieldValue::Float(i as f64 * 0.1)
            );
        }
    }

    #[test]
    fn test_value_truncated_data_error() {
        let result = decode_fields(&[0x00, 0x01]);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_invalid_type() {
        let data: Vec<u8> = vec![0x00, 0x01, 0x00, 0x01, 0x61, 0xFF];
        let result = decode_fields(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_projection_decode_single_field() {
        let fields = make_fields(&[
            ("usage", FieldValue::Float(2.71)),
            ("idle", FieldValue::Float(0.29)),
            ("count", FieldValue::Integer(42)),
            ("host", FieldValue::String("server01".to_string())),
        ]);
        let encoded = encode_fields(&fields);

        let projection: std::collections::HashSet<&str> = ["usage"].into();
        let decoded = decode_fields_projection(&encoded, &projection).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(*decoded.get("usage").unwrap(), FieldValue::Float(2.71));
        assert!(!decoded.contains_key("idle"));
        assert!(!decoded.contains_key("count"));
    }

    #[test]
    fn test_projection_decode_multiple_fields() {
        let fields = make_fields(&[
            ("usage", FieldValue::Float(2.71)),
            ("idle", FieldValue::Float(0.29)),
            ("count", FieldValue::Integer(42)),
        ]);
        let encoded = encode_fields(&fields);

        let projection: std::collections::HashSet<&str> = ["usage", "count"].into();
        let decoded = decode_fields_projection(&encoded, &projection).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(*decoded.get("usage").unwrap(), FieldValue::Float(2.71));
        assert_eq!(*decoded.get("count").unwrap(), FieldValue::Integer(42));
        assert!(!decoded.contains_key("idle"));
    }

    #[test]
    fn test_projection_decode_empty_projection() {
        let fields = make_fields(&[
            ("usage", FieldValue::Float(2.71)),
            ("count", FieldValue::Integer(42)),
        ]);
        let encoded = encode_fields(&fields);

        let projection: std::collections::HashSet<&str> = [].into();
        let decoded = decode_fields_projection(&encoded, &projection).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_projection_decode_nonexistent_field() {
        let fields = make_fields(&[("usage", FieldValue::Float(2.71))]);
        let encoded = encode_fields(&fields);

        let projection: std::collections::HashSet<&str> = ["nonexistent"].into();
        let decoded = decode_fields_projection(&encoded, &projection).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_projection_decode_all_fields() {
        let fields = make_fields(&[
            ("usage", FieldValue::Float(2.71)),
            ("count", FieldValue::Integer(42)),
        ]);
        let encoded = encode_fields(&fields);

        let projection: std::collections::HashSet<&str> = ["usage", "count"].into();
        let decoded = decode_fields_projection(&encoded, &projection).unwrap();
        let full_decoded = decode_fields(&encoded).unwrap();
        assert_eq!(decoded.len(), full_decoded.len());
    }

    #[test]
    fn test_projection_decode_consistency_with_full_decode() {
        let fields = make_fields(&[
            ("f1", FieldValue::Float(1.0)),
            ("f2", FieldValue::Integer(2)),
            ("f3", FieldValue::String("hello".to_string())),
            ("f4", FieldValue::Boolean(true)),
            ("f5", FieldValue::Float(3.15)),
        ]);
        let encoded = encode_fields(&fields);

        let projection: std::collections::HashSet<&str> = ["f2", "f5"].into();
        let projected = decode_fields_projection(&encoded, &projection).unwrap();
        let full = decode_fields(&encoded).unwrap();

        assert_eq!(*projected.get("f2").unwrap(), *full.get("f2").unwrap());
        assert_eq!(*projected.get("f5").unwrap(), *full.get("f5").unwrap());
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use tsdb_arrow::schema::Fields;

    fn field_value_strategy() -> BoxedStrategy<FieldValue> {
        prop_oneof![
            any::<f64>().prop_map(FieldValue::Float),
            any::<i64>().prop_map(FieldValue::Integer),
            ".*".prop_map(FieldValue::String),
            any::<bool>().prop_map(FieldValue::Boolean),
        ]
        .boxed()
    }

    proptest! {
        #[test]
        fn proptest_value_encode_decode_roundtrip(
            fields in prop::collection::btree_map(".*", field_value_strategy(), 0..20)
        ) {
            let fields: Fields = fields.into_iter().collect();
            let encoded = encode_fields(&fields);
            let decoded = decode_fields(&encoded).unwrap();
            prop_assert_eq!(decoded.len(), fields.len());
            for (k, v) in &fields {
                let decoded_v = decoded.get(k).unwrap();
                match (v, decoded_v) {
                    (FieldValue::Float(a), FieldValue::Float(b)) => {
                        if a.is_nan() && b.is_nan() {
                        } else {
                            prop_assert_eq!(a.to_bits(), b.to_bits());
                        }
                    }
                    _ => {
                        prop_assert_eq!(v.clone(), decoded_v.clone());
                    }
                }
            }
        }

        #[test]
        fn proptest_value_all_field_types(
            f in any::<f64>(),
            i in any::<i64>(),
            s in ".*",
            b in any::<bool>()
        ) {
            let mut fields = Fields::new();
            fields.insert("f".to_string(), FieldValue::Float(f));
            fields.insert("i".to_string(), FieldValue::Integer(i));
            fields.insert("s".to_string(), FieldValue::String(s.clone()));
            fields.insert("b".to_string(), FieldValue::Boolean(b));

            let encoded = encode_fields(&fields);
            let decoded = decode_fields(&encoded).unwrap();

            if !f.is_nan() {
                prop_assert_eq!(decoded.get("f").unwrap().clone(), FieldValue::Float(f));
            }
            prop_assert_eq!(decoded.get("i").unwrap().clone(), FieldValue::Integer(i));
            prop_assert_eq!(decoded.get("s").unwrap().clone(), FieldValue::String(s));
            prop_assert_eq!(decoded.get("b").unwrap().clone(), FieldValue::Boolean(b));
        }

        #[test]
        fn proptest_value_special_floats(v in prop_oneof![Just(f64::NAN), Just(f64::INFINITY), Just(f64::NEG_INFINITY), Just(0.0), Just(-0.0)]) {
            let mut fields = Fields::new();
            fields.insert("special".to_string(), FieldValue::Float(v));
            let encoded = encode_fields(&fields);
            let decoded = decode_fields(&encoded).unwrap();
            let decoded_v = decoded.get("special").unwrap();
            if let FieldValue::Float(dv) = decoded_v {
                if v.is_nan() {
                    prop_assert!(dv.is_nan());
                } else {
                    prop_assert_eq!(v.to_bits(), dv.to_bits());
                }
            } else {
                prop_assert!(false, "expected Float");
            }
        }
    }
}
