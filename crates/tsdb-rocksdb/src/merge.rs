use crate::value::{decode_fields, encode_fields};
use rocksdb::MergeOperands;
use std::collections::BTreeMap;
use tsdb_arrow::schema::{FieldValue, Fields};

/// 纯逻辑合并: 将已有字段与多个操作数合并
///
/// 合并语义: 字段集合的 **并集 (union)**, 同名字段后者覆盖前者.
pub fn merge_fields(existing: Option<&Fields>, operands: &[&Fields]) -> Fields {
    let mut merged: BTreeMap<String, FieldValue> = BTreeMap::new();

    if let Some(fields) = existing {
        for (k, v) in fields {
            merged.insert(k.clone(), v.clone());
        }
    }

    for op in operands {
        for (k, v) in *op {
            merged.insert(k.clone(), v.clone());
        }
    }

    merged.into_iter().collect()
}

/// 全量合并操作符
///
/// 合并语义: 字段集合的 **并集 (union)**, 同名字段后者覆盖前者.
///
/// 处理流程:
/// 1. 解码已有值 (existing_val) 中的字段
/// 2. 逐个解码 operands 中的字段, 依次合并到结果中
/// 3. 编码合并后的字段集合返回
///
/// 示例:
/// - existing = {cpu: 1.0}
/// - operand  = {mem: 2.0}
/// - result   = {cpu: 1.0, mem: 2.0}
///
/// - existing = {cpu: 1.0}
/// - operand  = {cpu: 99.0}
/// - result   = {cpu: 99.0}  (后者覆盖)
pub fn tsdb_full_merge(
    _key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let existing_fields: Option<Fields> = existing_val.and_then(|v| decode_fields(v).ok());
    let operand_fields: Vec<Fields> = operands
        .iter()
        .filter_map(|op| decode_fields(op).ok())
        .collect();
    let operand_refs: Vec<&Fields> = operand_fields.iter().collect();

    let merged = merge_fields(existing_fields.as_ref(), &operand_refs);
    Some(encode_fields(&merged))
}

/// 部分合并操作符 (降级策略)
///
/// 返回 None 表示 RocksDB 应在后续 Compaction 时
/// 调用 full_merge 重新处理, 而非在此阶段尝试合并.
/// 这是一种安全降级: 牺牲少量写入性能换取正确性保证.
pub fn tsdb_partial_merge(
    _key: &[u8],
    _existing_val: Option<&[u8]>,
    _operands: &MergeOperands,
) -> Option<Vec<u8>> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tsdb_arrow::schema::FieldValue;

    fn make_fields(pairs: &[(&str, FieldValue)]) -> Fields {
        let mut fields = Fields::new();
        for (name, value) in pairs {
            fields.insert(name.to_string(), value.clone());
        }
        fields
    }

    #[test]
    fn test_full_merge_single_operand() {
        let existing = make_fields(&[("cpu", FieldValue::Float(80.0))]);
        let operand = make_fields(&[("mem", FieldValue::Float(60.0))]);
        let result = merge_fields(Some(&existing), &[&operand]);
        assert_eq!(result.len(), 2);
        assert_eq!(*result.get("cpu").unwrap(), FieldValue::Float(80.0));
        assert_eq!(*result.get("mem").unwrap(), FieldValue::Float(60.0));
    }

    #[test]
    fn test_full_merge_multiple_operands() {
        let existing = make_fields(&[("cpu", FieldValue::Float(80.0))]);
        let op1 = make_fields(&[("mem", FieldValue::Float(60.0))]);
        let op2 = make_fields(&[("disk", FieldValue::Float(40.0))]);
        let result = merge_fields(Some(&existing), &[&op1, &op2]);
        assert_eq!(result.len(), 3);
        assert_eq!(*result.get("cpu").unwrap(), FieldValue::Float(80.0));
        assert_eq!(*result.get("mem").unwrap(), FieldValue::Float(60.0));
        assert_eq!(*result.get("disk").unwrap(), FieldValue::Float(40.0));
    }

    #[test]
    fn test_full_merge_field_override() {
        let existing = make_fields(&[("cpu", FieldValue::Float(80.0))]);
        let operand = make_fields(&[("cpu", FieldValue::Float(99.0))]);
        let result = merge_fields(Some(&existing), &[&operand]);
        assert_eq!(result.len(), 1);
        assert_eq!(*result.get("cpu").unwrap(), FieldValue::Float(99.0));
    }

    #[test]
    fn test_full_merge_5_plus_operands() {
        let existing = make_fields(&[("base", FieldValue::Integer(0))]);
        let operands: Vec<Fields> = (1..=6)
            .map(|i| make_fields(&[(&format!("field_{}", i), FieldValue::Integer(i))]))
            .collect();
        let operand_refs: Vec<&Fields> = operands.iter().collect();

        let result = merge_fields(Some(&existing), &operand_refs);
        assert_eq!(result.len(), 7);
        for i in 1..=6 {
            assert_eq!(
                *result.get(&format!("field_{}", i)).unwrap(),
                FieldValue::Integer(i)
            );
        }
    }

    #[test]
    fn test_full_merge_no_existing() {
        let operand = make_fields(&[("cpu", FieldValue::Float(80.0))]);
        let result = merge_fields(None, &[&operand]);
        assert_eq!(result.len(), 1);
        assert_eq!(*result.get("cpu").unwrap(), FieldValue::Float(80.0));
    }

    #[test]
    fn test_full_merge_empty_operands() {
        let existing = make_fields(&[("cpu", FieldValue::Float(80.0))]);
        let result = merge_fields(Some(&existing), &[]);
        assert_eq!(result.len(), 1);
        assert_eq!(*result.get("cpu").unwrap(), FieldValue::Float(80.0));
    }

    #[test]
    fn test_full_merge_empty_fields_union() {
        let existing = Fields::new();
        let operand = make_fields(&[("cpu", FieldValue::Float(80.0))]);
        let result = merge_fields(Some(&existing), &[&operand]);
        assert_eq!(result.len(), 1);
        assert_eq!(*result.get("cpu").unwrap(), FieldValue::Float(80.0));
    }

    #[test]
    fn test_full_merge_no_existing_no_operands() {
        let result = merge_fields(None, &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_full_merge_later_operand_overrides_earlier() {
        let op1 = make_fields(&[("cpu", FieldValue::Float(10.0))]);
        let op2 = make_fields(&[("cpu", FieldValue::Float(20.0))]);
        let op3 = make_fields(&[("cpu", FieldValue::Float(30.0))]);
        let result = merge_fields(None, &[&op1, &op2, &op3]);
        assert_eq!(result.len(), 1);
        assert_eq!(*result.get("cpu").unwrap(), FieldValue::Float(30.0));
    }

    #[test]
    fn test_full_merge_mixed_types() {
        let existing = make_fields(&[("f", FieldValue::Float(1.0))]);
        let operand = make_fields(&[
            ("i", FieldValue::Integer(42)),
            ("s", FieldValue::String("hello".to_string())),
            ("b", FieldValue::Boolean(true)),
        ]);
        let result = merge_fields(Some(&existing), &[&operand]);
        assert_eq!(result.len(), 4);
        assert_eq!(*result.get("f").unwrap(), FieldValue::Float(1.0));
        assert_eq!(*result.get("i").unwrap(), FieldValue::Integer(42));
        assert_eq!(
            *result.get("s").unwrap(),
            FieldValue::String("hello".to_string())
        );
        assert_eq!(*result.get("b").unwrap(), FieldValue::Boolean(true));
    }
}
