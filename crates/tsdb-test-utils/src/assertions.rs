/// 断言两个 DataPoint 列表相等
///
/// 比较逻辑:
/// 1. 按时间戳排序后逐个比较
/// 2. 比较 timestamp、measurement、tags
/// 3. 比较字段数量和每个字段的值 (Float 使用 1e-10 容差)
#[macro_export]
macro_rules! assert_datapoints_eq {
    ($expected:expr, $actual:expr) => {
        let mut e: Vec<_> = $expected.iter().collect();
        let mut a: Vec<_> = $actual.iter().collect();
        e.sort_by_key(|dp| dp.timestamp);
        a.sort_by_key(|dp| dp.timestamp);
        assert_eq!(e.len(), a.len(), "datapoint count mismatch");
        for (i, (exp, act)) in e.iter().zip(a.iter()).enumerate() {
            assert_eq!(
                exp.timestamp, act.timestamp,
                "timestamp mismatch at index {}",
                i
            );
            assert_eq!(
                exp.measurement, act.measurement,
                "measurement mismatch at index {}",
                i
            );
            assert_eq!(exp.tags, act.tags, "tags mismatch at index {}", i);
            assert_eq!(
                exp.fields.len(),
                act.fields.len(),
                "fields count mismatch at index {}",
                i
            );
            for (k, v) in &exp.fields {
                if let Some(av) = act.fields.get(k) {
                    match (v, av) {
                        (FieldValue::Float(a), FieldValue::Float(b)) => {
                            assert!(
                                (a - b).abs() < 1e-10,
                                "field '{}' float mismatch at index {}: {} vs {}",
                                k,
                                i,
                                a,
                                b
                            );
                        }
                        _ => assert_eq!(v, av, "field '{}' mismatch at index {}", k, i),
                    }
                } else {
                    panic!("field '{}' missing in actual at index {}", k, i);
                }
            }
        }
    };
}

/// 断言 DataPoint → RecordBatch → DataPoint 往返转换一致
///
/// 先将数据点转换为 Arrow RecordBatch，再转换回来，
/// 最后用 `assert_datapoints_eq!` 验证数据完整性。
#[macro_export]
macro_rules! assert_roundtrip {
    ($datapoints:expr, $schema:expr) => {
        let batch =
            $crate::tsdb_arrow::converter::datapoints_to_record_batch($datapoints, $schema.clone())
                .expect("datapoints_to_record_batch failed");
        let restored = $crate::tsdb_arrow::converter::record_batch_to_datapoints(&batch)
            .expect("record_batch_to_datapoints failed");
        assert_datapoints_eq!($datapoints, restored);
    };
}
