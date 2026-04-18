use arrow::array::{Array, Int64Array, TimestampMicrosecondArray, TimestampMicrosecondBuilder};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

/// time_bucket 标量函数实现
///
/// 将时间戳按指定的时间间隔进行分桶 (向下取整到最近的桶边界)。
/// 常用于时序数据的按时间窗口聚合场景。
///
/// 分桶算法: `bucketed = (timestamp / bucket_size) * bucket_size`
#[derive(Debug)]
struct TimeBucketFunc {
    /// 函数签名: (Timestamp(Microsecond), Int64) → Timestamp(Microsecond)
    signature: Signature,
}

impl TimeBucketFunc {
    /// 创建 time_bucket 函数实例
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    DataType::Int64,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TimeBucketFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// 返回函数名称 "time_bucket"
    fn name(&self) -> &str {
        "time_bucket"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// 返回值类型: Timestamp(Microsecond, None)
    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> std::result::Result<DataType, DataFusionError> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
    }

    /// 执行 time_bucket 函数
    ///
    /// 对输入的时间戳数组逐元素执行分桶计算:
    /// - 有效值: `bucketed = (ts / bucket) * bucket` (向下取整)
    /// - 空值: 保留为 null
    fn invoke(
        &self,
        args: &[ColumnarValue],
    ) -> std::result::Result<ColumnarValue, DataFusionError> {
        if args.len() != 2 {
            return Err(DataFusionError::Execution(
                "time_bucket requires 2 arguments".into(),
            ));
        }

        let ts_arr = match &args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };

        let bucket_arr = match &args[1] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };

        let timestamps = ts_arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("first argument must be TimestampMicrosecond".into())
            })?;

        let bucket_size = bucket_arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Execution("second argument must be Int64".into()))?;

        let bucket = bucket_size.value(0);
        let mut builder = TimestampMicrosecondBuilder::new();

        for i in 0..timestamps.len() {
            if timestamps.is_valid(i) {
                let ts = timestamps.value(i);
                let bucketed = (ts / bucket) * bucket;
                builder.append_value(bucketed);
            } else {
                builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// 创建 time_bucket UDF 实例
///
/// 返回可用于注册到 DataFusion SessionContext 的 ScalarUDF。
pub fn time_bucket_udf() -> Arc<datafusion::logical_expr::ScalarUDF> {
    Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
        TimeBucketFunc::new(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Builder;

    fn invoke_time_bucket(
        func: &TimeBucketFunc,
        args: Vec<ColumnarValue>,
        num_rows: usize,
    ) -> std::result::Result<ColumnarValue, DataFusionError> {
        func.invoke_with_args(datafusion::logical_expr::ScalarFunctionArgs {
            args,
            number_rows: num_rows,
            return_type: &func.return_type(&[]).unwrap(),
        })
    }

    #[test]
    fn test_time_bucket_basic() {
        let func = TimeBucketFunc::new();

        let mut ts_builder = TimestampMicrosecondBuilder::new();
        ts_builder.append_value(1_000_000);
        ts_builder.append_value(1_500_000);
        ts_builder.append_value(2_000_000);
        let ts_arr = ts_builder.finish();

        let mut bucket_builder = Int64Builder::new();
        bucket_builder.append_value(1_000_000);
        let bucket_arr = bucket_builder.finish();

        let args = vec![
            ColumnarValue::Array(Arc::new(ts_arr.clone()) as Arc<dyn Array>),
            ColumnarValue::Array(Arc::new(bucket_arr) as Arc<dyn Array>),
        ];

        let result = invoke_time_bucket(&func, args, ts_arr.len()).unwrap();
        let result_arr = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("expected array"),
        };

        let bucketed = result_arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(bucketed.value(0), 1_000_000);
        assert_eq!(bucketed.value(1), 1_000_000);
        assert_eq!(bucketed.value(2), 2_000_000);
    }

    #[test]
    fn test_time_bucket_null() {
        let func = TimeBucketFunc::new();

        let mut ts_builder = TimestampMicrosecondBuilder::new();
        ts_builder.append_null();
        ts_builder.append_value(2_000_000);
        let ts_arr = ts_builder.finish();

        let mut bucket_builder = Int64Builder::new();
        bucket_builder.append_value(1_000_000);
        let bucket_arr = bucket_builder.finish();

        let args = vec![
            ColumnarValue::Array(Arc::new(ts_arr.clone()) as Arc<dyn Array>),
            ColumnarValue::Array(Arc::new(bucket_arr) as Arc<dyn Array>),
        ];

        let result = invoke_time_bucket(&func, args, ts_arr.len()).unwrap();
        let result_arr = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("expected array"),
        };

        let bucketed = result_arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert!(bucketed.is_null(0));
        assert_eq!(bucketed.value(1), 2_000_000);
    }
}
