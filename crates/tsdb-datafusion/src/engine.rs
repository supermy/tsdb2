use crate::error::{Result, TsdbDatafusionError};
use crate::table_provider::TsdbTableProvider;
use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use std::path::PathBuf;
use std::sync::Arc;
use tsdb_arrow::schema::{DataPoint, FieldValue};

/// 从 Arrow 数组中提取指定行的字段值
///
/// 根据 DataType 将 Arrow 列数组中指定索引位置的值提取为 FieldValue 枚举。
/// 支持的数据类型: Float64, Int64, Utf8, Boolean, Timestamp(Microsecond)。
fn extract_field_value_from_array(
    col: &Arc<dyn Array>,
    index: usize,
    dtype: &DataType,
) -> Result<FieldValue> {
    match dtype {
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| FieldValue::Float(a.value(index)))
            .ok_or_else(|| TsdbDatafusionError::Query("failed to downcast Float64".into())),
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| FieldValue::Integer(a.value(index)))
            .ok_or_else(|| TsdbDatafusionError::Query("failed to downcast Int64".into())),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| FieldValue::String(a.value(index).to_string()))
            .ok_or_else(|| TsdbDatafusionError::Query("failed to downcast Utf8".into())),
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| FieldValue::Boolean(a.value(index)))
            .ok_or_else(|| TsdbDatafusionError::Query("failed to downcast Boolean".into())),
        DataType::Timestamp(_, _) => col
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(|a| FieldValue::Integer(a.value(index)))
            .ok_or_else(|| TsdbDatafusionError::Query("failed to downcast Timestamp".into())),
        _ => Err(TsdbDatafusionError::Query(format!(
            "unsupported data type: {:?}",
            dtype
        ))),
    }
}

/// DataFusion 查询引擎
///
/// 封装 DataFusion SessionContext, 提供 TSDB 数据源的 SQL 查询能力。
/// 通过注册 TsdbTableProvider 将时序数据暴露为 DataFusion 可查询的表,
/// 然后执行 SQL 语句并返回结果。
pub struct DataFusionQueryEngine {
    /// DataFusion 会话上下文
    ctx: SessionContext,
    /// 数据存储根目录
    base_dir: PathBuf,
}

impl DataFusionQueryEngine {
    /// 创建新的查询引擎实例
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            ctx: SessionContext::new(),
            base_dir: base_dir.into(),
        }
    }

    /// 注册 Measurement 表 (使用已有 Schema)
    pub fn register_measurement(&self, name: &str, schema: SchemaRef) -> Result<()> {
        let provider = TsdbTableProvider::new(name, schema, &self.base_dir);
        self.ctx
            .register_table(name, Arc::new(provider))
            .map_err(TsdbDatafusionError::DataFusion)?;
        Ok(())
    }

    /// 从数据点列表注册 Measurement 表 (自动推导 Schema)
    pub fn register_from_datapoints(&self, name: &str, datapoints: &[DataPoint]) -> Result<()> {
        let provider = TsdbTableProvider::from_datapoints(name, datapoints, &self.base_dir)?;
        self.ctx
            .register_table(name, Arc::new(provider))
            .map_err(TsdbDatafusionError::DataFusion)?;
        Ok(())
    }

    /// 执行 SQL 查询并返回结构化结果
    ///
    /// 空值 (null) 会被转换为 FieldValue::Float(NaN)。
    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(TsdbDatafusionError::DataFusion)?;

        let batches = df
            .collect()
            .await
            .map_err(TsdbDatafusionError::DataFusion)?;

        let mut columns = Vec::new();
        let mut rows = Vec::new();

        if !batches.is_empty() {
            let schema = batches[0].schema();
            columns = schema.fields().iter().map(|f| f.name().clone()).collect();

            for batch in &batches {
                for row_idx in 0..batch.num_rows() {
                    let mut row = Vec::new();
                    for (col_idx, field) in schema.fields().iter().enumerate() {
                        let col = batch.column(col_idx);
                        if col.is_null(row_idx) {
                            row.push(tsdb_arrow::schema::FieldValue::Float(f64::NAN));
                        } else {
                            let fv =
                                extract_field_value_from_array(col, row_idx, field.data_type())?;
                            row.push(fv);
                        }
                    }
                    rows.push(row);
                }
            }
        }

        Ok(QueryResult { columns, rows })
    }

    /// 执行 SQL 查询并返回原始 Arrow RecordBatch
    pub async fn execute_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(TsdbDatafusionError::DataFusion)?;

        df.collect().await.map_err(TsdbDatafusionError::DataFusion)
    }

    /// 获取底层 DataFusion SessionContext 引用
    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }
}

/// SQL 查询结果
///
/// 将 DataFusion 的 RecordBatch 查询结果转换为简单的二维表格结构。
#[derive(Debug)]
pub struct QueryResult {
    /// 列名列表
    pub columns: Vec<String>,
    /// 行数据, rows[i][j] 表示第 i 行第 j 列的值
    pub rows: Vec<Vec<tsdb_arrow::schema::FieldValue>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tsdb_arrow::schema::{DataPoint, FieldValue};

    fn make_test_datapoints() -> Vec<DataPoint> {
        (0..100)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", format!("host_{:02}", i % 5))
                    .with_tag("region", if i % 2 == 0 { "us-west" } else { "us-east" })
                    .with_field("usage", FieldValue::Float(0.3 + i as f64 * 0.005))
                    .with_field("idle", FieldValue::Float(0.7 - i as f64 * 0.005))
                    .with_field("count", FieldValue::Integer(i as i64 * 10))
            })
            .collect()
    }

    fn write_datapoints_to_dir(dir: &std::path::Path, dps: &[DataPoint]) {
        use tsdb_parquet::partition::PartitionConfig;
        use tsdb_parquet::partition::PartitionManager;
        use tsdb_parquet::writer::{TsdbParquetWriter, WriteBufferConfig};

        let pm = PartitionManager::new(dir, PartitionConfig::default()).unwrap();
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default());
        writer.write_batch(dps).unwrap();
        writer.flush_all().unwrap();
    }

    #[tokio::test]
    async fn test_register_from_datapoints() {
        let dir = tempfile::tempdir().unwrap();
        let engine = DataFusionQueryEngine::new(dir.path());
        let dps = make_test_datapoints();
        let result = engine.register_from_datapoints("cpu", &dps);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_select_all() {
        let dir = tempfile::tempdir().unwrap();
        let dps = make_test_datapoints();
        write_datapoints_to_dir(dir.path(), &dps);

        let engine = DataFusionQueryEngine::new(dir.path());
        engine.register_from_datapoints("cpu", &dps).unwrap();

        let result = engine.execute("SELECT * FROM cpu").await;
        assert!(result.is_ok());
        let qr = result.unwrap();
        assert!(!qr.columns.is_empty());
    }

    #[tokio::test]
    async fn test_execute_aggregation() {
        let dir = tempfile::tempdir().unwrap();
        let dps = make_test_datapoints();
        write_datapoints_to_dir(dir.path(), &dps);

        let engine = DataFusionQueryEngine::new(dir.path());
        engine.register_from_datapoints("cpu", &dps).unwrap();

        let result = engine
            .execute("SELECT AVG(usage) as avg_usage FROM cpu")
            .await;
        assert!(result.is_ok());
        let qr = result.unwrap();
        assert!(qr.columns.contains(&"avg_usage".to_string()));
    }

    #[tokio::test]
    async fn test_execute_filter() {
        let dir = tempfile::tempdir().unwrap();
        let dps = make_test_datapoints();
        write_datapoints_to_dir(dir.path(), &dps);

        let engine = DataFusionQueryEngine::new(dir.path());
        engine.register_from_datapoints("cpu", &dps).unwrap();

        let result = engine.execute("SELECT * FROM cpu").await.unwrap();
        assert!(!result.rows.is_empty());
    }

    #[tokio::test]
    async fn test_execute_arrow() {
        let dir = tempfile::tempdir().unwrap();
        let dps = make_test_datapoints();
        write_datapoints_to_dir(dir.path(), &dps);

        let engine = DataFusionQueryEngine::new(dir.path());
        engine.register_from_datapoints("cpu", &dps).unwrap();

        let result = engine.execute_arrow("SELECT * FROM cpu").await;
        assert!(result.is_ok());
        let batches = result.unwrap();
        assert!(!batches.is_empty());
    }

    #[tokio::test]
    async fn test_register_empty_datapoints_fails() {
        let dir = tempfile::tempdir().unwrap();
        let engine = DataFusionQueryEngine::new(dir.path());
        let result = engine.register_from_datapoints("cpu", &[]);
        assert!(result.is_err());
    }
}
