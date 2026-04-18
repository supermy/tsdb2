use crate::error::{Result, TsdbDatafusionError};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use tsdb_arrow::converter::datapoints_to_record_batch;
use tsdb_arrow::schema::DataPoint;
use tsdb_parquet::partition::PartitionConfig;
use tsdb_parquet::partition::PartitionManager;
use tsdb_parquet::reader::TsdbParquetReader;

/// TSDB 表提供器
///
/// 实现 DataFusion 的 TableProvider trait, 将 TSDB 中的时序数据
/// 以 Parquet 文件为存储后端暴露为 DataFusion 可查询的表。
///
/// 查询时从 Parquet 文件加载数据点, 转换为 Arrow RecordBatch,
/// 再通过 MemTable 交给 DataFusion 执行引擎处理。
pub struct TsdbTableProvider {
    /// 表的 Arrow Schema
    schema: SchemaRef,
    /// Measurement 名称
    measurement: String,
    /// 数据存储根目录
    base_dir: PathBuf,
}

impl fmt::Debug for TsdbTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TsdbTableProvider")
            .field("measurement", &self.measurement)
            .field("base_dir", &self.base_dir)
            .finish()
    }
}

impl TsdbTableProvider {
    /// 创建新的表提供器 (使用已有 Schema)
    pub fn new(
        measurement: impl Into<String>,
        schema: SchemaRef,
        base_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            measurement: measurement.into(),
            schema,
            base_dir: base_dir.into(),
        }
    }

    /// 从数据点列表创建表提供器 (自动推导 Schema)
    ///
    /// 从第一个数据点中提取 tag 键和 field 类型信息, 自动构建 Arrow Schema。
    /// 数据点列表为空时返回 Schema 错误。
    pub fn from_datapoints(
        measurement: impl Into<String>,
        datapoints: &[DataPoint],
        base_dir: impl Into<PathBuf>,
    ) -> Result<Self> {
        if datapoints.is_empty() {
            return Err(TsdbDatafusionError::Schema(
                "cannot create schema from empty datapoints".into(),
            ));
        }

        let dp = &datapoints[0];
        let tag_keys: Vec<String> = dp.tags.keys().cloned().collect();
        let field_types: Vec<(String, tsdb_arrow::schema::FieldValue)> = dp
            .fields
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let schema = crate::schema::measurement_to_schema(&dp.measurement, &tag_keys, &field_types);

        Ok(Self {
            measurement: measurement.into(),
            schema,
            base_dir: base_dir.into(),
        })
    }

    /// 从 Parquet 文件加载指定 measurement 的全部数据点
    fn load_data(&self) -> Result<Vec<DataPoint>> {
        let config = PartitionConfig::default();
        let pm = PartitionManager::new(&self.base_dir, config)?;
        let reader = TsdbParquetReader::new(pm);
        reader
            .read_all_datapoints(&self.measurement)
            .map_err(|e| TsdbDatafusionError::Query(e.to_string()))
    }
}

#[async_trait]
impl TableProvider for TsdbTableProvider {
    /// 返回 self 的 Any 引用, 用于类型向下转换
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// 返回表的 Schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// 返回表类型 (基础表)
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// 执行表扫描, 生成物理执行计划
    ///
    /// 从 Parquet 文件加载数据点, 转换为 RecordBatch,
    /// 再包装为 MemTable 供 DataFusion 执行。
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let datapoints = self
            .load_data()
            .map_err(|e| DataFusionError::Execution(format!("failed to load data: {}", e)))?;

        let limited_dps = if let Some(limit) = limit {
            &datapoints[..limit.min(datapoints.len())]
        } else {
            &datapoints
        };

        let batch = datapoints_to_record_batch(limited_dps, self.schema.clone())
            .map_err(|e| DataFusionError::Execution(format!("conversion error: {}", e)))?;

        let partitions = vec![vec![batch]];

        let mem_table = MemTable::try_new(self.schema.clone(), partitions)?;

        mem_table.scan(state, projection, filters, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tsdb_arrow::schema::{DataPoint, FieldValue};

    fn make_test_datapoints() -> Vec<DataPoint> {
        (0..50)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", format!("host_{:02}", i % 3))
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.01))
                    .with_field("count", FieldValue::Integer(i as i64 * 10))
            })
            .collect()
    }

    #[test]
    fn test_from_datapoints() {
        let dir = tempfile::tempdir().unwrap();
        let dps = make_test_datapoints();
        let provider = TsdbTableProvider::from_datapoints("cpu", &dps, dir.path());
        assert!(provider.is_ok());
        let p = provider.unwrap();
        assert_eq!(p.measurement, "cpu");
    }

    #[test]
    fn test_from_empty_datapoints_fails() {
        let dir = tempfile::tempdir().unwrap();
        let result = TsdbTableProvider::from_datapoints("cpu", &[], dir.path());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_scan_with_limit() {
        use datafusion::execution::context::SessionContext;

        let dir = tempfile::tempdir().unwrap();
        let dps = make_test_datapoints();
        let provider = TsdbTableProvider::from_datapoints("cpu", &dps, dir.path()).unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], Some(5)).await;
        assert!(plan.is_ok());
    }
}
