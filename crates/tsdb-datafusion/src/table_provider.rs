use crate::error::{Result, TsdbDatafusionError};
use crate::predicate::extract_filters_with_schema;
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
use tsdb_arrow::schema::DataPoint;
use tsdb_parquet::partition::PartitionConfig;
use tsdb_parquet::partition::PartitionManager;
use tsdb_parquet::reader::TsdbParquetReader;

/// TSDB 表提供器
///
/// 实现 DataFusion 的 TableProvider trait, 将 TSDB 中的时序数据
/// 以 Parquet 文件为存储后端暴露为 DataFusion 可查询的表。
///
/// 支持以下优化:
/// - **列投影下推**: 只读取查询所需的列, 减少内存占用
/// - **谓词下推**: 将过滤条件下推到扫描层, 减少内存中需要处理的数据量
/// - **Limit 下推**: 在扫描层直接截断数据, 避免读取多余数据
/// - **Parquet 原生扫描**: 直接读取 Parquet 为 RecordBatch, 避免中间 DataPoint 转换
pub struct TsdbTableProvider {
    schema: SchemaRef,
    measurement: String,
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

    #[allow(dead_code)]
    fn load_data_parquet(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        let config = PartitionConfig::default();
        let pm = PartitionManager::new(&self.base_dir, config)?;
        let reader = TsdbParquetReader::new(Arc::new(pm));

        let proj_columns: Option<Vec<String>> = projection.map(|proj| {
            proj.iter()
                .filter_map(|&idx| self.schema.field(idx).name().clone().into())
                .collect()
        });

        let start = 0i64;
        let end = 4_102_444_800_000_000i64;

        let batches = match &proj_columns {
            Some(cols) => {
                reader.read_range_arrow(&self.measurement, start, end, Some(cols.as_slice()))?
            }
            None => reader.read_range_arrow(&self.measurement, start, end, None)?,
        };

        Ok(batches)
    }
}

#[async_trait]
impl TableProvider for TsdbTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let projected_schema = if let Some(proj) = projection {
            Arc::new(self.schema.project(proj)?)
        } else {
            self.schema.clone()
        };

        let extracted = extract_filters_with_schema(filters, Some(self.schema.clone()));
        let time_range = extracted.time_range;
        let tag_filters = extracted.tag_filters;

        let config = PartitionConfig::default();
        let pm = PartitionManager::new(&self.base_dir, config)
            .map_err(|e| DataFusionError::Execution(format!("partition manager: {}", e)))?;
        let reader = TsdbParquetReader::new(Arc::new(pm));

        let proj_columns: Option<Vec<String>> = projection.map(|proj| {
            proj.iter()
                .filter_map(|&idx| self.schema.field(idx).name().clone().into())
                .collect()
        });

        let start = time_range.map(|(s, _)| s).unwrap_or(0i64);
        let end = time_range.map(|(_, e)| e).unwrap_or(4_102_444_800_000_000i64);

        let tag_filters_ref = if tag_filters.is_empty() {
            None
        } else {
            Some(tag_filters)
        };

        let batches = reader
            .read_range_arrow_with_filters(
                &self.measurement,
                start,
                end,
                proj_columns.as_deref(),
                time_range,
                tag_filters_ref.as_ref(),
            )
            .map_err(|e| DataFusionError::Execution(format!("parquet scan: {}", e)))?;

        let projected_batches: Vec<arrow::record_batch::RecordBatch> = match projection {
            Some(proj) => {
                batches
                    .into_iter()
                    .map(|batch| {
                        let indices: Vec<usize> = proj
                            .iter()
                            .filter_map(|&idx| {
                                batch.schema().index_of(self.schema.field(idx).name()).ok()
                            })
                            .collect();
                        if indices.is_empty() {
                            Ok(batch)
                        } else {
                            batch.project(&indices)
                        }
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| DataFusionError::Execution(format!("projection: {}", e)))?
            }
            None => batches,
        };

        let limited_batches = if let Some(limit) = limit {
            let mut rows_so_far = 0usize;
            let mut result = Vec::new();
            for batch in projected_batches {
                if rows_so_far >= limit {
                    break;
                }
                let remaining = limit - rows_so_far;
                if batch.num_rows() <= remaining {
                    rows_so_far += batch.num_rows();
                    result.push(batch);
                } else {
                    result.push(batch.slice(0, remaining));
                    rows_so_far = limit;
                }
            }
            result
        } else {
            projected_batches
        };

        if limited_batches.is_empty() {
            let partitions = vec![vec![arrow::record_batch::RecordBatch::new_empty(
                projected_schema.clone(),
            )]];
            let mem_table = MemTable::try_new(projected_schema, partitions)?;
            return mem_table.scan(state, None, filters, limit).await;
        }

        let aligned_batches: Vec<arrow::record_batch::RecordBatch> = limited_batches
            .into_iter()
            .map(|batch| {
                if batch.schema() == projected_schema {
                    batch
                } else {
                    let mut columns = Vec::new();
                    for field in projected_schema.fields() {
                        if let Ok(idx) = batch.schema().index_of(field.name()) {
                            columns.push(batch.column(idx).clone());
                        } else {
                            columns.push(arrow::array::new_null_array(
                                field.data_type(),
                                batch.num_rows(),
                            ));
                        }
                    }
                    arrow::record_batch::RecordBatch::try_new(projected_schema.clone(), columns)
                        .unwrap_or(batch)
                }
            })
            .collect();

        let partitions = vec![aligned_batches];
        let mem_table = MemTable::try_new(projected_schema, partitions)?;

        mem_table.scan(state, None, filters, limit).await
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
