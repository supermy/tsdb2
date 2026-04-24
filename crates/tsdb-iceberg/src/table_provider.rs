use arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::ExecutionPlan;
use futures::Stream;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use crate::catalog::IcebergCatalog;
use crate::manifest::DataFile;
use crate::scan::filters_to_predicate;
use crate::table::IcebergTable;

#[derive(Debug)]
pub struct IcebergTableProvider {
    catalog: Arc<IcebergCatalog>,
    table_name: String,
    schema: SchemaRef,
}

impl IcebergTableProvider {
    pub fn new(catalog: Arc<IcebergCatalog>, table_name: &str) -> crate::error::Result<Self> {
        let meta = catalog.load_metadata(table_name)?;
        let iceberg_schema = meta.current_schema();
        let arrow_schema = iceberg_schema.arrow_schema()?;
        Ok(Self {
            catalog,
            table_name: table_name.to_string(),
            schema: arrow_schema,
        })
    }

    pub fn from_table(
        table: &IcebergTable,
        catalog: Arc<IcebergCatalog>,
    ) -> crate::error::Result<Self> {
        let schema = table.schema().arrow_schema()?;
        Ok(Self {
            catalog,
            table_name: table.name().to_string(),
            schema,
        })
    }
}

fn iceberg_err_to_df(e: crate::error::IcebergError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let meta = self
            .catalog
            .load_metadata(&self.table_name)
            .map_err(iceberg_err_to_df)?;

        let predicate = filters_to_predicate(filters, meta.current_schema());

        let table = IcebergTable::new(self.catalog.clone(), self.table_name.clone(), meta);

        let scan = table.scan();
        let scan_builder = if let Some(pred) = predicate {
            scan.predicate(pred)
        } else {
            scan
        };
        let iceberg_scan = scan_builder.build().map_err(iceberg_err_to_df)?;

        let projected_schema = match projection {
            Some(proj) => {
                let fields: Vec<_> = proj
                    .iter()
                    .map(|&idx| self.schema.field(idx).clone())
                    .collect();
                Arc::new(ArrowSchema::new(fields))
            }
            None => self.schema.clone(),
        };

        let data_files = iceberg_scan.plan().to_vec();
        let eq_properties = EquivalenceProperties::new(projected_schema.clone());
        let properties = datafusion::physical_plan::PlanProperties::new(
            eq_properties,
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Arc::new(IcebergScanExec {
            data_files,
            schema: projected_schema,
            limit,
            properties,
        }))
    }
}

pub struct IcebergScanExec {
    data_files: Vec<DataFile>,
    schema: SchemaRef,
    limit: Option<usize>,
    properties: datafusion::physical_plan::PlanProperties,
}

impl fmt::Debug for IcebergScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergScanExec")
            .field("data_files", &self.data_files.len())
            .field("limit", &self.limit)
            .finish()
    }
}

impl datafusion::physical_plan::DisplayAs for IcebergScanExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        write!(
            f,
            "IcebergScanExec: files={}, limit={:?}",
            self.data_files.len(),
            self.limit
        )
    }
}

impl ExecutionPlan for IcebergScanExec {
    fn name(&self) -> &str {
        "IcebergScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> std::result::Result<datafusion::execution::SendableRecordBatchStream, DataFusionError>
    {
        let data_files = self.data_files.clone();
        let limit = self.limit;
        let schema = self.schema.clone();

        let stream = IcebergScanStream::new(data_files, schema, limit);
        Ok(Box::pin(stream))
    }
}

pub struct IcebergScanStream {
    data_files: Vec<DataFile>,
    schema: SchemaRef,
    limit: Option<usize>,
    current_file_idx: usize,
    current_batches: Vec<arrow::record_batch::RecordBatch>,
    current_batch_idx: usize,
    rows_returned: usize,
    done: bool,
}

impl IcebergScanStream {
    pub fn new(data_files: Vec<DataFile>, schema: SchemaRef, limit: Option<usize>) -> Self {
        Self {
            data_files,
            schema,
            limit,
            current_file_idx: 0,
            current_batches: Vec::new(),
            current_batch_idx: 0,
            rows_returned: 0,
            done: false,
        }
    }

    fn advance(
        &mut self,
    ) -> Option<std::result::Result<arrow::record_batch::RecordBatch, DataFusionError>> {
        if self.done {
            return None;
        }

        loop {
            if self.current_batch_idx < self.current_batches.len() {
                let batch = self.current_batches[self.current_batch_idx].clone();
                self.current_batch_idx += 1;

                if let Some(limit) = self.limit {
                    let remaining = limit.saturating_sub(self.rows_returned);
                    if remaining == 0 {
                        self.done = true;
                        return None;
                    }
                    if batch.num_rows() > remaining {
                        let sliced = batch.slice(0, remaining);
                        self.rows_returned += remaining;
                        self.done = true;
                        return Some(Ok(sliced));
                    }
                    self.rows_returned += batch.num_rows();
                }

                return Some(Ok(batch));
            }

            if self.current_file_idx >= self.data_files.len() {
                self.done = true;
                return None;
            }

            let df = &self.data_files[self.current_file_idx];
            self.current_file_idx += 1;

            let path = std::path::PathBuf::from(&df.file_path);
            if !path.exists() {
                return Some(Err(DataFusionError::Execution(format!(
                    "data file not found: {}",
                    df.file_path
                ))));
            }

            let file = match std::fs::File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    return Some(Err(DataFusionError::Execution(format!(
                        "failed to open data file {}: {}",
                        df.file_path, e
                    ))));
                }
            };

            let builder =
                match parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file) {
                    Ok(b) => b,
                    Err(e) => {
                        return Some(Err(DataFusionError::Execution(format!(
                            "failed to create parquet reader for {}: {}",
                            df.file_path, e
                        ))));
                    }
                };

            let reader = match builder.build() {
                Ok(r) => r,
                Err(e) => {
                    return Some(Err(DataFusionError::Execution(format!(
                        "failed to build parquet reader for {}: {}",
                        df.file_path, e
                    ))));
                }
            };

            match reader.collect::<std::result::Result<Vec<_>, _>>() {
                Ok(batches) => {
                    self.current_batches = batches;
                    self.current_batch_idx = 0;
                }
                Err(e) => {
                    return Some(Err(DataFusionError::Execution(format!(
                        "failed to read parquet batches from {}: {}",
                        df.file_path, e
                    ))));
                }
            }
        }
    }
}

impl Stream for IcebergScanStream {
    type Item = std::result::Result<arrow::record_batch::RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.advance())
    }
}

impl datafusion::physical_plan::RecordBatchStream for IcebergScanStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::IcebergCatalog;
    use crate::partition::PartitionSpec;
    use crate::schema::{Field, IcebergType, Schema};
    use crate::table::IcebergTable;
    use tempfile::TempDir;
    use tsdb_arrow::schema::{DataPoint, FieldValue};

    fn setup_table_with_data() -> (TempDir, Arc<IcebergCatalog>, String) {
        let dir = TempDir::new().unwrap();
        let catalog = Arc::new(IcebergCatalog::open(dir.path()).unwrap());

        let schema = Schema::new(
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
                    id: 1000,
                    name: "usage".to_string(),
                    required: false,
                    field_type: IcebergType::Double,
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
            ],
        );
        let spec = PartitionSpec::unpartitioned(0);
        catalog.create_table("cpu", schema, spec).unwrap();

        let meta = catalog.load_metadata("cpu").unwrap();
        let mut table = IcebergTable::new(catalog.clone(), "cpu".to_string(), meta);

        let dps: Vec<DataPoint> = (0..10)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.01))
            })
            .collect();
        table.append(&dps).unwrap();

        (dir, catalog, "cpu".to_string())
    }

    #[test]
    fn test_provider_creation() {
        let (_dir, catalog, name) = setup_table_with_data();
        let provider = IcebergTableProvider::new(catalog, &name).unwrap();
        assert_eq!(provider.schema().fields().len(), 3);
    }

    #[test]
    fn test_provider_schema() {
        let (_dir, catalog, name) = setup_table_with_data();
        let provider = IcebergTableProvider::new(catalog, &name).unwrap();
        let schema = provider.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"timestamp"));
        assert!(field_names.contains(&"tag_host"));
        assert!(field_names.contains(&"usage"));
    }
}
