use crate::error::TsdbArrowError;
use crate::schema::{DataPoint, Tags};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

pub type EngineResult<T> = std::result::Result<T, TsdbArrowError>;

pub trait StorageEngine: Send + Sync {
    fn write(&self, dp: &DataPoint) -> EngineResult<()>;
    fn write_batch(&self, datapoints: &[DataPoint]) -> EngineResult<()>;
    fn read_range(
        &self,
        measurement: &str,
        tags: &Tags,
        start: i64,
        end: i64,
    ) -> EngineResult<Vec<DataPoint>>;
    fn get_point(
        &self,
        measurement: &str,
        tags: &Tags,
        timestamp: i64,
    ) -> EngineResult<Option<DataPoint>>;
    fn list_measurements(&self) -> Vec<String>;
    fn flush(&self) -> EngineResult<()>;

    fn read_range_arrow(
        &self,
        measurement: &str,
        start: i64,
        end: i64,
        projection: Option<&[String]>,
    ) -> EngineResult<Vec<RecordBatch>> {
        let _ = (measurement, start, end, projection);
        Err(TsdbArrowError::UnsupportedOperation(
            "read_range_arrow not implemented".into(),
        ))
    }

    fn measurement_schema(&self, measurement: &str) -> Option<SchemaRef> {
        let _ = measurement;
        None
    }
}
