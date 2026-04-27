use crate::error::{Result, TsdbParquetError};
use crate::partition::{micros_to_date, PartitionManager};
use crate::pruning::prune_row_groups;
use arrow::array::{BooleanArray, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter,
};
use parquet::arrow::ProjectionMask;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use tsdb_arrow::converter::record_batch_to_datapoints;
use tsdb_arrow::schema::{DataPoint, Tags};

/// Parquet 存储读取器
///
/// 按日期分区读取 Parquet 文件, 支持范围查询和标签过滤。
/// 读取流程: 时间范围 → 日期分区列表 → 遍历 Parquet 文件 → 解码 RecordBatch → 过滤标签
pub struct TsdbParquetReader {
    partition_manager: Arc<PartitionManager>,
}

impl TsdbParquetReader {
    /// 创建读取器
    pub fn new(partition_manager: Arc<PartitionManager>) -> Self {
        Self { partition_manager }
    }

    /// 范围读取并过滤标签
    ///
    /// 1. 按时间范围扫描相关分区的 Parquet 文件
    /// 2. 解码 RecordBatch 为 DataPoint
    /// 3. 过滤: 时间戳在范围内 + 标签完全匹配
    /// 4. 补充缺失的 measurement 名称
    /// 5. 按时间戳排序返回
    pub fn read_range(
        &self,
        measurement: &str,
        tags: &Tags,
        start_micros: i64,
        end_micros: i64,
    ) -> Result<Vec<DataPoint>> {
        let batches = self.read_range_arrow(measurement, start_micros, end_micros, None)?;
        let mut all_points = Vec::new();

        for batch in &batches {
            let mut points = record_batch_to_datapoints(batch)?;
            points.retain(|dp| {
                let ts_ok = dp.timestamp >= start_micros && dp.timestamp <= end_micros;
                let t_ok = tags.iter().all(|(k, v)| dp.tags.get(k) == Some(v));
                ts_ok && t_ok
            });
            for dp in &mut points {
                if dp.measurement.is_empty() {
                    dp.measurement = measurement.to_string();
                }
            }
            all_points.append(&mut points);
        }

        all_points.sort_by_key(|dp| dp.timestamp);
        Ok(all_points)
    }

    /// 范围读取 Arrow RecordBatch
    ///
    /// 根据时间范围确定日期分区, 遍历分区内的 Parquet 文件,
    /// 可选投影指定列。
    pub fn read_range_arrow(
        &self,
        _measurement: &str,
        start_micros: i64,
        end_micros: i64,
        projection: Option<&[String]>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_range_arrow_with_filters(
            _measurement,
            start_micros,
            end_micros,
            projection,
            Some((start_micros, end_micros)),
            None,
        )
    }

    pub fn read_range_arrow_with_filters(
        &self,
        _measurement: &str,
        start_micros: i64,
        end_micros: i64,
        projection: Option<&[String]>,
        time_range: Option<(i64, i64)>,
        tag_filters: Option<&HashMap<String, String>>,
    ) -> Result<Vec<RecordBatch>> {
        self.partition_manager.refresh()?;

        let start_date = micros_to_date(start_micros);
        let end_date = micros_to_date(end_micros);

        let partitions = self
            .partition_manager
            .get_partitions_in_range(start_date, end_date);

        let mut batches = Vec::new();

        for partition in partitions {
            let files = self.partition_manager.list_parquet_files(partition.date)?;

            for file_path in files {
                let file_batches = self.read_parquet_file_with_pruning(
                    &file_path,
                    time_range,
                    tag_filters,
                    projection,
                )?;
                batches.extend(file_batches);
            }
        }

        Ok(batches)
    }

    /// 读取单个数据点 (精确时间戳匹配)
    pub fn get_point(
        &self,
        measurement: &str,
        tags: &Tags,
        timestamp: i64,
    ) -> Result<Option<DataPoint>> {
        let points = self.read_range(measurement, tags, timestamp, timestamp)?;
        Ok(points.into_iter().next())
    }

    /// 读取单个 Parquet 文件, 可选列投影
    pub fn read_parquet_file(
        &self,
        path: &PathBuf,
        projection: Option<&[String]>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_parquet_file_with_pruning(path, None, None, projection)
    }

    pub fn read_parquet_file_with_pruning(
        &self,
        path: &PathBuf,
        time_range: Option<(i64, i64)>,
        tag_filters: Option<&HashMap<String, String>>,
        projection: Option<&[String]>,
    ) -> Result<Vec<RecordBatch>> {
        let metadata = std::fs::metadata(path).map_err(|e| {
            TsdbParquetError::Io(std::io::Error::other(format!(
                "parquet file not found: {}",
                e
            )))
        })?;

        if metadata.len() < 8 {
            tracing::warn!("skipping invalid parquet file (too small): {:?}", path);
            return Ok(Vec::new());
        }

        let file = File::open(path).map_err(|e| {
            TsdbParquetError::Io(std::io::Error::other(format!(
                "parquet file open failed: {}",
                e
            )))
        })?;

        let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("skipping corrupt parquet file {:?}: {}", path, e);
                return Ok(Vec::new());
            }
        };

        let parquet_metadata = builder.metadata();
        let arrow_schema = builder.schema().clone();

        let arrow_schema = if arrow_schema.metadata().get("measurement").is_none() {
            if let Some(kv) = parquet_metadata.file_metadata().key_value_metadata() {
                let mut meta = arrow_schema.metadata().clone();
                for item in kv {
                    if item.key == "ARROW:sCHEmA" || item.key == "arrow_schema" {
                        continue;
                    }
                    if let Some(ref val) = item.value {
                        meta.insert(item.key.clone(), val.clone());
                    }
                }
                Arc::new(arrow_schema.as_ref().clone().with_metadata(meta))
            } else {
                arrow_schema
            }
        } else {
            arrow_schema
        };

        let num_row_groups = parquet_metadata.num_row_groups();

        let row_groups = prune_row_groups(parquet_metadata, time_range);
        let has_pruned = row_groups.len() < num_row_groups;
        let pruned_count = row_groups.len();

        let row_filter = if let Some(filters) = tag_filters {
            if !filters.is_empty() {
                let parquet_schema = parquet_metadata.file_metadata().schema_descr();
                build_tag_row_filter(filters, parquet_schema, &arrow_schema)
            } else {
                None
            }
        } else {
            None
        };

        let projection_mask = if let Some(cols) = projection {
            let parquet_schema = parquet_metadata.file_metadata().schema_descr();
            Some(ProjectionMask::columns(parquet_schema, cols.iter().map(|s| s.as_str())))
        } else {
            None
        };

        let builder = builder.with_row_groups(row_groups);

        let builder = if let Some(filter) = row_filter {
            builder.with_row_filter(filter)
        } else {
            builder
        };

        let builder = if let Some(mask) = projection_mask {
            builder.with_projection(mask)
        } else {
            builder
        };

        let reader = builder.build()?;

        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result?;
            if batch.schema().metadata().get("measurement").is_none()
                && arrow_schema.metadata().get("measurement").is_some()
            {
                let enriched_schema = arrow_schema.clone();
                let batch = RecordBatch::try_new(enriched_schema, batch.columns().to_vec())
                    .unwrap_or(batch);
                batches.push(batch);
            } else {
                batches.push(batch);
            }
        }

        if has_pruned {
            tracing::debug!(
                "read_parquet_file {:?}: selected {}/{} row groups",
                path.file_name().unwrap_or_default(),
                pruned_count,
                num_row_groups
            );
        }

        Ok(batches)
    }

    /// 读取指定 measurement 的所有数据点
    pub fn read_all_datapoints(&self, measurement: &str) -> Result<Vec<DataPoint>> {
        self.partition_manager.refresh()?;

        let start = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or(chrono::NaiveDate::MIN);
        let end = chrono::Utc::now().date_naive() + chrono::Duration::days(1);
        let partitions = self.partition_manager.get_partitions_in_range(start, end);

        let mut all_points = Vec::new();

        for partition in partitions {
            let files = self.partition_manager.list_parquet_files(partition.date)?;

            for file_path in files {
                let file_batches = self.read_parquet_file(&file_path, None)?;
                for batch in file_batches {
                    let mut points = record_batch_to_datapoints(&batch)?;
                    for dp in &mut points {
                        if dp.measurement.is_empty() && !measurement.is_empty() {
                            dp.measurement = measurement.to_string();
                        }
                    }
                    if !measurement.is_empty() {
                        points.retain(|dp| dp.measurement == measurement);
                    }
                    all_points.append(&mut points);
                }
            }
        }

        all_points.sort_by_key(|dp| dp.timestamp);
        Ok(all_points)
    }
}

fn build_tag_row_filter(
    tag_filters: &HashMap<String, String>,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    arrow_schema: &arrow::datatypes::Schema,
) -> Option<RowFilter> {
    let mut filters: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> = Vec::new();

    for (tag_key, tag_value) in tag_filters {
        let col_name = format!("tag_{}", tag_key);
        let idx = match arrow_schema.index_of(&col_name) {
            Ok(idx) => idx,
            Err(_) => continue,
        };

        let mask = ProjectionMask::leaves(parquet_schema, [idx]);
        let tag_value_clone = tag_value.clone();
        let predicate = ArrowPredicateFn::new(mask, move |batch| {
            let col = batch.column(0);
            let string_col = match col.as_any().downcast_ref::<StringArray>() {
                Some(c) => c,
                None => return Ok(BooleanArray::from_iter((0..batch.num_rows()).map(|_| Some(true)))),
            };
            let filtered: BooleanArray = string_col
                .iter()
                .map(|opt| opt.map(|s| s == tag_value_clone.as_str()))
                .collect();
            Ok(filtered)
        });
        filters.push(Box::new(predicate) as Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>);
    }

    if filters.is_empty() {
        None
    } else {
        Some(RowFilter::new(filters))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::PartitionConfig;
    use crate::writer::{TsdbParquetWriter, WriteBufferConfig};
    use tempfile::TempDir;
    use tsdb_arrow::schema::FieldValue;

    fn write_test_data(dir: &TempDir) -> PartitionManager {
        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default());

        let dps: Vec<DataPoint> = (0..50)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_tag("region", "us-west")
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.01))
                    .with_field("count", FieldValue::Integer(i as i64 * 10))
            })
            .collect();

        writer.write_batch(&dps).unwrap();
        writer.flush_all().unwrap();

        PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap()
    }

    #[test]
    fn test_read_range() {
        let dir = TempDir::new().unwrap();
        let pm = write_test_data(&dir);
        let reader = TsdbParquetReader::new(Arc::new(pm));

        let tags = Tags::new();
        let points = reader
            .read_range("cpu", &tags, 1_000_000_000, 1_000_050_000_000)
            .unwrap();

        assert!(!points.is_empty());
        for p in &points {
            assert_eq!(p.measurement, "cpu");
        }
    }

    #[test]
    fn test_read_with_tag_filter() {
        let dir = TempDir::new().unwrap();
        let pm = write_test_data(&dir);
        let reader = TsdbParquetReader::new(Arc::new(pm));

        let mut tags = Tags::new();
        tags.insert("host".to_string(), "server01".to_string());

        let points = reader
            .read_range("cpu", &tags, 1_000_000_000, 1_000_050_000_000)
            .unwrap();

        assert!(!points.is_empty());
        for p in &points {
            assert_eq!(p.tags.get("host").unwrap(), "server01");
        }
    }

    #[test]
    fn test_read_parquet_file() {
        let dir = TempDir::new().unwrap();
        let pm = write_test_data(&dir);
        let reader = TsdbParquetReader::new(Arc::new(pm));

        let date = micros_to_date(1_000_000_000);
        let files = reader.partition_manager.list_parquet_files(date).unwrap();
        assert!(!files.is_empty());

        let batches = reader.read_parquet_file(&files[0], None).unwrap();
        assert!(!batches.is_empty());
        assert!(batches[0].num_rows() > 0);
    }
}
