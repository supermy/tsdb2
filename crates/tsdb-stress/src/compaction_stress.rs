#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tsdb_arrow::schema::{DataPoint, FieldValue};
    use tsdb_parquet::compaction::{CompactionConfig, ParquetCompactor};
    use tsdb_parquet::partition::PartitionConfig;
    use tsdb_parquet::reader::TsdbParquetReader;
    use tsdb_parquet::writer::{TsdbParquetWriter, WriteBufferConfig};
    use tsdb_test_utils::micros_to_date;

    /// Compaction 完整性压力测试
    ///
    /// 写入数据 → Compaction → 验证数据点数量不变
    #[test]
    fn stress_compaction_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let pm =
            tsdb_parquet::partition::PartitionManager::new(dir.path(), PartitionConfig::default())
                .unwrap();

        let mut writer = TsdbParquetWriter::new(
            Arc::new(pm),
            WriteBufferConfig {
                max_buffer_rows: 100,
                ..Default::default()
            },
        );

        let total_points = 5000;
        let dps: Vec<DataPoint> = (0..total_points)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000_000 + i as i64 * 1_000)
                    .with_tag("host", "server01")
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.0001))
            })
            .collect();

        for dp in &dps {
            writer.write(dp).unwrap();
        }
        writer.flush_all().unwrap();

        let date = micros_to_date(1_000_000_000);
        let pm2 =
            tsdb_parquet::partition::PartitionManager::new(dir.path(), PartitionConfig::default())
                .unwrap();
        let reader = TsdbParquetReader::new(Arc::new(pm2));
        let before = reader.read_all_datapoints("cpu").unwrap();
        let before_count = before.len();

        let pm3 =
            tsdb_parquet::partition::PartitionManager::new(dir.path(), PartitionConfig::default())
                .unwrap();
        let compactor = ParquetCompactor::new(Arc::new(pm3), CompactionConfig::default());

        compactor.compact_date(date).unwrap();

        let pm4 =
            tsdb_parquet::partition::PartitionManager::new(dir.path(), PartitionConfig::default())
                .unwrap();
        let reader2 = TsdbParquetReader::new(Arc::new(pm4));
        let after = reader2.read_all_datapoints("cpu").unwrap();
        assert_eq!(after.len(), before_count);
    }
}
