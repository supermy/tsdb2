use std::sync::Arc;
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tsdb_arrow::schema::{DataPoint, FieldValue, Tags};
    use tsdb_parquet::partition::{PartitionConfig, PartitionManager};
    use tsdb_parquet::reader::TsdbParquetReader;
    use tsdb_parquet::writer::{TsdbParquetWriter, WriteBufferConfig};

    #[test]
    fn stress_concurrent_write() {
        let dir = tempfile::tempdir().unwrap();

        let threads = 4;
        let points_per_thread = 5_000;

        let mut all_dps: Vec<Vec<DataPoint>> = Vec::new();
        for t in 0..threads {
            let dps: Vec<DataPoint> = (0..points_per_thread)
                .map(|i| {
                    DataPoint::new(format!("metric_{}", t), 1_000_000 + i as i64 * 1_000)
                        .with_tag("thread", format!("t{}", t))
                        .with_field("value", FieldValue::Float(i as f64 * 0.1))
                })
                .collect();
            all_dps.push(dps);
        }

        let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default());

        for dps in &all_dps {
            writer.write_batch(dps).unwrap();
        }
        writer.flush_all().unwrap();

        let pm2 = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
        let reader = TsdbParquetReader::new(Arc::new(pm2));

        let total: usize = (0..threads)
            .map(|t| {
                let tags = Tags::new();
                reader
                    .read_range(&format!("metric_{}", t), &tags, 1_000_000, 100_000_000)
                    .unwrap()
                    .len()
            })
            .sum();

        assert!(
            total > 0,
            "expected some data to be readable, got {}",
            total
        );
    }
}
