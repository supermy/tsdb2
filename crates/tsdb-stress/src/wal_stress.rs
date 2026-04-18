#[cfg(test)]
mod tests {
    use tsdb_arrow::schema::{DataPoint, FieldValue};
    use tsdb_parquet::wal::{TsdbWAL, WALEntryType};

    #[test]
    fn stress_wal_crash_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal").join("stress.wal");
        let wal = TsdbWAL::create(&wal_path).unwrap();

        let total_points = 100_000;
        let dps: Vec<DataPoint> = (0..total_points)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000)
                    .with_tag("host", "server01")
                    .with_field("usage", FieldValue::Float(0.5))
            })
            .collect();

        for dp in &dps {
            let payload = serde_json::to_vec(dp).unwrap();
            wal.append(WALEntryType::Insert, &payload).unwrap();
        }
        wal.sync().unwrap();

        let entries = TsdbWAL::recover(&wal_path).unwrap();
        assert_eq!(entries.len(), total_points);

        let mut recovered_count = 0;
        for entry in &entries {
            if entry.entry_type == WALEntryType::Insert {
                let dp: DataPoint = serde_json::from_slice(&entry.payload).unwrap();
                assert_eq!(dp.measurement, "cpu");
                recovered_count += 1;
            }
        }
        assert_eq!(recovered_count, total_points);
    }
}
