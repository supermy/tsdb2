#[cfg(test)]
mod tests {
    use tsdb_arrow::schema::Tags;
    use tsdb_storage_arrow::config::ArrowStorageConfig;
    use tsdb_storage_arrow::engine::ArrowStorageEngine;
    use tsdb_test_utils::make_simple_datapoints;

    #[test]
    fn stress_read_range() {
        let dir = tempfile::tempdir().unwrap();
        let config = ArrowStorageConfig {
            wal_enabled: false,
            max_buffer_rows: 100_000,
            flush_interval_ms: 100,
            ..Default::default()
        };

        let engine = ArrowStorageEngine::open(dir.path(), config).unwrap();
        let dps = make_simple_datapoints(100_000);
        engine.write_batch(&dps).unwrap();
        engine.flush().unwrap();

        let tags = Tags::new();
        let start = std::time::Instant::now();
        let result = engine
            .read_range(
                "cpu",
                &tags,
                1_000_000_000,
                1_000_000_000 + 100_000 * 1_000_000,
            )
            .unwrap();
        let elapsed = start.elapsed();

        eprintln!(
            "read 100K range: {:.2}s ({} points, {:.0} points/sec)",
            elapsed.as_secs_f64(),
            result.len(),
            result.len() as f64 / elapsed.as_secs_f64()
        );

        assert!(!result.is_empty());
    }
}
