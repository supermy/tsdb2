#[cfg(test)]
mod tests {
    use tsdb_storage_arrow::config::ArrowStorageConfig;
    use tsdb_storage_arrow::engine::ArrowStorageEngine;
    use tsdb_test_utils::make_simple_datapoints;

    #[test]
    fn stress_write_throughput() {
        let dir = tempfile::tempdir().unwrap();
        let config = ArrowStorageConfig {
            wal_enabled: false,
            max_buffer_rows: 100_000,
            flush_interval_ms: 100,
            ..Default::default()
        };

        let engine = ArrowStorageEngine::open(dir.path(), config).unwrap();
        let dps = make_simple_datapoints(1_000_000);

        let start = std::time::Instant::now();
        engine.write_batch(&dps).unwrap();
        engine.flush().unwrap();
        let elapsed = start.elapsed();

        eprintln!(
            "write 1M points: {:.2}s ({:.0} points/sec)",
            elapsed.as_secs_f64(),
            1_000_000.0 / elapsed.as_secs_f64()
        );

        assert!(elapsed.as_secs() < 120, "write took too long");
    }
}
