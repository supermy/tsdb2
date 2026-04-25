use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use tsdb_parquet::partition::{PartitionConfig, PartitionManager};
use tsdb_parquet::writer::{TsdbParquetWriter, WriteBufferConfig};
use tsdb_test_utils::make_simple_datapoints;

fn bench_write_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_batch");

    for size in [100, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::new("batch", size), &size, |b, &size| {
            let dps = make_simple_datapoints(size);
            b.iter(|| {
                let dir = tempfile::tempdir().unwrap();
                let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
                let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default());
                writer.write_batch(black_box(&dps)).unwrap();
                writer.flush_all().unwrap();
            });
        });
    }
    group.finish();
}

fn bench_write_single(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
    let mut writer = TsdbParquetWriter::new(Arc::new(pm), WriteBufferConfig::default());
    let dps = make_simple_datapoints(1000);

    c.bench_function("write_single_1000", |b| {
        b.iter(|| {
            for dp in &dps {
                writer.write(black_box(dp)).unwrap();
            }
        });
    });
}

criterion_group!(benches, bench_write_batch, bench_write_single);
criterion_main!(benches);
