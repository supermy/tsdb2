use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tsdb_arrow::schema::Tags;
use tsdb_parquet::partition::{PartitionConfig, PartitionManager};
use tsdb_parquet::reader::TsdbParquetReader;
use tsdb_parquet::writer::{TsdbParquetWriter, WriteBufferConfig};
use tsdb_test_utils::make_simple_datapoints;

fn bench_read_range(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let pm = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
    let mut writer = TsdbParquetWriter::new(pm, WriteBufferConfig::default());
    let dps = make_simple_datapoints(10_000);
    writer.write_batch(&dps).unwrap();
    writer.flush_all().unwrap();

    let pm2 = PartitionManager::new(dir.path(), PartitionConfig::default()).unwrap();
    let reader = TsdbParquetReader::new(pm2);

    c.bench_function("read_range_10k", |b| {
        b.iter(|| {
            let tags = Tags::new();
            reader
                .read_range(black_box("cpu"), &tags, 1_000_000_000, 1_010_000_000)
                .unwrap();
        });
    });
}

criterion_group!(benches, bench_read_range);
criterion_main!(benches);
