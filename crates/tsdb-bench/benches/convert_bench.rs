use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tsdb_arrow::converter::{datapoints_to_record_batch, record_batch_to_datapoints};
use tsdb_arrow::schema::TsdbSchemaBuilder;
use tsdb_test_utils::make_simple_datapoints;

fn bench_convert_to_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("datapoints_to_record_batch");

    for size in [100, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::new("size", size), &size, |b, &size| {
            let dps = make_simple_datapoints(size);
            let schema = TsdbSchemaBuilder::new("cpu")
                .compact()
                .with_tag_key("host")
                .with_tag_key("region")
                .with_float_field("usage")
                .with_float_field("idle")
                .with_int_field("count")
                .build();
            b.iter(|| {
                datapoints_to_record_batch(black_box(&dps), black_box(schema.clone())).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_convert_from_batch(c: &mut Criterion) {
    let dps = make_simple_datapoints(1000);
    let schema = TsdbSchemaBuilder::new("cpu")
        .compact()
        .with_tag_key("host")
        .with_tag_key("region")
        .with_float_field("usage")
        .with_float_field("idle")
        .with_int_field("count")
        .build();
    let batch = datapoints_to_record_batch(&dps, schema).unwrap();

    c.bench_function("record_batch_to_datapoints_1k", |b| {
        b.iter(|| {
            record_batch_to_datapoints(black_box(&batch)).unwrap();
        });
    });
}

criterion_group!(benches, bench_convert_to_batch, bench_convert_from_batch);
criterion_main!(benches);
