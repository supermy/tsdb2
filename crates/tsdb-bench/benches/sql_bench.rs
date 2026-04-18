use criterion::{criterion_group, criterion_main, Criterion};
use tsdb_datafusion::engine::DataFusionQueryEngine;
use tsdb_test_utils::make_simple_datapoints;

fn bench_sql_query(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let engine = DataFusionQueryEngine::new(dir.path());
    let dps = make_simple_datapoints(1000);

    engine.register_from_datapoints("cpu", &dps).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("sql_select_all_1k", |b| {
        b.iter(|| {
            rt.block_on(async {
                engine.execute("SELECT * FROM cpu").await.unwrap();
            });
        });
    });

    c.bench_function("sql_avg_aggregation_1k", |b| {
        b.iter(|| {
            rt.block_on(async {
                engine
                    .execute("SELECT AVG(usage) as avg_usage FROM cpu")
                    .await
                    .unwrap();
            });
        });
    });
}

criterion_group!(benches, bench_sql_query);
criterion_main!(benches);
