#[allow(unused_imports)]
use std::sync::Arc;
#[allow(unused_imports)]
use tempfile::TempDir;
#[allow(unused_imports)]
use tsdb_arrow::schema::{DataPoint, FieldValue};
#[allow(unused_imports)]
use tsdb_iceberg::{
    IcebergCatalog, IcebergTable, IcebergTableProvider, PartitionSpec, Predicate, Schema,
};

#[cfg(test)]
fn make_catalog(dir: &TempDir) -> Arc<IcebergCatalog> {
    Arc::new(IcebergCatalog::open(dir.path()).unwrap())
}

#[cfg(test)]
fn make_cpu_schema() -> Schema {
    Schema::new(
        0,
        vec![
            tsdb_iceberg::schema::Field {
                id: 1,
                name: "timestamp".to_string(),
                required: true,
                field_type: tsdb_iceberg::schema::IcebergType::Timestamptz,
                doc: None,
                initial_default: None,
                write_default: None,
            },
            tsdb_iceberg::schema::Field {
                id: 2,
                name: "tag_host".to_string(),
                required: false,
                field_type: tsdb_iceberg::schema::IcebergType::String,
                doc: None,
                initial_default: None,
                write_default: None,
            },
            tsdb_iceberg::schema::Field {
                id: 3,
                name: "tag_region".to_string(),
                required: false,
                field_type: tsdb_iceberg::schema::IcebergType::String,
                doc: None,
                initial_default: None,
                write_default: None,
            },
            tsdb_iceberg::schema::Field {
                id: 1000,
                name: "usage".to_string(),
                required: false,
                field_type: tsdb_iceberg::schema::IcebergType::Double,
                doc: None,
                initial_default: None,
                write_default: None,
            },
            tsdb_iceberg::schema::Field {
                id: 1001,
                name: "count".to_string(),
                required: false,
                field_type: tsdb_iceberg::schema::IcebergType::Long,
                doc: None,
                initial_default: None,
                write_default: None,
            },
        ],
    )
}

#[cfg(test)]
fn make_iot_datapoints(n: usize, host_prefix: &str) -> Vec<DataPoint> {
    (0..n)
        .map(|i| {
            DataPoint::new("cpu", 1_000_000 + i as i64 * 60_000_000)
                .with_tag("host", format!("{}-{}", host_prefix, i % 3))
                .with_tag("region", if i % 2 == 0 { "us-west" } else { "us-east" })
                .with_field("usage", FieldValue::Float(10.0 + i as f64 * 0.5))
                .with_field("count", FieldValue::Integer(100 + i as i64))
        })
        .collect()
}

#[test]
fn test_iceberg_full_lifecycle() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("metrics", schema, spec).unwrap();

    let meta = catalog.load_table("metrics").unwrap();
    assert_eq!(meta.format_version, 2);
    assert!(meta.current_snapshot().is_none());

    let tables = catalog.list_tables().unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0], "metrics");

    catalog.rename_table("metrics", "metrics_v2").unwrap();
    assert!(catalog.load_table("metrics").is_err());
    assert!(catalog.load_table("metrics_v2").is_ok());

    catalog.drop_table("metrics_v2").unwrap();
    assert!(catalog.load_table("metrics_v2").is_err());
    assert!(catalog.list_tables().unwrap().is_empty());
}

#[test]
fn test_iceberg_append_and_scan() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("cpu", schema, spec).unwrap();

    let meta = catalog.load_table("cpu").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "cpu".to_string(), meta);

    let dps = make_iot_datapoints(20, "server");
    table.append(&dps).unwrap();

    let scan = table.scan().build().unwrap();
    let batches = scan.execute().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 20);

    let data_files = scan.plan();
    assert!(!data_files.is_empty());

    for df in data_files {
        assert_eq!(df.content, tsdb_iceberg::manifest::DataContentType::Data);
        assert_eq!(df.file_format, "parquet");
        assert!(df.record_count > 0);
        assert!(std::path::Path::new(&df.file_path).exists());
    }
}

#[test]
fn test_iceberg_multiple_appends() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("cpu", schema, spec).unwrap();

    let meta = catalog.load_table("cpu").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "cpu".to_string(), meta);

    for batch in 0..5 {
        let dps = make_iot_datapoints(10, &format!("batch{}", batch));
        table.append(&dps).unwrap();
    }

    assert_eq!(table.snapshots().len(), 5);

    let current_snap = table.current_snapshot().unwrap();
    assert_eq!(current_snap.summary.operation, "append");

    let scan = table.scan().build().unwrap();
    let batches = scan.execute().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 50);

    let data_files = scan.plan();
    assert!(data_files.len() >= 5);
}

#[test]
fn test_iceberg_scan_with_predicate() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("cpu", schema, spec).unwrap();

    let meta = catalog.load_table("cpu").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "cpu".to_string(), meta);

    let dps = make_iot_datapoints(30, "server");
    table.append(&dps).unwrap();

    let pred = Predicate::Gt(
        1,
        tsdb_iceberg::scan::PredicateValue::Long(15 * 60_000_000 + 1_000_000),
    );
    let scan = table.scan().predicate(pred).build().unwrap();
    let batches = scan.execute().unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(rows > 0 && rows <= 30);
}

#[test]
fn test_iceberg_scan_empty_result() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("cpu", schema, spec).unwrap();

    let meta = catalog.load_table("cpu").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "cpu".to_string(), meta);

    let dps = make_iot_datapoints(10, "server");
    table.append(&dps).unwrap();

    let pred = Predicate::Gt(1, tsdb_iceberg::scan::PredicateValue::Long(999_999_999_999));
    let scan = table.scan().predicate(pred).build().unwrap();
    assert!(scan.plan().is_empty());
    let batches = scan.execute().unwrap();
    assert!(batches.is_empty());
}

#[test]
fn test_iceberg_time_travel() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("cpu", schema, spec).unwrap();

    let meta = catalog.load_table("cpu").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "cpu".to_string(), meta);

    let dps1 = make_iot_datapoints(5, "batch1");
    table.append(&dps1).unwrap();
    let snap1_id = table.current_snapshot().unwrap().snapshot_id;

    let dps2 = make_iot_datapoints(7, "batch2");
    table.append(&dps2).unwrap();
    let snap2_id = table.current_snapshot().unwrap().snapshot_id;

    assert!(snap1_id != snap2_id);

    let scan1 = table.scan_at_snapshot(snap1_id).unwrap().build().unwrap();
    let rows1: usize = scan1.execute().unwrap().iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows1, 5);

    let scan2 = table.scan_at_snapshot(snap2_id).unwrap().build().unwrap();
    let rows2: usize = scan2.execute().unwrap().iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows2, 12);
}

#[test]
fn test_iceberg_compaction() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("cpu", schema, spec).unwrap();

    let meta = catalog.load_table("cpu").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "cpu".to_string(), meta);

    for _ in 0..4 {
        let dps = make_iot_datapoints(5, "compact");
        table.append(&dps).unwrap();
    }

    let before_files = table
        .collect_data_files(table.current_snapshot().unwrap())
        .unwrap()
        .len();
    assert!(before_files >= 4);

    table.compact().unwrap();

    let after_files = table
        .collect_data_files(table.current_snapshot().unwrap())
        .unwrap()
        .len();
    assert!(after_files < before_files);

    let snap = table.current_snapshot().unwrap();
    assert_eq!(snap.summary.operation, "replace");
}

#[test]
fn test_iceberg_rollback() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("cpu", schema, spec).unwrap();

    let meta = catalog.load_table("cpu").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "cpu".to_string(), meta);

    let dps1 = make_iot_datapoints(8, "rollback_test");
    table.append(&dps1).unwrap();
    let snap1_id = table.current_snapshot().unwrap().snapshot_id;

    let dps2 = make_iot_datapoints(12, "rollback_extra");
    table.append(&dps2).unwrap();
    let before_rows = table
        .scan()
        .build()
        .unwrap()
        .execute()
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();

    assert_eq!(before_rows, 20);

    table.rollback_to_snapshot(snap1_id).unwrap();

    let after_rows = table
        .scan()
        .build()
        .unwrap()
        .execute()
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    assert_eq!(after_rows, 8);
}

#[test]
fn test_iceberg_schema_evolution() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = Schema::new(
        0,
        vec![
            tsdb_iceberg::schema::Field {
                id: 1,
                name: "timestamp".to_string(),
                required: true,
                field_type: tsdb_iceberg::schema::IcebergType::Timestamptz,
                doc: None,
                initial_default: None,
                write_default: None,
            },
            tsdb_iceberg::schema::Field {
                id: 1000,
                name: "value".to_string(),
                required: false,
                field_type: tsdb_iceberg::schema::IcebergType::Double,
                doc: None,
                initial_default: None,
                write_default: None,
            },
        ],
    );
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("evolve", schema, spec).unwrap();

    let meta = catalog.load_table("evolve").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "evolve".to_string(), meta);

    assert_eq!(table.schema().fields.len(), 2);
    assert!(table.schema().field_by_name("idle").is_none());

    table
        .update_schema(vec![tsdb_iceberg::schema::SchemaChange::AddField {
            parent_id: None,
            name: "idle".to_string(),
            field_type: tsdb_iceberg::schema::IcebergType::Double,
            required: false,
            write_default: None,
        }])
        .unwrap();

    assert_eq!(table.schema().fields.len(), 3);
    assert!(table.schema().field_by_name("idle").is_some());
    assert_eq!(table.schema().schema_id, 1);
}

#[test]
fn test_iceberg_partition_evolution() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("part_evolve", schema, spec).unwrap();

    let meta = catalog.load_table("part_evolve").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "part_evolve".to_string(), meta);

    assert_eq!(table.partition_spec().spec_id, 0);
    assert!(table.partition_spec().fields.is_empty());

    let day_spec =
        tsdb_iceberg::PartitionSpec::day_partition(table.partition_spec().spec_id + 1, 1);
    table.update_partition_spec(day_spec).unwrap();

    assert_eq!(table.partition_spec().spec_id, 1);
    assert_eq!(table.partition_spec().fields.len(), 1);
}

#[test]
fn test_iceberg_expire_snapshots() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("expire", schema, spec).unwrap();

    let meta = catalog.load_table("expire").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "expire".to_string(), meta);

    for _ in 0..6 {
        let dps = make_iot_datapoints(3, "expire");
        table.append(&dps).unwrap();
    }

    assert_eq!(table.snapshots().len(), 6);

    let future_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 100_000;

    table.expire_snapshots(future_ms, 2).unwrap();
    assert!(table.snapshots().len() >= 2);
}

#[test]
fn test_iceberg_branch_tag() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("refs", schema, spec).unwrap();

    let meta = catalog.load_table("refs").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "refs".to_string(), meta);

    let dps = make_iot_datapoints(5, "branch_test");
    table.append(&dps).unwrap();
    let snap_id = table.current_snapshot().unwrap().snapshot_id;

    table.create_branch("dev", snap_id).unwrap();
    table.create_tag("v1.0", snap_id).unwrap();

    let branch_ref = catalog.load_ref("refs", "dev").unwrap();
    assert_eq!(branch_ref.snapshot_id, snap_id);
    assert_eq!(
        format!("{:?}", branch_ref.ref_type),
        format!("{:?}", tsdb_iceberg::snapshot::RefType::Branch)
    );

    let tag_ref = catalog.load_ref("refs", "v1.0").unwrap();
    assert_eq!(tag_ref.snapshot_id, snap_id);
    assert_eq!(
        format!("{:?}", tag_ref.ref_type),
        format!("{:?}", tsdb_iceberg::snapshot::RefType::Tag)
    );
}

#[test]
fn test_iceberg_snapshot_diff() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("diff", schema, spec).unwrap();

    let meta = catalog.load_table("diff").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "diff".to_string(), meta);

    let dps1 = make_iot_datapoints(5, "diff_a");
    table.append(&dps1).unwrap();
    let snap1 = table.current_snapshot().unwrap().snapshot_id;

    let dps2 = make_iot_datapoints(10, "diff_b");
    table.append(&dps2).unwrap();
    let snap2 = table.current_snapshot().unwrap().snapshot_id;

    let diff = table.snapshot_diff(snap1, snap2).unwrap();
    assert!(diff.added_records > 0);
    assert_eq!(diff.deleted_files, 0);
}

#[test]
fn test_iceberg_persistence_across_reopen() {
    let dir = TempDir::new().unwrap();
    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);

    {
        let catalog = make_catalog(&dir);
        catalog.create_table("persist", schema, spec).unwrap();
        let meta = catalog.load_table("persist").unwrap();
        let mut table = IcebergTable::new(catalog, "persist".to_string(), meta);
        let dps = make_iot_datapoints(15, "persist_test");
        table.append(&dps).unwrap();
    }

    let catalog = make_catalog(&dir);
    let meta = catalog.load_table("persist").unwrap();
    let table = IcebergTable::new(catalog, "persist".to_string(), meta);

    assert!(table.current_snapshot().is_some());
    assert_eq!(table.snapshots().len(), 1);

    let scan = table.scan().build().unwrap();
    let rows: usize = scan.execute().unwrap().iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 15);
}

#[test]
fn test_iceberg_day_partition_append() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = tsdb_iceberg::PartitionSpec::day_partition(0, 1);
    catalog.create_table("part_cpu", schema, spec).unwrap();

    let meta = catalog.load_table("part_cpu").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "part_cpu".to_string(), meta);

    let dps = make_iot_datapoints(25, "part_server");
    table.append(&dps).unwrap();

    let scan = table.scan().build().unwrap();
    let batches = scan.execute().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 25);

    let data_files = scan.plan();
    for df in data_files {
        assert!(!df.partition.is_empty());
    }
}

#[test]
fn test_iceberg_provider_creation() {
    use datafusion::catalog::TableProvider;

    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("provider_test", schema, spec).unwrap();

    let provider = IcebergTableProvider::new(catalog, "provider_test").unwrap();
    let arrow_schema = provider.schema();

    assert_eq!(arrow_schema.fields().len(), 5);
    let names: Vec<&str> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert!(names.contains(&"timestamp"));
    assert!(names.contains(&"tag_host"));
    assert!(names.contains(&"tag_region"));
    assert!(names.contains(&"usage"));
    assert!(names.contains(&"count"));
}

#[test]
fn test_iceberg_large_batch_write() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("large", schema, spec).unwrap();

    let meta = catalog.load_table("large").unwrap();
    let mut table = IcebergTable::new(catalog, "large".to_string(), meta);

    let dps = make_iot_datapoints(5000, "stress");
    table.append(&dps).unwrap();

    let scan = table.scan().build().unwrap();
    let rows: usize = scan.execute().unwrap().iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 5000);
}

#[test]
fn test_iceberg_concurrent_table_isolation() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog
        .create_table("t1", schema.clone(), spec.clone())
        .unwrap();
    catalog.create_table("t2", schema, spec).unwrap();

    let m1 = catalog.load_table("t1").unwrap();
    let m2 = catalog.load_table("t2").unwrap();

    let mut t1 = IcebergTable::new(catalog.clone(), "t1".to_string(), m1);
    let mut t2 = IcebergTable::new(catalog.clone(), "t2".to_string(), m2);

    let dps1 = make_iot_datapoints(10, "concurrent_a");
    let dps2 = make_iot_datapoints(10, "concurrent_b");

    t1.append(&dps1).unwrap();
    t2.append(&dps2).unwrap();

    let r1: usize = t1
        .scan()
        .build()
        .unwrap()
        .execute()
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    let r2: usize = t2
        .scan()
        .build()
        .unwrap()
        .execute()
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();

    assert_eq!(r1, 10);
    assert_eq!(r2, 10);
}

#[test]
fn test_iceberg_concurrent_append_conflict() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("conflict", schema, spec).unwrap();

    let meta = catalog.load_table("conflict").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "conflict".to_string(), meta);

    let dps1 = make_iot_datapoints(5, "first");
    table.append(&dps1).unwrap();
    let snap1 = table.current_snapshot().unwrap().snapshot_id;

    let dps2 = make_iot_datapoints(5, "second");
    table.append(&dps2).unwrap();

    let result = table.rollback_to_snapshot(snap1);
    assert!(result.is_ok());

    let dps3 = make_iot_datapoints(5, "third");
    table.append(&dps3).unwrap();

    let rows: usize = table
        .scan()
        .build()
        .unwrap()
        .execute()
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 10);
}

#[test]
fn test_iceberg_schema_evolution_backward_compatibility() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema_v1 = Schema::new(
        0,
        vec![
            tsdb_iceberg::schema::Field {
                id: 1,
                name: "timestamp".to_string(),
                required: true,
                field_type: tsdb_iceberg::schema::IcebergType::Timestamptz,
                doc: None,
                initial_default: None,
                write_default: None,
            },
            tsdb_iceberg::schema::Field {
                id: 1000,
                name: "value".to_string(),
                required: false,
                field_type: tsdb_iceberg::schema::IcebergType::Double,
                doc: None,
                initial_default: None,
                write_default: None,
            },
        ],
    );
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("compat", schema_v1, spec).unwrap();

    let meta = catalog.load_table("compat").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "compat".to_string(), meta);

    let dps_v1: Vec<DataPoint> = (0..5)
        .map(|i| {
            DataPoint::new("compat", 1_000_000 + i as i64 * 60_000_000)
                .with_tag("host", "s1")
                .with_field("value", FieldValue::Float(i as f64 * 1.5))
        })
        .collect();
    table.append(&dps_v1).unwrap();
    let snap_v1 = table.current_snapshot().unwrap().snapshot_id;

    table
        .update_schema(vec![tsdb_iceberg::schema::SchemaChange::AddField {
            parent_id: None,
            name: "extra".to_string(),
            field_type: tsdb_iceberg::schema::IcebergType::Long,
            required: false,
            write_default: Some(serde_json::json!(0)),
        }])
        .unwrap();

    let dps_v2: Vec<DataPoint> = (5..10)
        .map(|i| {
            DataPoint::new("compat", 1_000_000 + i as i64 * 60_000_000)
                .with_tag("host", "s2")
                .with_field("value", FieldValue::Float(i as f64 * 1.5))
                .with_field("extra", FieldValue::Integer(i as i64 * 10))
        })
        .collect();
    table.append(&dps_v2).unwrap();

    let scan = table.scan().build().unwrap();
    let batches = scan.execute().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10);

    let scan_v1 = table.scan_at_snapshot(snap_v1).unwrap().build().unwrap();
    let v1_rows: usize = scan_v1
        .execute()
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(v1_rows, 5);
}

#[test]
fn test_iceberg_wal_crash_recovery_integrity() {
    let dir = TempDir::new().unwrap();
    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);

    let snap_id = {
        let catalog = make_catalog(&dir);
        catalog.create_table("crash", schema, spec).unwrap();
        let meta = catalog.load_table("crash").unwrap();
        let mut table = IcebergTable::new(catalog, "crash".to_string(), meta);
        let dps = make_iot_datapoints(20, "crash_test");
        table.append(&dps).unwrap();
        table.current_snapshot().unwrap().snapshot_id
    };

    let catalog2 = make_catalog(&dir);
    let meta2 = catalog2.load_table("crash").unwrap();
    let table2 = IcebergTable::new(catalog2, "crash".to_string(), meta2);

    assert!(table2.current_snapshot().is_some());
    assert_eq!(table2.current_snapshot().unwrap().snapshot_id, snap_id);

    let scan = table2.scan().build().unwrap();
    let rows: usize = scan.execute().unwrap().iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 20);
}

#[test]
fn test_iceberg_partition_evolution_data_continuity() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("part_cont", schema, spec).unwrap();

    let meta = catalog.load_table("part_cont").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "part_cont".to_string(), meta);

    let dps1 = make_iot_datapoints(10, "unpart");
    table.append(&dps1).unwrap();
    let snap_unpart = table.current_snapshot().unwrap().snapshot_id;

    let day_spec =
        tsdb_iceberg::PartitionSpec::day_partition(table.partition_spec().spec_id + 1, 1);
    table.update_partition_spec(day_spec).unwrap();

    let dps2 = make_iot_datapoints(10, "parted");
    table.append(&dps2).unwrap();

    let scan_all = table.scan().build().unwrap();
    let all_rows: usize = scan_all
        .execute()
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(all_rows, 20);

    let scan_old = table
        .scan_at_snapshot(snap_unpart)
        .unwrap()
        .build()
        .unwrap();
    let old_rows: usize = scan_old
        .execute()
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(old_rows, 10);
}

#[test]
fn test_iceberg_error_table_not_found() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);
    let result = catalog.load_table("nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_iceberg_error_duplicate_table() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog
        .create_table("dup", schema.clone(), spec.clone())
        .unwrap();
    let result = catalog.create_table("dup", schema, spec);
    assert!(result.is_err());
}

#[test]
fn test_iceberg_error_rollback_to_invalid_snapshot() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("rberr", schema, spec).unwrap();

    let meta = catalog.load_table("rberr").unwrap();
    let mut table = IcebergTable::new(catalog.clone(), "rberr".to_string(), meta);

    let dps = make_iot_datapoints(5, "rb");
    table.append(&dps).unwrap();

    let result = table.rollback_to_snapshot(-999);
    assert!(result.is_err());
}

#[test]
fn test_iceberg_error_scan_nonexistent_snapshot() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("scanerr", schema, spec).unwrap();

    let meta = catalog.load_table("scanerr").unwrap();
    let table = IcebergTable::new(catalog.clone(), "scanerr".to_string(), meta);

    let result = table.scan_at_snapshot(-999);
    assert!(result.is_err());
}

#[test]
fn test_iceberg_error_drop_nonexistent_table() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);
    let result = catalog.drop_table("no_such_table");
    assert!(result.is_err());
}

#[test]
fn test_iceberg_empty_table_scan() {
    let dir = TempDir::new().unwrap();
    let catalog = make_catalog(&dir);

    let schema = make_cpu_schema();
    let spec = PartitionSpec::unpartitioned(0);
    catalog.create_table("empty_scan", schema, spec).unwrap();

    let meta = catalog.load_table("empty_scan").unwrap();
    let table = IcebergTable::new(catalog, "empty_scan".to_string(), meta);

    assert!(table.current_snapshot().is_none());

    let scan = table.scan().build().unwrap();
    let batches = scan.execute().unwrap();
    assert!(batches.is_empty());
}
