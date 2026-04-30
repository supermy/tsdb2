use crate::protocol::*;
use crate::utils::parse_partition_dir;
use std::path::PathBuf;
use std::sync::Arc;
use tsdb_arrow::engine::StorageEngine;

const MAX_SQL_RESULT_ROWS: usize = 5000;
const SQL_TIMEOUT_SECS: u64 = 30;
const MAX_ROCKSDB_LOAD_ROWS: usize = 100_000;
const MAX_SQL_LENGTH: usize = 10000;

pub struct SqlApi {
    engine: Arc<dyn StorageEngine>,
    parquet_dir: PathBuf,
    iceberg_catalog: Option<Arc<tsdb_iceberg::IcebergCatalog>>,
}

impl SqlApi {
    pub fn new(
        engine: Arc<dyn StorageEngine>,
        parquet_dir: PathBuf,
        iceberg_catalog: Option<Arc<tsdb_iceberg::IcebergCatalog>>,
    ) -> Self {
        Self {
            engine,
            parquet_dir,
            iceberg_catalog,
        }
    }

    fn validate_sql_readonly(sql: &str) -> Result<(), String> {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Err("SQL query must not be empty".to_string());
        }
        if trimmed.len() > MAX_SQL_LENGTH {
            return Err(format!(
                "SQL query too long: {} bytes (max {})",
                trimmed.len(),
                MAX_SQL_LENGTH
            ));
        }

        use datafusion::sql::sqlparser::ast::Statement;
        use datafusion::sql::sqlparser::dialect::GenericDialect;
        use datafusion::sql::sqlparser::parser::Parser;

        let dialect = GenericDialect;
        let statements =
            Parser::parse_sql(&dialect, sql).map_err(|_| "SQL parse error".to_string())?;

        for stmt in &statements {
            match stmt {
                Statement::Query(_)
                | Statement::Explain { .. }
                | Statement::ExplainTable { .. }
                | Statement::ShowTables { .. }
                | Statement::ShowColumns { .. }
                | Statement::ShowCreate { .. } => {},
                _ => {
                    return Err("Only SELECT/EXPLAIN/SHOW queries are allowed".to_string());
                },
            }
        }
        Ok(())
    }

    pub async fn execute(&self, sql: &str) -> Result<SqlResult, String> {
        Self::validate_sql_readonly(sql)?;

        let config =
            datafusion::execution::context::SessionConfig::new().with_information_schema(true);
        let ctx = datafusion::prelude::SessionContext::new_with_config(config);

        let rocksdb_tables = self.register_rocksdb_tables(&ctx)?;
        self.register_parquet_tables(&ctx, &rocksdb_tables)?;
        self.register_iceberg_tables(&ctx)?;

        let start = std::time::Instant::now();
        let df = ctx.sql(sql).await.map_err(|e| {
            tracing::error!("SQL parse error: {}", e);
            "SQL parse error".to_string()
        })?;

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(SQL_TIMEOUT_SECS),
            df.collect(),
        )
        .await;

        let batches = match result {
            Ok(Ok(batches)) => batches,
            Ok(Err(e)) => {
                tracing::error!("SQL execute error: {}", e);
                return Err("SQL execute error".to_string());
            },
            Err(_) => return Err(format!("SQL query timed out after {}s", SQL_TIMEOUT_SECS)),
        };
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

        let mut columns = Vec::new();
        let mut rows: Vec<serde_json::Value> = Vec::new();
        let mut total_rows: usize = 0;
        let mut truncated = false;

        for batch in &batches {
            if columns.is_empty() {
                columns = batch
                    .schema()
                    .fields()
                    .iter()
                    .filter(|f| f.name() != "tags_hash")
                    .map(|f| f.name().clone())
                    .collect();
            }
            for row_idx in 0..batch.num_rows() {
                total_rows += 1;
                if rows.len() < MAX_SQL_RESULT_ROWS {
                    let mut map = serde_json::Map::new();
                    for col_name in columns.iter() {
                        let real_idx = match batch.schema().index_of(col_name) {
                            Ok(idx) => idx,
                            Err(_) => {
                                map.insert(col_name.clone(), serde_json::Value::Null);
                                continue;
                            },
                        };
                        if real_idx >= batch.num_columns() {
                            map.insert(col_name.clone(), serde_json::Value::Null);
                            continue;
                        }
                        let val = Self::arrow_col_to_json(batch, real_idx, row_idx);
                        map.insert(col_name.clone(), val);
                    }
                    rows.push(serde_json::Value::Object(map));
                } else {
                    truncated = true;
                }
            }
        }

        Ok(SqlResult {
            sql: sql.to_string(),
            columns,
            rows,
            total_rows,
            elapsed_ms,
            truncated,
        })
    }

    fn register_rocksdb_tables(
        &self,
        ctx: &datafusion::prelude::SessionContext,
    ) -> Result<
        Vec<(
            String,
            arrow::datatypes::SchemaRef,
            Vec<arrow::record_batch::RecordBatch>,
        )>,
        String,
    > {
        let measurements = self.engine.list_measurements();
        let now = chrono::Utc::now().timestamp_micros();
        let start = now - 7 * 86_400_000_000i64;
        let mut registered = Vec::new();

        for m in &measurements {
            if let Some(schema) = self.engine.measurement_schema(m) {
                match self.engine.read_range_arrow(m, start, now, None) {
                    Ok(batches) => {
                        let mut limited_batches = Vec::new();
                        let mut row_count = 0usize;
                        for batch in batches {
                            row_count += batch.num_rows();
                            limited_batches.push(batch.clone());
                            if row_count >= MAX_ROCKSDB_LOAD_ROWS {
                                break;
                            }
                        }
                        if !limited_batches.is_empty() {
                            match datafusion::datasource::MemTable::try_new(
                                schema.clone(),
                                vec![limited_batches.clone()],
                            ) {
                                Ok(mem_table) => {
                                    if let Err(e) = ctx.register_table(m, Arc::new(mem_table)) {
                                        tracing::debug!("register table {}: {}", m, e);
                                    }
                                },
                                Err(e) => tracing::debug!("create memtable for {}: {}", m, e),
                            }
                        }
                        registered.push((m.clone(), schema, limited_batches));
                    },
                    Err(e) => tracing::debug!("read_range_arrow for {}: {}", m, e),
                }
            }
        }
        Ok(registered)
    }

    fn register_parquet_tables(
        &self,
        ctx: &datafusion::prelude::SessionContext,
        rocksdb_tables: &[(
            String,
            arrow::datatypes::SchemaRef,
            Vec<arrow::record_batch::RecordBatch>,
        )],
    ) -> Result<(), String> {
        let mut parquet_files_by_measurement: std::collections::BTreeMap<String, Vec<PathBuf>> =
            std::collections::BTreeMap::new();

        for tier in &["warm", "cold"] {
            let tier_dir = self.parquet_dir.join(tier);
            let mut all_parquet_files = Vec::new();
            Self::collect_parquet_files_recursive(&tier_dir, &mut all_parquet_files, 0);

            for pq_path in all_parquet_files {
                let measurement = Self::extract_measurement_from_parquet_path(&pq_path);
                if !measurement.is_empty() {
                    parquet_files_by_measurement
                        .entry(measurement)
                        .or_default()
                        .push(pq_path);
                }
            }
        }

        let rocksdb_map: std::collections::HashMap<
            String,
            (
                arrow::datatypes::SchemaRef,
                Vec<arrow::record_batch::RecordBatch>,
            ),
        > = rocksdb_tables
            .iter()
            .map(|(n, s, b)| (n.clone(), (s.clone(), b.clone())))
            .collect();

        for (measurement, files) in &parquet_files_by_measurement {
            let (existing_batches, existing_schema) = match rocksdb_map.get(measurement) {
                Some((schema, batches)) => (batches.clone(), Some(schema.clone())),
                None => (Vec::new(), None),
            };
            self.register_parquet_as_memtable(
                ctx,
                measurement,
                existing_batches,
                existing_schema,
                files,
            )?;
        }

        Ok(())
    }

    fn register_parquet_as_memtable(
        &self,
        ctx: &datafusion::prelude::SessionContext,
        measurement: &str,
        existing_batches: Vec<arrow::record_batch::RecordBatch>,
        existing_schema: Option<arrow::datatypes::SchemaRef>,
        files: &[PathBuf],
    ) -> Result<(), String> {
        let mut all_batches = existing_batches;
        let mut schema_ref = existing_schema;
        let mut row_count: usize = all_batches.iter().map(|b| b.num_rows()).sum();

        for pq_path in files {
            match std::fs::File::open(pq_path) {
                Ok(file) => {
                    match parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                        file,
                    ) {
                        Ok(builder) => {
                            if schema_ref.is_none() {
                                schema_ref = Some(builder.schema().clone());
                            }
                            match builder.build() {
                                Ok(reader) => {
                                    for batch_result in reader {
                                        match batch_result {
                                            Ok(batch) => {
                                                row_count += batch.num_rows();
                                                let aligned =
                                                    Self::align_schema(batch, &schema_ref);
                                                all_batches.push(aligned);
                                                if row_count >= MAX_ROCKSDB_LOAD_ROWS {
                                                    break;
                                                }
                                            },
                                            Err(e) => tracing::debug!("read parquet batch: {}", e),
                                        }
                                    }
                                },
                                Err(e) => tracing::debug!("build parquet reader: {}", e),
                            }
                        },
                        Err(e) => tracing::debug!("open parquet {}: {}", pq_path.display(), e),
                    }
                },
                Err(e) => tracing::debug!("open file {}: {}", pq_path.display(), e),
            }
            if row_count >= MAX_ROCKSDB_LOAD_ROWS {
                break;
            }
        }

        if let Some(schema) = schema_ref {
            if !all_batches.is_empty() {
                let mem_table =
                    datafusion::datasource::MemTable::try_new(schema, vec![all_batches])
                        .map_err(|e| format!("create memtable for {}: {}", measurement, e))?;
                if let Err(e) = ctx.register_table(measurement, Arc::new(mem_table)) {
                    tracing::debug!("register table {}: {}", measurement, e);
                }
            }
        }

        Ok(())
    }

    fn align_schema(
        batch: arrow::record_batch::RecordBatch,
        target_schema: &Option<arrow::datatypes::SchemaRef>,
    ) -> arrow::record_batch::RecordBatch {
        let Some(target) = target_schema else {
            return batch;
        };
        if batch.schema() == *target {
            return batch;
        }
        let mut columns = Vec::new();
        for field in target.fields() {
            if let Ok(idx) = batch.schema().index_of(field.name()) {
                columns.push(batch.column(idx).clone());
            } else {
                columns.push(arrow::array::new_null_array(
                    field.data_type(),
                    batch.num_rows(),
                ));
            }
        }
        arrow::record_batch::RecordBatch::try_new(target.clone(), columns).unwrap_or(batch)
    }

    fn collect_parquet_files_recursive(
        dir: &std::path::Path,
        files: &mut Vec<std::path::PathBuf>,
        depth: usize,
    ) {
        if depth > 5 || !dir.is_dir() {
            return;
        }
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    Self::collect_parquet_files_recursive(&path, files, depth + 1);
                } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    files.push(path);
                }
            }
        }
    }

    fn extract_measurement_from_parquet_path(path: &std::path::Path) -> String {
        for ancestor in path.ancestors() {
            let name = ancestor
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            if name.starts_with("ts_") {
                let (m, _) = crate::utils::parse_partition_dir(&name);
                return m;
            }
            if name.starts_with("data_") {
                if let Some(parent) = ancestor.parent() {
                    if let Some(parent_name) = parent.file_name() {
                        let parent_str = parent_name.to_string_lossy().to_string();
                        if !parent_str.starts_with("ts_")
                            && !parent_str.starts_with("data_")
                            && parent_str != "warm"
                            && parent_str != "cold"
                            && parent_str != "archive"
                        {
                            return parent_str;
                        }
                    }
                }
            }
        }
        String::new()
    }

    fn register_iceberg_tables(
        &self,
        ctx: &datafusion::prelude::SessionContext,
    ) -> Result<(), String> {
        if let Some(catalog) = &self.iceberg_catalog {
            let tables = catalog
                .list_tables()
                .map_err(|e| format!("list iceberg tables: {}", e))?;
            for table_name in &tables {
                match tsdb_iceberg::IcebergTableProvider::new(catalog.clone(), table_name) {
                    Ok(provider) => {
                        let registered_name = format!("iceberg_{}", table_name);
                        if let Err(e) = ctx.register_table(&registered_name, Arc::new(provider)) {
                            tracing::debug!("register iceberg table {}: {}", table_name, e);
                        }
                    },
                    Err(e) => tracing::debug!("create iceberg provider for {}: {}", table_name, e),
                }
            }
        }
        Ok(())
    }

    pub async fn tables(&self) -> Result<Vec<String>, String> {
        let mut tables: Vec<String> = self
            .engine
            .list_measurements()
            .into_iter()
            .filter(|m| !m.starts_with("sys_"))
            .collect();

        for tier in &["warm", "cold"] {
            let tier_dir = self.parquet_dir.join(tier);
            if let Ok(entries) = std::fs::read_dir(&tier_dir) {
                for entry in entries.flatten() {
                    let partition_dir = entry.path();
                    if !partition_dir.is_dir() {
                        continue;
                    }
                    let dir_name = partition_dir
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string();
                    let (measurement, _) = parse_partition_dir(&dir_name);
                    if !tables.contains(&measurement) {
                        tables.push(measurement);
                    }
                }
            }
        }

        if let Some(catalog) = &self.iceberg_catalog {
            if let Ok(iceberg_tables) = catalog.list_tables() {
                for t in iceberg_tables {
                    let name = format!("iceberg_{}", t);
                    if !tables.contains(&name) {
                        tables.push(name);
                    }
                }
            }
        }

        tables.sort();
        Ok(tables)
    }

    fn arrow_col_to_json(
        batch: &arrow::record_batch::RecordBatch,
        col_idx: usize,
        row_idx: usize,
    ) -> serde_json::Value {
        let col = batch.column(col_idx);
        if row_idx >= col.len() {
            return serde_json::Value::Null;
        }

        use arrow::array::*;
        use arrow::datatypes::DataType;

        match col.data_type() {
            DataType::Float64 => {
                if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                    if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        let v = arr.value(row_idx);
                        if v.is_nan() {
                            serde_json::Value::Null
                        } else {
                            serde_json::json!(v)
                        }
                    }
                } else {
                    serde_json::Value::Null
                }
            },
            DataType::Float32 => {
                if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
                    if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        let v = arr.value(row_idx);
                        if v.is_nan() {
                            serde_json::Value::Null
                        } else {
                            serde_json::json!(v)
                        }
                    }
                } else {
                    serde_json::Value::Null
                }
            },
            DataType::Int64 => {
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(arr.value(row_idx))
                    }
                } else {
                    serde_json::Value::Null
                }
            },
            DataType::Int32 => {
                if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                    if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(arr.value(row_idx))
                    }
                } else {
                    serde_json::Value::Null
                }
            },
            DataType::UInt64 => {
                if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                    if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(arr.value(row_idx))
                    }
                } else {
                    serde_json::Value::Null
                }
            },
            DataType::Utf8 => {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(arr.value(row_idx))
                    }
                } else {
                    serde_json::Value::Null
                }
            },
            DataType::Boolean => {
                if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                    if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(arr.value(row_idx))
                    }
                } else {
                    serde_json::Value::Null
                }
            },
            DataType::Timestamp(unit, _) => {
                let ts_val =
                    match unit {
                        arrow::datatypes::TimeUnit::Microsecond => {
                            if let Some(arr) =
                                col.as_any().downcast_ref::<arrow::array::PrimitiveArray<
                                    arrow::datatypes::TimestampMicrosecondType,
                                >>()
                            {
                                if arr.is_null(row_idx) {
                                    return serde_json::Value::Null;
                                }
                                arr.value(row_idx)
                            } else {
                                return serde_json::Value::Null;
                            }
                        },
                        arrow::datatypes::TimeUnit::Millisecond => {
                            if let Some(arr) =
                                col.as_any().downcast_ref::<arrow::array::PrimitiveArray<
                                    arrow::datatypes::TimestampMillisecondType,
                                >>()
                            {
                                if arr.is_null(row_idx) {
                                    return serde_json::Value::Null;
                                }
                                arr.value(row_idx) * 1000
                            } else {
                                return serde_json::Value::Null;
                            }
                        },
                        arrow::datatypes::TimeUnit::Second => {
                            if let Some(arr) =
                                col.as_any().downcast_ref::<arrow::array::PrimitiveArray<
                                    arrow::datatypes::TimestampSecondType,
                                >>()
                            {
                                if arr.is_null(row_idx) {
                                    return serde_json::Value::Null;
                                }
                                arr.value(row_idx) * 1_000_000
                            } else {
                                return serde_json::Value::Null;
                            }
                        },
                        arrow::datatypes::TimeUnit::Nanosecond => {
                            if let Some(arr) =
                                col.as_any().downcast_ref::<arrow::array::PrimitiveArray<
                                    arrow::datatypes::TimestampNanosecondType,
                                >>()
                            {
                                if arr.is_null(row_idx) {
                                    return serde_json::Value::Null;
                                }
                                arr.value(row_idx) / 1000
                            } else {
                                return serde_json::Value::Null;
                            }
                        },
                    };
                let secs = ts_val / 1_000_000;
                let dt =
                    chrono::DateTime::from_timestamp(secs, ((ts_val % 1_000_000) * 1000) as u32)
                        .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| ts_val.to_string());
                serde_json::json!(dt)
            },
            _ => serde_json::Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_sql_readonly_select() {
        assert!(SqlApi::validate_sql_readonly("SELECT * FROM cpu").is_ok());
    }

    #[test]
    fn test_validate_sql_readonly_explain() {
        assert!(SqlApi::validate_sql_readonly("EXPLAIN SELECT * FROM cpu").is_ok());
    }

    #[test]
    fn test_validate_sql_readonly_show() {
        assert!(SqlApi::validate_sql_readonly("SHOW TABLES").is_ok());
    }

    #[test]
    fn test_validate_sql_readonly_insert_rejected() {
        assert!(SqlApi::validate_sql_readonly("INSERT INTO cpu VALUES (1, 2, 3)").is_err());
    }

    #[test]
    fn test_validate_sql_readonly_delete_rejected() {
        assert!(SqlApi::validate_sql_readonly("DELETE FROM cpu WHERE host='a'").is_err());
    }

    #[test]
    fn test_validate_sql_readonly_drop_rejected() {
        assert!(SqlApi::validate_sql_readonly("DROP TABLE cpu").is_err());
    }

    #[test]
    fn test_validate_sql_readonly_update_rejected() {
        assert!(SqlApi::validate_sql_readonly("UPDATE cpu SET value=1").is_err());
    }

    #[test]
    fn test_validate_sql_readonly_create_rejected() {
        assert!(SqlApi::validate_sql_readonly("CREATE TABLE evil (id INT)").is_err());
    }

    #[test]
    fn test_validate_sql_length_limit() {
        let long_sql = format!("SELECT * FROM {}", "a".repeat(MAX_SQL_LENGTH));
        assert!(SqlApi::validate_sql_readonly(&long_sql).is_err());
    }

    #[test]
    fn test_validate_sql_empty_rejected() {
        assert!(SqlApi::validate_sql_readonly("").is_err());
        assert!(SqlApi::validate_sql_readonly("   ").is_err());
    }

    #[test]
    fn test_validate_sql_invalid_rejected() {
        assert!(SqlApi::validate_sql_readonly("NOT SQL AT ALL!!!").is_err());
    }
}
