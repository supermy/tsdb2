use crate::protocol::*;
use crate::utils::parse_partition_dir;
use std::path::PathBuf;
use std::sync::Arc;
use tsdb_arrow::engine::StorageEngine;

const MAX_SQL_RESULT_ROWS: usize = 5000;
const SQL_TIMEOUT_SECS: u64 = 30;
const MAX_ROCKSDB_LOAD_ROWS: usize = 100_000;

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

    pub async fn execute(&self, sql: &str) -> Result<SqlResult, String> {
        let upper = sql.trim().to_uppercase();
        if !upper.starts_with("SELECT")
            && !upper.starts_with("SHOW")
            && !upper.starts_with("EXPLAIN")
            && !upper.starts_with("WITH")
        {
            return Err("only SELECT/SHOW/EXPLAIN/WITH queries are allowed".to_string());
        }

        let config =
            datafusion::execution::context::SessionConfig::new().with_information_schema(true);
        let ctx = datafusion::prelude::SessionContext::new_with_config(config);

        let rocksdb_tables = self.register_rocksdb_tables(&ctx)?;
        self.register_parquet_tables(&ctx, &rocksdb_tables)?;
        self.register_iceberg_tables(&ctx)?;

        let start = std::time::Instant::now();
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| format!("SQL parse error: {}", e))?;

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(SQL_TIMEOUT_SECS),
            df.collect(),
        )
        .await;

        let batches = match result {
            Ok(Ok(batches)) => batches,
            Ok(Err(e)) => return Err(format!("SQL execute error: {}", e)),
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
                    .map(|f| f.name().clone())
                    .collect();
            }
            for row_idx in 0..batch.num_rows() {
                total_rows += 1;
                if rows.len() < MAX_SQL_RESULT_ROWS {
                    let mut map = serde_json::Map::new();
                    for (col_idx, col_name) in columns.iter().enumerate() {
                        let val = Self::arrow_col_to_json(batch, col_idx, row_idx);
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
                                }
                                Err(e) => tracing::debug!("create memtable for {}: {}", m, e),
                            }
                        }
                        registered.push((m.clone(), schema, limited_batches));
                    }
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
                    if let Ok(sub_entries) = std::fs::read_dir(&partition_dir) {
                        for sub_entry in sub_entries.flatten() {
                            let p = sub_entry.path();
                            if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                                parquet_files_by_measurement
                                    .entry(measurement.clone())
                                    .or_default()
                                    .push(p);
                            }
                        }
                    }
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
                                                all_batches.push(batch);
                                                if row_count >= MAX_ROCKSDB_LOAD_ROWS {
                                                    break;
                                                }
                                            }
                                            Err(e) => tracing::debug!("read parquet batch: {}", e),
                                        }
                                    }
                                }
                                Err(e) => tracing::debug!("build parquet reader: {}", e),
                            }
                        }
                        Err(e) => tracing::debug!("open parquet {}: {}", pq_path.display(), e),
                    }
                }
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
                    }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
                        }
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
                        }
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
                        }
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
                        }
                    };
                let secs = ts_val / 1_000_000;
                let dt =
                    chrono::DateTime::from_timestamp(secs, ((ts_val % 1_000_000) * 1000) as u32)
                        .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| ts_val.to_string());
                serde_json::json!(dt)
            }
            _ => serde_json::Value::Null,
        }
    }
}
