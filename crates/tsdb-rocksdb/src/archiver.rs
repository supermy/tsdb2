use crate::db::TsdbRocksDb;
use crate::error::Result;
use std::path::Path;

pub struct DataArchiver;

impl DataArchiver {
    /// 将 RocksDB 中指定 CF 的数据导出为 JSON Lines 文件
    ///
    /// 每行一个 DataPoint 的 JSON 序列化, 可被 Parquet 引擎导入。
    /// 导出完成后可选择删除源 CF。
    pub fn export_cf_to_json(db: &TsdbRocksDb, cf_name: &str, output_path: &Path) -> Result<usize> {
        let data_cf = db
            .db()
            .cf_handle(cf_name)
            .ok_or_else(|| crate::error::TsdbRocksDbError::CfNotFound(cf_name.to_string()))?;

        let meta_cf = db.db().cf_handle("_series_meta");
        let iter = db.db().iterator_cf(&data_cf, rocksdb::IteratorMode::Start);

        let mut file = std::fs::File::create(output_path)?;
        let mut count = 0usize;

        for item in iter {
            let (raw_key, raw_value) = item?;
            let ts_key = crate::key::TsdbKey::decode(&raw_key)?;

            let fields = crate::value::decode_fields(&raw_value)?;

            let tags = if let Some(ref meta_cf) = meta_cf {
                db.db()
                    .get_pinned_cf(meta_cf, ts_key.tags_hash.to_be_bytes())?
                    .and_then(|v| crate::tags::decode_tags(&v).ok())
                    .unwrap_or_default()
            } else {
                tsdb_arrow::schema::Tags::new()
            };

            let measurement = cf_name
                .strip_prefix("ts_")
                .and_then(|s| {
                    let date_part = s.rsplit_once('_').map(|(_, d)| d)?;
                    if date_part.len() == 8 && date_part.chars().all(|c| c.is_ascii_digit()) {
                        s.rsplit_once('_').map(|(m, _)| m)
                    } else {
                        None
                    }
                })
                .unwrap_or("unknown");

            let dp = tsdb_arrow::schema::DataPoint {
                measurement: measurement.to_string(),
                tags,
                fields,
                timestamp: ts_key.timestamp,
            };

            let json = serde_json::to_string(&dp)?;
            use std::io::Write;
            writeln!(file, "{}", json)?;
            count += 1;
        }

        Ok(count)
    }

    /// 将 RocksDB 中超过指定天数的所有 CF 数据归档到 JSON 文件,
    /// 然后删除源 CF。
    ///
    /// 返回 (归档的 CF 数量, 总数据点数)
    pub fn archive_expired_cfs(
        db: &TsdbRocksDb,
        archive_dir: &Path,
        retention_days: u64,
    ) -> Result<(usize, usize)> {
        std::fs::create_dir_all(archive_dir)?;

        let now = chrono::Utc::now().date_naive();
        let cfs = db.list_ts_cfs();

        let mut archived_count = 0usize;
        let mut total_points = 0usize;

        for cf_name in &cfs {
            let date_str = cf_name.rsplit_once('_').map(|(_, d)| d).unwrap_or("");

            let cf_date = chrono::NaiveDate::parse_from_str(date_str, "%Y%m%d");
            if let Ok(cf_date) = cf_date {
                let age_days = (now - cf_date).num_days() as u64;
                if age_days >= retention_days {
                    let output_path = archive_dir.join(format!("{}.jsonl", cf_name));
                    let marker_path = archive_dir.join(format!("{}.archived", cf_name));

                    if marker_path.exists() {
                        if output_path.exists() {
                            db.drop_cf(cf_name)?;
                            let _ = std::fs::remove_file(&marker_path);
                            archived_count += 1;
                            tracing::info!(
                                "completed previously interrupted archive for CF {}",
                                cf_name
                            );
                        }
                        continue;
                    }

                    let count = Self::export_cf_to_json(db, cf_name, &output_path)?;
                    std::fs::write(&marker_path, format!("{}", count))?;
                    total_points += count;
                    db.drop_cf(cf_name)?;
                    let _ = std::fs::remove_file(&marker_path);
                    archived_count += 1;
                }
            }
        }

        Ok((archived_count, total_points))
    }

    /// 列出归档目录中的文件
    pub fn list_archives(archive_dir: &Path) -> Result<Vec<String>> {
        if !archive_dir.exists() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        for entry in std::fs::read_dir(archive_dir)? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                if name.ends_with(".jsonl") || name.ends_with(".parquet") {
                    files.push(name.to_string());
                }
            }
        }
        files.sort();
        Ok(files)
    }

    pub fn export_cf_to_parquet(
        db: &TsdbRocksDb,
        cf_name: &str,
        output_dir: &Path,
        batch_size: usize,
    ) -> Result<usize> {
        let data_cf = db
            .db()
            .cf_handle(cf_name)
            .ok_or_else(|| crate::error::TsdbRocksDbError::CfNotFound(cf_name.to_string()))?;

        let meta_cf = db.db().cf_handle("_series_meta");
        let iter = db.db().iterator_cf(&data_cf, rocksdb::IteratorMode::Start);

        let measurement = cf_name
            .strip_prefix("ts_")
            .and_then(|s| {
                let date_part = s.rsplit_once('_').map(|(_, d)| d)?;
                if date_part.len() == 8 && date_part.chars().all(|c| c.is_ascii_digit()) {
                    s.rsplit_once('_').map(|(m, _)| m)
                } else {
                    None
                }
            })
            .unwrap_or("unknown");

        let mut buffer: Vec<tsdb_arrow::schema::DataPoint> = Vec::with_capacity(batch_size);
        let mut total = 0usize;
        let mut file_idx = 0usize;
        let mut schema: Option<arrow::datatypes::SchemaRef> = None;

        std::fs::create_dir_all(output_dir)?;

        for item in iter {
            let (raw_key, raw_value) = item?;
            let ts_key = crate::key::TsdbKey::decode(&raw_key)?;
            let fields = crate::value::decode_fields(&raw_value)?;
            let tags = if let Some(ref meta_cf) = meta_cf {
                db.db()
                    .get_pinned_cf(meta_cf, ts_key.tags_hash.to_be_bytes())?
                    .and_then(|v| crate::tags::decode_tags(&v).ok())
                    .unwrap_or_default()
            } else {
                tsdb_arrow::schema::Tags::new()
            };
            let dp = tsdb_arrow::schema::DataPoint {
                measurement: measurement.to_string(),
                tags,
                fields,
                timestamp: ts_key.timestamp,
            };

            if schema.is_none() {
                let tag_keys: Vec<String> = dp.tags.keys().cloned().collect();
                let field_types: Vec<(String, _)> = dp
                    .fields
                    .iter()
                    .map(|(k, v)| (k.clone(), tsdb_arrow::schema::field_value_to_data_type(v)))
                    .collect();
                schema = Some(tsdb_arrow::schema::compact_tsdb_schema(
                    measurement,
                    &tag_keys,
                    &field_types,
                ));
            }

            buffer.push(dp);

            if buffer.len() >= batch_size {
                let s = schema.clone().unwrap();
                Self::write_parquet_chunk(output_dir, file_idx, &buffer, &s)?;
                total += buffer.len();
                buffer.clear();
                file_idx += 1;
            }
        }

        if !buffer.is_empty() {
            let s = schema.clone().unwrap();
            Self::write_parquet_chunk(output_dir, file_idx, &buffer, &s)?;
            total += buffer.len();
        }

        Ok(total)
    }

    fn write_parquet_chunk(
        output_dir: &Path,
        file_idx: usize,
        chunk: &[tsdb_arrow::schema::DataPoint],
        schema: &arrow::datatypes::SchemaRef,
    ) -> Result<()> {
        let batch = tsdb_arrow::converter::datapoints_to_record_batch(chunk, schema.clone())?;
        let file_path = output_dir.join(format!("part-{:08}.parquet", file_idx));
        let file = std::fs::File::create(&file_path)?;
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::default(),
            ))
            .build();
        let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(
            file,
            schema.clone(),
            Some(props),
        )
        .map_err(|e| crate::error::TsdbRocksDbError::Io(std::io::Error::other(e.to_string())))?;
        writer
            .write(&batch)
            .map_err(|e| crate::error::TsdbRocksDbError::Io(std::io::Error::other(e.to_string())))?;
        writer
            .close()
            .map_err(|e| crate::error::TsdbRocksDbError::Io(std::io::Error::other(e.to_string())))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RocksDbConfig;
    use tsdb_arrow::schema::{DataPoint, FieldValue};

    fn make_test_db() -> (tempfile::TempDir, TsdbRocksDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = TsdbRocksDb::open(dir.path(), RocksDbConfig::default()).unwrap();
        (dir, db)
    }

    #[test]
    fn test_export_cf_to_json() {
        let (dir, db) = make_test_db();
        let dps: Vec<DataPoint> = (0..5)
            .map(|i| {
                DataPoint::new("cpu", 1_000_000 + i as i64 * 1_000_000)
                    .with_tag("host", "server01")
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.1))
            })
            .collect();
        db.write_batch(&dps).unwrap();

        let cfs = db.list_ts_cfs();
        assert!(!cfs.is_empty());

        let output_path = dir.path().join("export.jsonl");
        let count = DataArchiver::export_cf_to_json(&db, &cfs[0], &output_path).unwrap();
        assert_eq!(count, 5);
        assert!(output_path.exists());

        let content = std::fs::read_to_string(&output_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 5);

        for line in &lines {
            let dp: DataPoint = serde_json::from_str(line).unwrap();
            assert_eq!(dp.measurement, "cpu");
        }
    }

    #[test]
    fn test_export_cf_not_found() {
        let (dir, db) = make_test_db();
        let output_path = dir.path().join("export.jsonl");
        let result = DataArchiver::export_cf_to_json(&db, "ts_nonexistent_20240101", &output_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_archives_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let result = DataArchiver::list_archives(dir.path()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_list_archives_nonexistent_dir() {
        let result =
            DataArchiver::list_archives(std::path::Path::new("/tmp/nonexistent_tsdb_test_dir"))
                .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_list_archives_with_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("cpu_20240101.jsonl"), "").unwrap();
        std::fs::write(dir.path().join("mem_20240101.jsonl"), "").unwrap();
        std::fs::write(dir.path().join("not_json.txt"), "").unwrap();

        let result = DataArchiver::list_archives(dir.path()).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"cpu_20240101.jsonl".to_string()));
        assert!(result.contains(&"mem_20240101.jsonl".to_string()));
    }
}
