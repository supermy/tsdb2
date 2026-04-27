use crate::file_stats::FileStats;
use std::collections::HashMap;

pub fn prune_files_by_time_range(
    files: &[FileStats],
    start_micros: i64,
    end_micros: i64,
) -> Vec<&FileStats> {
    files
        .iter()
        .filter(|f| {
            if let (Some(min), Some(max)) = (f.timestamp_min, f.timestamp_max) {
                max >= start_micros && min <= end_micros
            } else {
                true
            }
        })
        .collect()
}

pub fn prune_files_by_tags<'a>(
    files: &'a [FileStats],
    tag_filters: &HashMap<String, String>,
) -> Vec<&'a FileStats> {
    files
        .iter()
        .filter(|f| {
            for (tag_key, tag_value) in tag_filters {
                if let Some(stats) = f.tag_values.get(tag_key) {
                    if let Some(ref min) = stats.min {
                        if tag_value < min {
                            return false;
                        }
                    }
                    if let Some(ref max) = stats.max {
                        if tag_value > max {
                            return false;
                        }
                    }
                }
            }
            true
        })
        .collect()
}

pub fn prune_files<'a>(
    files: &'a [FileStats],
    time_range: Option<(i64, i64)>,
    tag_filters: &HashMap<String, String>,
) -> Vec<&'a FileStats> {
    let result: Vec<&FileStats> = if let Some((start, end)) = time_range {
        files
            .iter()
            .filter(|f| {
                if let (Some(min), Some(max)) = (f.timestamp_min, f.timestamp_max) {
                    max >= start && min <= end
                } else {
                    true
                }
            })
            .collect()
    } else {
        files.iter().collect()
    };

    if tag_filters.is_empty() {
        result
    } else {
        result
            .into_iter()
            .filter(|f| {
                for (tag_key, tag_value) in tag_filters {
                    if let Some(stats) = f.tag_values.get(tag_key) {
                        if let Some(ref min) = stats.min {
                            if tag_value < min {
                                return false;
                            }
                        }
                        if let Some(ref max) = stats.max {
                            if tag_value > max {
                                return false;
                            }
                        }
                    }
                }
                true
            })
            .collect()
    }
}

pub fn prune_row_groups(
    metadata: &parquet::file::metadata::ParquetMetaData,
    time_range: Option<(i64, i64)>,
) -> Vec<usize> {
    let row_groups = metadata.row_groups();
    let mut result = Vec::new();

    for (idx, rg) in row_groups.iter().enumerate() {
        let mut include = true;

        if let Some((start, end)) = time_range {
            for col in rg.columns() {
                if col.column_path().string() == "timestamp" {
                    if let Some(stats) = col.statistics() {
                        if let (Some(max_bs), Some(min_bs)) = (stats.max_bytes_opt(), stats.min_bytes_opt()) {
                            if max_bs.len() == 8 {
                                let max_ts = i64::from_le_bytes(max_bs.try_into().unwrap_or([0; 8]));
                                if max_ts < start {
                                    include = false;
                                }
                            }
                            if min_bs.len() == 8 {
                                let min_ts = i64::from_le_bytes(min_bs.try_into().unwrap_or([0; 8]));
                                if min_ts > end {
                                    include = false;
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }

        if include {
            result.push(idx);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_stats(
        ts_min: i64,
        ts_max: i64,
        tag_host_min: Option<&str>,
        tag_host_max: Option<&str>,
    ) -> FileStats {
        let mut tag_values = HashMap::new();
        if let (Some(min), Some(max)) = (tag_host_min, tag_host_max) {
            tag_values.insert(
                "host".to_string(),
                crate::file_stats::ValueStats {
                    min: Some(min.to_string()),
                    max: Some(max.to_string()),
                    null_count: 0,
                },
            );
        }
        FileStats {
            file_path: format!("test_{}_{}.parquet", ts_min, ts_max),
            measurement: "cpu".to_string(),
            date: "20260427".to_string(),
            tier: "warm".to_string(),
            row_count: 1000,
            size_bytes: 4096,
            timestamp_min: Some(ts_min),
            timestamp_max: Some(ts_max),
            tags_hash_min: None,
            tags_hash_max: None,
            tag_values,
        }
    }

    #[test]
    fn test_prune_files_by_time_range() {
        let files = vec![
            make_stats(100, 200, None, None),
            make_stats(300, 400, None, None),
            make_stats(500, 600, None, None),
        ];

        let result = prune_files_by_time_range(&files, 250, 450);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].timestamp_min, Some(300));
    }

    #[test]
    fn test_prune_files_by_tags() {
        let files = vec![
            make_stats(100, 200, Some("server01"), Some("server10")),
            make_stats(300, 400, Some("server20"), Some("server30")),
            make_stats(500, 600, Some("server40"), Some("server50")),
        ];

        let mut filters = HashMap::new();
        filters.insert("host".to_string(), "server25".to_string());

        let result = prune_files_by_tags(&files, &filters);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].timestamp_min, Some(300));
    }

    #[test]
    fn test_prune_files_combined() {
        let files = vec![
            make_stats(100, 200, Some("a"), Some("c")),
            make_stats(300, 400, Some("d"), Some("f")),
            make_stats(500, 600, Some("g"), Some("i")),
        ];

        let mut filters = HashMap::new();
        filters.insert("host".to_string(), "e".to_string());

        let result = prune_files(&files, Some((250, 450)), &filters);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].timestamp_min, Some(300));
    }

    #[test]
    fn test_prune_files_no_match() {
        let files = vec![
            make_stats(100, 200, Some("a"), Some("c")),
            make_stats(300, 400, Some("d"), Some("f")),
        ];

        let mut filters = HashMap::new();
        filters.insert("host".to_string(), "z".to_string());

        let result = prune_files(&files, None, &filters);
        assert!(result.is_empty());
    }
}
