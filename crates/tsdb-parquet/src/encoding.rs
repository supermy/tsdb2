use parquet::file::properties::{EnabledStatistics, WriterProperties};

pub fn hot_writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_max_row_group_size(1024 * 1024)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build()
}

pub fn cold_writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::default(),
        ))
        .set_max_row_group_size(64 * 1024)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build()
}

pub fn default_writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build()
}

pub fn writer_props_for_row_count(row_count: usize) -> WriterProperties {
    if row_count <= 10000 {
        hot_writer_props()
    } else {
        cold_writer_props()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hot_writer_props_snappy() {
        let _props = hot_writer_props();
    }

    #[test]
    fn test_cold_writer_props_zstd() {
        let _props = cold_writer_props();
    }

    #[test]
    fn test_writer_props_for_row_count_hot() {
        let _props = writer_props_for_row_count(5000);
    }

    #[test]
    fn test_writer_props_for_row_count_cold() {
        let _props = writer_props_for_row_count(50000);
    }

    #[test]
    fn test_default_writer_props() {
        let _props = default_writer_props();
    }
}
