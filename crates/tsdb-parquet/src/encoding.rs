use parquet::file::properties::WriterProperties;

/// 热数据 Writer 属性
///
/// 热数据频繁读写, 使用较低的压缩率和较大的行组,
/// 牺牲存储空间换取读写性能。
pub fn hot_writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_max_row_group_size(1024 * 1024)
        .build()
}

/// 冷数据 Writer 属性
///
/// 冷数据很少访问, 使用高压缩率减小存储占用。
pub fn cold_writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::default(),
        ))
        .set_max_row_group_size(64 * 1024)
        .build()
}

/// 默认 Writer 属性
///
/// 平衡压缩率和性能。
pub fn default_writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build()
}

/// 根据行数自动选择 hot/cold 编码
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
