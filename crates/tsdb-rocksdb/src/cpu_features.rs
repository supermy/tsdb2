use crate::config::RocksDbConfig;

pub struct CpuFeatures {
    pub has_avx2: bool,
    pub has_neon: bool,
    pub has_sse42: bool,
    pub cpu_count: usize,
}

impl std::fmt::Debug for CpuFeatures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CpuFeatures")
            .field("has_avx2", &self.has_avx2)
            .field("has_neon", &self.has_neon)
            .field("has_sse42", &self.has_sse42)
            .field("cpu_count", &self.cpu_count)
            .finish()
    }
}

pub fn detect_cpu_features() -> CpuFeatures {
    CpuFeatures {
        has_avx2: is_x86_feature_detected!("avx2"),
        has_neon: cfg!(target_arch = "aarch64"),
        has_sse42: is_x86_feature_detected!("sse4.2"),
        cpu_count: num_cpus::get(),
    }
}

pub fn optimized_config() -> RocksDbConfig {
    let features = detect_cpu_features();

    let block_size = if features.has_avx2 || features.has_neon {
        8192
    } else {
        4096
    };

    let cache_size = if features.cpu_count >= 8 {
        1024 * 1024 * 1024
    } else if features.cpu_count >= 4 {
        512 * 1024 * 1024
    } else {
        256 * 1024 * 1024
    };

    RocksDbConfig {
        cache_size,
        block_size,
        ..RocksDbConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_cpu_features() {
        let features = detect_cpu_features();
        assert!(features.cpu_count > 0);
    }

    #[test]
    fn test_optimized_config() {
        let config = optimized_config();
        assert!(config.cache_size > 0);
        assert!(config.block_size > 0);
    }

    #[test]
    fn test_cpu_features_debug() {
        let features = detect_cpu_features();
        let debug = format!("{:?}", features);
        assert!(debug.contains("cpu_count"));
    }
}
