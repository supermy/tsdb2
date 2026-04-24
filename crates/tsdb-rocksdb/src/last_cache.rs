use std::collections::HashMap;
use std::sync::RwLock;
use tsdb_arrow::schema::{DataPoint, Fields, Tags};

struct LastCacheEntry {
    timestamp: i64,
    fields: Fields,
    tags: Tags,
}

pub struct LastCache {
    cache: RwLock<HashMap<String, HashMap<String, LastCacheEntry>>>,
    max_series_per_measurement: usize,
}

impl LastCache {
    pub fn new(max_series_per_measurement: usize) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            max_series_per_measurement,
        }
    }

    pub fn update(&self, dp: &DataPoint) {
        let series_key = dp.series_key();
        let mut cache = self.cache.write().unwrap_or_else(|e| e.into_inner());

        let measurement_cache = cache.entry(dp.measurement.clone()).or_default();

        let should_insert = match measurement_cache.get(&series_key) {
            Some(existing) => dp.timestamp >= existing.timestamp,
            None => true,
        };

        if should_insert {
            if measurement_cache.len() >= self.max_series_per_measurement
                && !measurement_cache.contains_key(&series_key)
            {
                if let Some(oldest_key) = measurement_cache
                    .iter()
                    .min_by_key(|(_, v)| v.timestamp)
                    .map(|(k, _)| k.clone())
                {
                    measurement_cache.remove(&oldest_key);
                }
            }
            measurement_cache.insert(
                series_key,
                LastCacheEntry {
                    timestamp: dp.timestamp,
                    fields: dp.fields.clone(),
                    tags: dp.tags.clone(),
                },
            );
        }
    }

    pub fn update_batch(&self, datapoints: &[DataPoint]) {
        for dp in datapoints {
            self.update(dp);
        }
    }

    pub fn get_last(&self, measurement: &str, tags: &Tags) -> Option<DataPoint> {
        let cache = self.cache.read().unwrap_or_else(|e| e.into_inner());
        let measurement_cache = cache.get(measurement)?;

        if tags.is_empty() {
            let latest = measurement_cache.values().max_by_key(|v| v.timestamp)?;
            Some(DataPoint {
                measurement: measurement.to_string(),
                tags: latest.tags.clone(),
                fields: latest.fields.clone(),
                timestamp: latest.timestamp,
            })
        } else {
            let temp_dp = DataPoint {
                measurement: measurement.to_string(),
                tags: tags.clone(),
                fields: Fields::new(),
                timestamp: 0,
            };
            let series_key = temp_dp.series_key();
            let entry = measurement_cache.get(&series_key)?;
            Some(DataPoint {
                measurement: measurement.to_string(),
                tags: entry.tags.clone(),
                fields: entry.fields.clone(),
                timestamp: entry.timestamp,
            })
        }
    }

    pub fn get_all_last(&self, measurement: &str) -> Vec<DataPoint> {
        let cache = self.cache.read().unwrap_or_else(|e| e.into_inner());
        let Some(measurement_cache) = cache.get(measurement) else {
            return Vec::new();
        };
        measurement_cache
            .values()
            .map(|entry| DataPoint {
                measurement: measurement.to_string(),
                tags: entry.tags.clone(),
                fields: entry.fields.clone(),
                timestamp: entry.timestamp,
            })
            .collect()
    }

    pub fn invalidate(&self, measurement: &str) {
        let mut cache = self.cache.write().unwrap_or_else(|e| e.into_inner());
        cache.remove(measurement);
    }

    pub fn invalidate_series(&self, measurement: &str, tags: &Tags) {
        let mut cache = self.cache.write().unwrap_or_else(|e| e.into_inner());
        if let Some(measurement_cache) = cache.get_mut(measurement) {
            let temp_dp = DataPoint {
                measurement: measurement.to_string(),
                tags: tags.clone(),
                fields: Fields::new(),
                timestamp: 0,
            };
            let series_key = temp_dp.series_key();
            measurement_cache.remove(&series_key);
        }
    }

    pub fn series_count(&self, measurement: &str) -> usize {
        let cache = self.cache.read().unwrap_or_else(|e| e.into_inner());
        cache.get(measurement).map(|m| m.len()).unwrap_or(0)
    }

    pub fn measurement_count(&self) -> usize {
        let cache = self.cache.read().unwrap_or_else(|e| e.into_inner());
        cache.len()
    }
}

impl Default for LastCache {
    fn default() -> Self {
        Self::new(100_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tsdb_arrow::schema::FieldValue;

    #[test]
    fn test_last_cache_update_and_get() {
        let cache = LastCache::new(100);

        let dp1 = DataPoint::new("cpu", 1000)
            .with_tag("host", "s1")
            .with_field("usage", FieldValue::Float(0.5));
        cache.update(&dp1);

        let dp2 = DataPoint::new("cpu", 2000)
            .with_tag("host", "s1")
            .with_field("usage", FieldValue::Float(0.8));
        cache.update(&dp2);

        let tags = Tags::new();
        let result = cache.get_last("cpu", &tags).unwrap();
        assert_eq!(result.timestamp, 2000);
        assert_eq!(result.fields.get("usage").unwrap().as_f64().unwrap(), 0.8);
    }

    #[test]
    fn test_last_cache_older_timestamp_not_replacing() {
        let cache = LastCache::new(100);

        let dp1 = DataPoint::new("cpu", 2000)
            .with_tag("host", "s1")
            .with_field("usage", FieldValue::Float(0.8));
        cache.update(&dp1);

        let dp2 = DataPoint::new("cpu", 1000)
            .with_tag("host", "s1")
            .with_field("usage", FieldValue::Float(0.5));
        cache.update(&dp2);

        let tags = Tags::new();
        let result = cache.get_last("cpu", &tags).unwrap();
        assert_eq!(result.timestamp, 2000);
    }

    #[test]
    fn test_last_cache_multiple_series() {
        let cache = LastCache::new(100);

        let dp1 = DataPoint::new("cpu", 1000)
            .with_tag("host", "s1")
            .with_field("usage", FieldValue::Float(0.5));
        let dp2 = DataPoint::new("cpu", 2000)
            .with_tag("host", "s2")
            .with_field("usage", FieldValue::Float(0.8));
        cache.update(&dp1);
        cache.update(&dp2);

        let all = cache.get_all_last("cpu");
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_last_cache_get_with_tags() {
        let cache = LastCache::new(100);

        let dp1 = DataPoint::new("cpu", 1000)
            .with_tag("host", "s1")
            .with_field("usage", FieldValue::Float(0.5));
        let dp2 = DataPoint::new("cpu", 2000)
            .with_tag("host", "s2")
            .with_field("usage", FieldValue::Float(0.8));
        cache.update(&dp1);
        cache.update(&dp2);

        let mut tags = Tags::new();
        tags.insert("host", "s2");
        let result = cache.get_last("cpu", &tags).unwrap();
        assert_eq!(result.timestamp, 2000);
    }

    #[test]
    fn test_last_cache_eviction() {
        let cache = LastCache::new(2);

        let dp1 = DataPoint::new("cpu", 1000)
            .with_tag("host", "s1")
            .with_field("usage", FieldValue::Float(0.1));
        let dp2 = DataPoint::new("cpu", 2000)
            .with_tag("host", "s2")
            .with_field("usage", FieldValue::Float(0.2));
        let dp3 = DataPoint::new("cpu", 3000)
            .with_tag("host", "s3")
            .with_field("usage", FieldValue::Float(0.3));

        cache.update(&dp1);
        cache.update(&dp2);
        cache.update(&dp3);

        assert_eq!(cache.series_count("cpu"), 2);
        assert!(cache.get_last("cpu", &Tags::new()).is_some());
    }

    #[test]
    fn test_last_cache_invalidate() {
        let cache = LastCache::new(100);

        let dp = DataPoint::new("cpu", 1000)
            .with_tag("host", "s1")
            .with_field("usage", FieldValue::Float(0.5));
        cache.update(&dp);

        cache.invalidate("cpu");
        assert_eq!(cache.series_count("cpu"), 0);
    }

    #[test]
    fn test_last_cache_batch_update() {
        let cache = LastCache::new(100);

        let dps: Vec<DataPoint> = (0..10)
            .map(|i| {
                DataPoint::new("cpu", 1000 + i as i64 * 100)
                    .with_tag("host", format!("s{}", i))
                    .with_field("usage", FieldValue::Float(i as f64 * 0.1))
            })
            .collect();

        cache.update_batch(&dps);
        assert_eq!(cache.series_count("cpu"), 10);
    }

    #[test]
    fn test_last_cache_not_found() {
        let cache = LastCache::new(100);
        assert!(cache.get_last("nonexistent", &Tags::new()).is_none());
    }

    #[test]
    fn test_last_cache_invalidate_series() {
        let cache = LastCache::new(100);

        let dp1 = DataPoint::new("cpu", 1000)
            .with_tag("host", "s1")
            .with_field("usage", FieldValue::Float(0.5));
        let dp2 = DataPoint::new("cpu", 2000)
            .with_tag("host", "s2")
            .with_field("usage", FieldValue::Float(0.8));
        cache.update(&dp1);
        cache.update(&dp2);

        let mut tags = Tags::new();
        tags.insert("host", "s1");
        cache.invalidate_series("cpu", &tags);

        assert_eq!(cache.series_count("cpu"), 1);
    }
}
