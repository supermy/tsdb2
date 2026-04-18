use chrono::NaiveDate;
use tsdb_arrow::schema::{DataPoint, FieldValue, Tags};

/// 生成简单的 CPU 指标数据点
///
/// 每个数据点间隔 1 秒, 包含 host 和 region 标签,
/// 以及 usage/idle/count 三个字段。
pub fn make_simple_datapoints(count: usize) -> Vec<DataPoint> {
    (0..count)
        .map(|i| {
            DataPoint::new("cpu", 1_000_000_000 + i as i64 * 1_000_000)
                .with_tag("host", format!("host_{:04}", i % 10))
                .with_tag("region", if i % 2 == 0 { "us-west" } else { "us-east" })
                .with_field("usage", FieldValue::Float(0.3 + i as f64 * 0.01))
                .with_field("idle", FieldValue::Float(0.7 - i as f64 * 0.01))
                .with_field("count", FieldValue::Integer(i as i64 * 10))
        })
        .collect()
}

/// 生成 DevOps 监控场景的多设备时序数据
///
/// 模拟多个设备在指定小时数内的 CPU 监控数据, 每个设备每分钟一条记录。
/// 基准时间戳使用当前 UTC 时间动态获取, 避免硬编码日期。
pub fn make_devops_datapoints(device_count: usize, hours: usize) -> Vec<DataPoint> {
    let mut dps = Vec::new();
    let base_ts = chrono::Utc::now().timestamp_micros();
    let interval = 60_000_000i64;

    for device in 0..device_count {
        let hostname = format!("host_{:04}", device);
        let region = if device % 2 == 0 {
            "us-west"
        } else {
            "us-east"
        };

        for minute in 0..(hours * 60) {
            let ts = base_ts + minute as i64 * interval;
            let cpu_usage = 0.3 + (device as f64 * 0.01) + (minute as f64 * 0.0001);
            let disk_io = (device * 100 + minute) as i64;

            dps.push(
                DataPoint::new("cpu", ts)
                    .with_tag("host", &hostname)
                    .with_tag("region", region)
                    .with_field("usage", FieldValue::Float(cpu_usage.min(0.99)))
                    .with_field("idle", FieldValue::Float((1.0 - cpu_usage).max(0.01)))
                    .with_field("count", FieldValue::Integer(disk_io)),
            );
        }
    }

    dps
}

/// 生成包含多种字段类型的混合数据点
///
/// 包含 Float、Integer、String、Boolean 四种字段类型。
pub fn make_mixed_type_datapoints(count: usize) -> Vec<DataPoint> {
    (0..count)
        .map(|i| {
            DataPoint::new("metrics", 1_000_000 + i as i64 * 1_000_000)
                .with_tag("host", "server01")
                .with_field("cpu", FieldValue::Float(0.5 + i as f64 * 0.01))
                .with_field("requests", FieldValue::Integer(i as i64 * 100))
                .with_field(
                    "status",
                    FieldValue::String(if i % 2 == 0 { "ok" } else { "error" }.to_string()),
                )
                .with_field("active", FieldValue::Boolean(i % 3 != 0))
        })
        .collect()
}

/// 生成跨越多天的时序数据点
///
/// 每天生成 1000 条记录, 用于测试按天分区和跨天查询功能。
/// 基准时间戳使用当前 UTC 时间动态获取, 避免硬编码日期。
pub fn make_cross_day_datapoints(days: usize) -> Vec<DataPoint> {
    let mut dps = Vec::new();
    let base_ts = chrono::Utc::now().timestamp_micros();
    let one_day_micros = 86_400_000_000i64;

    for day in 0..days {
        for i in 0..1000 {
            let ts = base_ts + day as i64 * one_day_micros + i as i64 * 60_000_000;
            dps.push(
                DataPoint::new("cpu", ts)
                    .with_tag("host", format!("host_{:03}", i % 5))
                    .with_tag("region", "us-west")
                    .with_field("usage", FieldValue::Float(0.5 + i as f64 * 0.001))
                    .with_field("count", FieldValue::Integer(i as i64)),
            );
        }
    }

    dps
}

/// 生成大规模真实场景模拟数据 (1000 台设备 × 24 小时)
pub fn make_realistic_series() -> Vec<DataPoint> {
    make_devops_datapoints(1000, 24)
}

/// 将微秒时间戳转换为日期
pub fn micros_to_date(micros: i64) -> NaiveDate {
    let secs = micros / 1_000_000;
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|dt| dt.date_naive())
        .unwrap_or_else(|| chrono::Local::now().date_naive())
}

/// 创建空的标签集合
pub fn empty_tags() -> Tags {
    Tags::new()
}

/// 生成增强版 DevOps 监控数据
///
/// 支持周期性模式（白天高、夜间低）和异常注入。
pub fn make_devops_datapoints_enhanced(
    hosts: usize,
    hours: usize,
    measurements: &[&str],
    anomaly_rate: f64,
) -> Vec<DataPoint> {
    let mut dps = Vec::new();
    let base_ts = chrono::Utc::now().timestamp_micros();

    for host in 0..hosts {
        let host_name = format!("host_{:04}", host);
        for hour in 0..hours {
            let is_daytime = (hour % 24) >= 8 && (hour % 24) < 20;
            let base_value = if is_daytime { 60.0 } else { 20.0 };
            let noise = ((host * 7 + hour * 3) as f64 * 0.1).sin() * 5.0;

            for (m_idx, measurement) in measurements.iter().enumerate() {
                let ts = base_ts + (hour * 3600 + m_idx * 60) as i64 * 1_000_000;

                let is_anomaly =
                    ((host * 13 + hour * 7 + m_idx * 3) as f64 * 0.01 % 1.0) < anomaly_rate;
                let value = if is_anomaly {
                    base_value + 50.0
                } else {
                    base_value + noise + m_idx as f64 * 2.0
                };

                let dp = DataPoint::new(*measurement, ts)
                    .with_tag("host", &host_name)
                    .with_tag("region", if host % 2 == 0 { "us-east" } else { "eu-west" })
                    .with_field("value", FieldValue::Float(value))
                    .with_field("count", FieldValue::Integer((value * 100.0) as i64))
                    .with_field("anomaly", FieldValue::Boolean(is_anomaly));

                dps.push(dp);
            }
        }
    }

    dps
}

/// 生成 IoT 传感器高频数据
///
/// 模拟大量设备以秒级间隔上报传感器数据。
pub fn make_iot_datapoints(devices: usize, duration_hours: usize) -> Vec<DataPoint> {
    let mut dps = Vec::new();
    let base_ts = chrono::Utc::now().timestamp_micros();
    let interval = 1_000_000i64;

    for device in 0..devices {
        let device_id = format!("device_{:06}", device);
        let region = match device % 3 {
            0 => "zone-a",
            1 => "zone-b",
            _ => "zone-c",
        };
        let device_type = match device % 4 {
            0 => "temperature",
            1 => "humidity",
            2 => "pressure",
            _ => "vibration",
        };

        let points_per_device = duration_hours * 3600;
        for sec in 0..points_per_device {
            let ts = base_ts + sec as i64 * interval;
            let value = match device % 4 {
                0 => 20.0 + (device as f64 * 0.1) + (sec as f64 * 0.001).sin() * 5.0,
                1 => 50.0 + (sec as f64 * 0.002).cos() * 10.0,
                2 => 1013.0 + (sec as f64 * 0.0005).sin() * 5.0,
                _ => 0.5 + (sec as f64 * 0.01).sin().abs(),
            };

            dps.push(
                DataPoint::new("iot_sensor", ts)
                    .with_tag("device_id", &device_id)
                    .with_tag("region", region)
                    .with_tag("type", device_type)
                    .with_field("value", FieldValue::Float(value))
                    .with_field(
                        "quality",
                        FieldValue::Integer(if sec % 7 == 0 { 0 } else { 100 }),
                    ),
            );
        }
    }

    dps
}

/// 生成金融 OHLCV K线数据
///
/// 使用几何布朗运动模型生成价格数据。
pub fn make_financial_ohlcv(tickers: usize, bars: usize) -> Vec<DataPoint> {
    let mut dps = Vec::new();
    let base_ts = chrono::Utc::now().timestamp_micros();
    let bar_interval = 60_000_000i64;

    for ticker in 0..tickers {
        let symbol = format!("SYM{}", (b'A' + (ticker % 26) as u8) as char);
        let mut price = 100.0 + ticker as f64 * 10.0;
        let volatility = 0.02;

        for bar in 0..bars {
            let ts = base_ts + bar as i64 * bar_interval;
            let drift = 0.0001;
            let shock = volatility * ((bar as f64 * 0.1).sin() * 2.0 - 1.0);
            price *= 1.0 + drift + shock;
            price = price.max(1.0);

            let open = price;
            let high = price * (1.0 + ((bar as f64 * 0.3).sin().abs()) * 0.01);
            let low = price * (1.0 - ((bar as f64 * 0.2).cos().abs()) * 0.01);
            let close = price * (1.0 + shock * 0.5);
            let volume = (1000 + (bar * 100 + ticker * 50)) as i64;

            dps.push(
                DataPoint::new("ohlcv", ts)
                    .with_tag("symbol", &symbol)
                    .with_field("open", FieldValue::Float(open))
                    .with_field("high", FieldValue::Float(high))
                    .with_field("low", FieldValue::Float(low))
                    .with_field("close", FieldValue::Float(close))
                    .with_field("volume", FieldValue::Integer(volume)),
            );
        }
    }

    dps
}
