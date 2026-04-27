import React, { useEffect, useState, useRef, useCallback } from 'react';
import { Card, Row, Col, Statistic, Tag, Typography, Select, Spin, Progress } from 'antd';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, AreaChart, Area } from 'recharts';
import { api, useWebSocket, type HealthStatus, type MetricsSnapshot, type Alert } from '../api';
import { fmtBytes } from '../utils';

const { Title, Text } = Typography;

const fmtUptime = (secs: number): string => {
  const d = Math.floor(secs / 86400);
  const h = Math.floor((secs % 86400) / 3600);
  const m = Math.floor((secs % 3600) / 60);
  if (d > 0) return `${d}天${h}时${m}分`;
  if (h > 0) return `${h}时${m}分`;
  return `${m}分`;
};

const pctColor = (v: number) => {
  if (v >= 90) return '#ff4d4f';
  if (v >= 70) return '#faad14';
  return '#52c41a';
};

const Monitoring: React.FC = () => {
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [timeseries, setTimeseries] = useState<MetricsSnapshot[]>([]);
  const [services, setServices] = useState<{value: string, label: string}[]>([{value: 'default', label: 'default'}]);
  const [service, setService] = useState('default');
  const [loading, setLoading] = useState(false);
  const { lastMessage } = useWebSocket();
  const wsDataRef = useRef<MetricsSnapshot[]>([]);

  useEffect(() => {
    api.services.list().then(list => {
      const opts = list.map(s => ({ value: s.name, label: s.name }));
      if (opts.length > 0) setServices(opts);
    }).catch(() => {});
  }, []);

  useEffect(() => {
    if (lastMessage) {
      wsDataRef.current = [...wsDataRef.current.slice(-120), lastMessage];
    }
  }, [lastMessage]);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [h, a, ts] = await Promise.all([
        api.metrics.health(service),
        api.metrics.alerts(service),
        api.metrics.timeseries(service, 'all', 300),
      ]);
      setHealth(h);
      setAlerts(a);
      setTimeseries(ts);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  }, [service]);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const chartData = wsDataRef.current.length > 5 ? wsDataRef.current : timeseries;
  const formattedData = chartData.map(d => ({
    ...d,
    time: new Date(d.timestamp_ms).toLocaleTimeString(),
    memory_mb: +(d.memory_bytes / 1024 / 1024).toFixed(1),
    disk_mb: +(d.disk_bytes / 1024 / 1024).toFixed(1),
    sys_memory_used_gb: +(d.sys_memory_used_bytes / 1024 / 1024 / 1024).toFixed(2),
    sys_disk_used_gb: +(d.sys_disk_used_bytes / 1024 / 1024 / 1024).toFixed(2),
    net_rx_kb: +(d.net_rx_bytes / 1024).toFixed(1),
    net_tx_kb: +(d.net_tx_bytes / 1024).toFixed(1),
  }));

  const alertColor = (level: string) => {
    switch (level) {
      case 'Critical': return 'red';
      case 'Warning': return 'orange';
      default: return 'blue';
    }
  };

  const latest = formattedData.length > 0 ? formattedData[formattedData.length - 1] : null;

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={3} style={{ color: '#fff', margin: 0 }}>系统监控</Title>
        <Select value={service} onChange={setService} style={{ width: 200 }} options={services} />
      </div>

      <Spin spinning={loading}>
        {health && (
          <>
            <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
              <Col span={3}>
                <Card size="small" bodyStyle={{ textAlign: 'center' }}>
                  <Statistic
                    title="服务状态"
                    value={health.healthy ? '正常' : '异常'}
                    valueStyle={{ color: health.healthy ? '#52c41a' : '#ff4d4f', fontSize: 18 }}
                  />
                  <Text type="secondary" style={{ fontSize: 11 }}>运行 {fmtUptime(health.uptime_secs)}</Text>
                </Card>
              </Col>
              <Col span={3}>
                <Card size="small" bodyStyle={{ textAlign: 'center' }}>
                  <Progress type="dashboard" percent={Math.round(health.cpu_pct)} size={70}
                    strokeColor={pctColor(health.cpu_pct)} format={p => `${p}%`} />
                  <div style={{ marginTop: 2, color: '#aaa', fontSize: 11 }}>CPU</div>
                </Card>
              </Col>
              <Col span={3}>
                <Card size="small" bodyStyle={{ textAlign: 'center' }}>
                  <Progress type="dashboard" percent={Math.round(health.sys_memory_pct)} size={70}
                    strokeColor={pctColor(health.sys_memory_pct)} format={p => `${p}%`} />
                  <div style={{ marginTop: 2, color: '#aaa', fontSize: 11 }}>内存</div>
                </Card>
              </Col>
              <Col span={3}>
                <Card size="small" bodyStyle={{ textAlign: 'center' }}>
                  <Progress type="dashboard" percent={Math.round(health.sys_disk_pct)} size={70}
                    strokeColor={pctColor(health.sys_disk_pct)} format={p => `${p}%`} />
                  <div style={{ marginTop: 2, color: '#aaa', fontSize: 11 }}>磁盘</div>
                </Card>
              </Col>
              <Col span={3}>
                <Card size="small" bodyStyle={{ textAlign: 'center' }}>
                  <Statistic title="CPU°C" value={health.cpu_temp_c.toFixed(1)}
                    suffix="°C" valueStyle={{ color: health.cpu_temp_c > 70 ? '#ff4d4f' : health.cpu_temp_c > 50 ? '#faad14' : '#52c41a', fontSize: 20 }} />
                </Card>
              </Col>
              <Col span={3}>
                <Card size="small" bodyStyle={{ textAlign: 'center' }}>
                  <Statistic title="GPU°C" value={health.gpu_temp_c.toFixed(1)}
                    suffix="°C" valueStyle={{ color: health.gpu_temp_c > 70 ? '#ff4d4f' : health.gpu_temp_c > 50 ? '#faad14' : '#52c41a', fontSize: 20 }} />
                </Card>
              </Col>
              <Col span={3}>
                <Card size="small" bodyStyle={{ textAlign: 'center' }}>
                  <Statistic title="负载(1m)" value={health.load_avg_1m.toFixed(2)}
                    valueStyle={{ color: health.load_avg_1m > 4 ? '#faad14' : '#52c41a' }} />
                </Card>
              </Col>
              <Col span={3}>
                <Card size="small" bodyStyle={{ textAlign: 'center' }}>
                  <Statistic title="数据点" value={health.total_data_points}
                    valueStyle={{ fontSize: 16 }} />
                  <Text type="secondary" style={{ fontSize: 11 }}>{health.measurements_count} ms</Text>
                </Card>
              </Col>
            </Row>

            {latest && (
              <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
                <Col span={6}>
                  <Card size="small">
                    <Statistic title="系统内存" value={fmtBytes(latest.sys_memory_used_bytes)}
                      suffix={`/ ${fmtBytes(latest.sys_memory_total_bytes)}`}
                      valueStyle={{ fontSize: 16 }} />
                  </Card>
                </Col>
                <Col span={6}>
                  <Card size="small">
                    <Statistic title="系统磁盘" value={fmtBytes(latest.sys_disk_used_bytes)}
                      suffix={`/ ${fmtBytes(latest.sys_disk_total_bytes)}`}
                      valueStyle={{ fontSize: 16 }} />
                  </Card>
                </Col>
                <Col span={6}>
                  <Card size="small">
                    <Statistic title="网络接收" value={fmtBytes(latest.net_rx_bytes)}
                      suffix="/s" valueStyle={{ fontSize: 16, color: '#1890ff' }} />
                  </Card>
                </Col>
                <Col span={6}>
                  <Card size="small">
                    <Statistic title="网络发送" value={fmtBytes(latest.net_tx_bytes)}
                      suffix="/s" valueStyle={{ fontSize: 16, color: '#52c41a' }} />
                  </Card>
                </Col>
              </Row>
            )}
          </>
        )}

        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          <Col span={12}>
            <Card title="CPU 使用率" size="small">
              <ResponsiveContainer width="100%" height={220}>
                <AreaChart data={formattedData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                  <XAxis dataKey="time" stroke="#999" tick={{ fontSize: 11 }} />
                  <YAxis stroke="#999" domain={[0, 100]} tick={{ fontSize: 11 }} unit="%" />
                  <Tooltip contentStyle={{ background: '#1f1f1f', border: '1px solid #333' }} />
                  <Area type="monotone" dataKey="cpu_pct" stroke="#1890ff" fill="#1890ff22" name="CPU %" dot={false} />
                </AreaChart>
              </ResponsiveContainer>
            </Card>
          </Col>
          <Col span={12}>
            <Card title="内存使用率" size="small">
              <ResponsiveContainer width="100%" height={220}>
                <AreaChart data={formattedData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                  <XAxis dataKey="time" stroke="#999" tick={{ fontSize: 11 }} />
                  <YAxis stroke="#999" domain={[0, 100]} tick={{ fontSize: 11 }} unit="%" />
                  <Tooltip contentStyle={{ background: '#1f1f1f', border: '1px solid #333' }} />
                  <Area type="monotone" dataKey="sys_memory_pct" stroke="#722ed1" fill="#722ed122" name="内存 %" dot={false} />
                </AreaChart>
              </ResponsiveContainer>
            </Card>
          </Col>
        </Row>

        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          <Col span={12}>
            <Card title="磁盘使用率" size="small">
              <ResponsiveContainer width="100%" height={220}>
                <AreaChart data={formattedData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                  <XAxis dataKey="time" stroke="#999" tick={{ fontSize: 11 }} />
                  <YAxis stroke="#999" domain={[0, 100]} tick={{ fontSize: 11 }} unit="%" />
                  <Tooltip contentStyle={{ background: '#1f1f1f', border: '1px solid #333' }} />
                  <Area type="monotone" dataKey="sys_disk_pct" stroke="#fa8c16" fill="#fa8c1622" name="磁盘 %" dot={false} />
                </AreaChart>
              </ResponsiveContainer>
            </Card>
          </Col>
          <Col span={12}>
            <Card title="网络流量 (KB/s)" size="small">
              <ResponsiveContainer width="100%" height={220}>
                <AreaChart data={formattedData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                  <XAxis dataKey="time" stroke="#999" tick={{ fontSize: 11 }} />
                  <YAxis stroke="#999" tick={{ fontSize: 11 }} />
                  <Tooltip contentStyle={{ background: '#1f1f1f', border: '1px solid #333' }} />
                  <Area type="monotone" dataKey="net_rx_kb" stroke="#1890ff" fill="#1890ff22" name="接收 KB/s" dot={false} />
                  <Area type="monotone" dataKey="net_tx_kb" stroke="#52c41a" fill="#52c41a22" name="发送 KB/s" dot={false} />
                </AreaChart>
              </ResponsiveContainer>
            </Card>
          </Col>
        </Row>

        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          <Col span={12}>
            <Card title="温度 (°C)" size="small">
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={formattedData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                  <XAxis dataKey="time" stroke="#999" tick={{ fontSize: 11 }} />
                  <YAxis stroke="#999" tick={{ fontSize: 11 }} unit="°C" />
                  <Tooltip contentStyle={{ background: '#1f1f1f', border: '1px solid #333' }} />
                  <Line type="monotone" dataKey="cpu_temp_c" stroke="#ff4d4f" name="CPU °C" dot={false} />
                  <Line type="monotone" dataKey="gpu_temp_c" stroke="#faad14" name="GPU °C" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </Card>
          </Col>
          <Col span={12}>
            <Card title="写入/读取速率" size="small">
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={formattedData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                  <XAxis dataKey="time" stroke="#999" tick={{ fontSize: 11 }} />
                  <YAxis stroke="#999" tick={{ fontSize: 11 }} />
                  <Tooltip contentStyle={{ background: '#1f1f1f', border: '1px solid #333' }} />
                  <Line type="monotone" dataKey="write_rate" stroke="#1890ff" name="写入/s" dot={false} />
                  <Line type="monotone" dataKey="read_rate" stroke="#52c41a" name="读取/s" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </Card>
          </Col>
          <Col span={12}>
            <Card title="系统负载 & L0文件" size="small">
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={formattedData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                  <XAxis dataKey="time" stroke="#999" tick={{ fontSize: 11 }} />
                  <YAxis yAxisId="left" stroke="#999" tick={{ fontSize: 11 }} />
                  <YAxis yAxisId="right" orientation="right" stroke="#999" tick={{ fontSize: 11 }} />
                  <Tooltip contentStyle={{ background: '#1f1f1f', border: '1px solid #333' }} />
                  <Line yAxisId="left" type="monotone" dataKey="load_avg_1m" stroke="#eb2f96" name="负载(1m)" dot={false} />
                  <Line yAxisId="right" type="monotone" dataKey="l0_file_count" stroke="#ff4d4f" name="L0文件" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </Card>
          </Col>
        </Row>

        <Row gutter={[16, 16]}>
          <Col span={12}>
            <Card title="RocksDB 内存/磁盘" size="small">
              <ResponsiveContainer width="100%" height={200}>
                <AreaChart data={formattedData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                  <XAxis dataKey="time" stroke="#999" tick={{ fontSize: 11 }} />
                  <YAxis stroke="#999" tick={{ fontSize: 11 }} unit="MB" />
                  <Tooltip contentStyle={{ background: '#1f1f1f', border: '1px solid #333' }} />
                  <Area type="monotone" dataKey="memory_mb" stroke="#722ed1" fill="#722ed122" name="内存(MB)" dot={false} />
                  <Area type="monotone" dataKey="disk_mb" stroke="#fa8c16" fill="#fa8c1622" name="磁盘(MB)" dot={false} />
                </AreaChart>
              </ResponsiveContainer>
            </Card>
          </Col>
          <Col span={12}>
            <Card title="告警" size="small">
              {alerts.map((alert) => (
                <div key={`${alert.timestamp_ms}-${alert.message}`} style={{ marginBottom: 8, display: 'flex', gap: 8, alignItems: 'center' }}>
                  <Tag color={alertColor(alert.level)}>{alert.level}</Tag>
                  <span style={{ color: '#ccc' }}>{alert.message}</span>
                  <span style={{ color: '#666', fontSize: 12 }}>{new Date(alert.timestamp_ms).toLocaleTimeString()}</span>
                </div>
              ))}
              {alerts.length === 0 && <span style={{ color: '#999' }}>暂无告警</span>}
            </Card>
          </Col>
        </Row>
      </Spin>
    </div>
  );
};

export default Monitoring;
