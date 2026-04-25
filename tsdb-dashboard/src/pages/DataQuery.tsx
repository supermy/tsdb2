import React, { useEffect, useState, useCallback } from 'react';
import { Card, Row, Col, Select, Button, Typography, Table, Tag, InputNumber, Space } from 'antd';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { api, useWebSocket, type MetricsSnapshot, type CollectorStatus } from '../api';

const { Title, Text } = Typography;

const fmtBytes = (bytes: number): string => {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`;
};

const METRIC_OPTIONS = [
  { value: 'cpu_pct', label: 'CPU 使用率 (%)' },
  { value: 'sys_memory_pct', label: '内存使用率 (%)' },
  { value: 'sys_disk_pct', label: '磁盘使用率 (%)' },
  { value: 'cpu_temp_c', label: 'CPU 温度 (°C)' },
  { value: 'gpu_temp_c', label: 'GPU 温度 (°C)' },
  { value: 'load_avg_1m', label: '系统负载 (1m)' },
  { value: 'write_rate', label: '写入速率 (/s)' },
  { value: 'read_rate', label: '读取速率 (/s)' },
  { value: 'net_rx_bytes', label: '网络接收 (B)' },
  { value: 'net_tx_bytes', label: '网络发送 (B)' },
  { value: 'l0_file_count', label: 'L0 文件数' },
];

const DataQuery: React.FC = () => {
  const [collectorStatus, setCollectorStatus] = useState<CollectorStatus | null>(null);
  const [timeseries, setTimeseries] = useState<MetricsSnapshot[]>([]);
  const [selectedMetrics, setSelectedMetrics] = useState<string[]>(['cpu_pct', 'sys_memory_pct', 'cpu_temp_c']);
  const [rangeSecs, setRangeSecs] = useState(300);
  const [intervalSecs, setIntervalSecs] = useState(1);
  const [loading, setLoading] = useState(false);
  const { lastMessage } = useWebSocket();

  const fetchCollectorStatus = useCallback(async () => {
    try {
      const status = await api.collector.status();
      setCollectorStatus(status);
      setIntervalSecs(status.interval_secs);
    } catch (e) { console.error(e); }
  }, []);

  const fetchTimeseries = useCallback(async () => {
    setLoading(true);
    try {
      const ts = await api.metrics.timeseries('default', 'all', rangeSecs);
      setTimeseries(ts);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  }, [rangeSecs]);

  useEffect(() => { fetchCollectorStatus(); }, [fetchCollectorStatus]);
  useEffect(() => { fetchTimeseries(); }, [fetchTimeseries]);

  const allData = lastMessage
    ? [...timeseries.filter(t => t.timestamp_ms !== lastMessage.timestamp_ms), lastMessage]
        .sort((a, b) => a.timestamp_ms - b.timestamp_ms)
        .slice(-300)
    : timeseries;

  const chartData = allData.map(d => ({
    ...d,
    time: new Date(d.timestamp_ms).toLocaleTimeString(),
  }));

  const tableData = allData.slice().reverse().slice(0, 50).map((d, i) => ({
    key: i,
    time: new Date(d.timestamp_ms).toLocaleTimeString(),
    cpu_pct: d.cpu_pct.toFixed(1),
    mem_pct: d.sys_memory_pct.toFixed(1),
    disk_pct: d.sys_disk_pct.toFixed(1),
    cpu_temp: d.cpu_temp_c.toFixed(1),
    gpu_temp: d.gpu_temp_c.toFixed(1),
    load: d.load_avg_1m.toFixed(2),
    write: d.write_rate.toFixed(0),
    read: d.read_rate.toFixed(0),
    net_rx: fmtBytes(d.net_rx_bytes),
    net_tx: fmtBytes(d.net_tx_bytes),
  }));

  const columns = [
    { title: '时间', dataIndex: 'time', key: 'time', width: 100 },
    { title: 'CPU%', dataIndex: 'cpu_pct', key: 'cpu_pct', width: 70 },
    { title: '内存%', dataIndex: 'mem_pct', key: 'mem_pct', width: 70 },
    { title: '磁盘%', dataIndex: 'disk_pct', key: 'disk_pct', width: 70 },
    { title: 'CPU°C', dataIndex: 'cpu_temp', key: 'cpu_temp', width: 70 },
    { title: 'GPU°C', dataIndex: 'gpu_temp', key: 'gpu_temp', width: 70 },
    { title: '负载', dataIndex: 'load', key: 'load', width: 70 },
    { title: '写/s', dataIndex: 'write', key: 'write', width: 70 },
    { title: '读/s', dataIndex: 'read', key: 'read', width: 70 },
    { title: '接收', dataIndex: 'net_rx', key: 'net_rx', width: 80 },
    { title: '发送', dataIndex: 'net_tx', key: 'net_tx', width: 80 },
  ];

  const handleStart = async () => {
    await api.collector.start();
    fetchCollectorStatus();
  };

  const handleStop = async () => {
    await api.collector.stop();
    fetchCollectorStatus();
  };

  const handleConfigure = async () => {
    await api.collector.configure(intervalSecs, true);
    fetchCollectorStatus();
  };

  return (
    <div>
      <Title level={3} style={{ color: '#fff', marginBottom: 16 }}>数据采集查询</Title>

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col span={24}>
          <Card title="采集器控制" size="small">
            <Space wrap>
              <Tag color={collectorStatus?.running ? 'green' : 'red'} style={{ fontSize: 14, padding: '2px 12px' }}>
                {collectorStatus?.running ? '运行中' : '已停止'}
              </Tag>
              <Text style={{ color: '#aaa' }}>
                采集间隔: <Text strong style={{ color: '#fff' }}>{collectorStatus?.interval_secs || '-'}s</Text>
              </Text>
              <Text style={{ color: '#aaa' }}>
                已采集: <Text strong style={{ color: '#fff' }}>{collectorStatus?.snapshots_collected || 0}</Text>
              </Text>
              <Text style={{ color: '#aaa' }}>
                上次采集: {collectorStatus?.last_collect_ms
                  ? new Date(collectorStatus.last_collect_ms).toLocaleTimeString()
                  : '-'}
              </Text>
              <InputNumber
                min={1} max={60} value={intervalSecs} onChange={v => setIntervalSecs(v || 1)}
                addonAfter="秒" style={{ width: 120 }}
              />
              <Button type="primary" onClick={handleConfigure}>应用间隔</Button>
              <Button onClick={handleStart} disabled={collectorStatus?.running}>启动</Button>
              <Button danger onClick={handleStop} disabled={!collectorStatus?.running}>停止</Button>
            </Space>
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col span={24}>
          <Card title="指标图表" size="small" extra={
            <Space>
              <Select
                mode="multiple"
                value={selectedMetrics}
                onChange={setSelectedMetrics}
                options={METRIC_OPTIONS}
                style={{ minWidth: 300 }}
                maxCount={4}
                placeholder="选择指标"
              />
              <InputNumber
                min={60} max={3600} value={rangeSecs} onChange={v => setRangeSecs(v || 300)}
                addonAfter="秒" style={{ width: 130 }}
              />
              <Button onClick={fetchTimeseries} loading={loading}>刷新</Button>
            </Space>
          }>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                <XAxis dataKey="time" stroke="#999" tick={{ fontSize: 11 }} />
                <YAxis stroke="#999" tick={{ fontSize: 11 }} />
                <Tooltip contentStyle={{ background: '#1f1f1f', border: '1px solid #333' }} />
                {selectedMetrics.map((metric, i) => {
                  const colors = ['#1890ff', '#52c41a', '#fa8c16', '#722ed1'];
                  const labels: Record<string, string> = {
                    cpu_pct: 'CPU%', sys_memory_pct: '内存%', sys_disk_pct: '磁盘%',
                    cpu_temp_c: 'CPU°C', gpu_temp_c: 'GPU°C', load_avg_1m: '负载',
                    write_rate: '写/s', read_rate: '读/s', net_rx_bytes: '接收B',
                    net_tx_bytes: '发送B', l0_file_count: 'L0文件',
                  };
                  return (
                    <Line key={metric} type="monotone" dataKey={metric}
                      stroke={colors[i % colors.length]} name={labels[metric] || metric} dot={false} />
                  );
                })}
              </LineChart>
            </ResponsiveContainer>
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col span={24}>
          <Card title="采集数据明细 (最近50条)" size="small">
            <Table
              dataSource={tableData}
              columns={columns}
              size="small"
              pagination={false}
              scroll={{ x: 900 }}
              loading={loading}
            />
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default DataQuery;
