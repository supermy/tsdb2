import React, { useEffect, useState } from 'react';
import { Row, Col, Card, Statistic, Tag, Typography, Spin, Button, Select, InputNumber, Space, message, Table, Progress, Switch } from 'antd';
import {
  DatabaseOutlined,
  CloudServerOutlined,
  CheckCircleOutlined,
  WarningOutlined,
  ExperimentOutlined,
} from '@ant-design/icons';
import { api, useWebSocket, type HealthStatus, type ServiceInfo, type MeasurementSchema, type GenResult } from '../api';

const { Title, Text } = Typography;

const SCENARIOS = [
  { value: 'iot', label: 'IoT 物联网' },
  { value: 'devops', label: 'DevOps 运维' },
  { value: 'ecommerce', label: '电商业务' },
  { value: 'generic', label: '通用指标' },
];

const Dashboard: React.FC = () => {
  const [services, setServices] = useState<ServiceInfo[]>([]);
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [scenario, setScenario] = useState('iot');
  const [pointsPerSeries, setPointsPerSeries] = useState(60);
  const [generating, setGenerating] = useState(false);
  const [skipAutoDemote, setSkipAutoDemote] = useState(true);
  const [genResult, setGenResult] = useState<GenResult | null>(null);
  const [measurements, setMeasurements] = useState<MeasurementSchema[]>([]);
  const { lastMessage } = useWebSocket();

  useEffect(() => {
    const fetchData = async () => {
      try {
        setError(null);
        const svcList = await api.services.list();
        setServices(svcList);
        if (svcList.length > 0) {
          const h = await api.metrics.health(svcList[0].name);
          setHealth(h);
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setLoading(false);
      }
    };
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    api.rocksdb.seriesSchema().then(s => setMeasurements(s.measurements)).catch(() => {});
  }, [genResult]);

  const handleGenerate = async () => {
    setGenerating(true);
    try {
      const result = await api.test.generateBusinessData(scenario, pointsPerSeries, skipAutoDemote);
      setGenResult(result);
      message.success(`生成完成: ${result.total_points} 条数据`);
    } catch (e) {
      message.error(e instanceof Error ? e.message : String(e));
    } finally {
      setGenerating(false);
    }
  };

  const businessMeasurements = measurements.filter(m => !m.name.startsWith('sys_'));
  const sysMeasurements = measurements.filter(m => m.name.startsWith('sys_'));
  const totalBusinessSeries = businessMeasurements.reduce((s, m) => s + m.series.length, 0);
  const totalSysSeries = sysMeasurements.reduce((s, m) => s + m.series.length, 0);

  const runningCount = services.filter(s => s.status === 'Running').length;
  const errorCount = services.filter(s => s.status === 'Error').length;

  const measurementColumns = [
    { title: 'Measurement', dataIndex: 'name', key: 'name', render: (v: string) => <Tag color={v.startsWith('sys_') ? 'default' : 'blue'}>{v}</Tag> },
    { title: '序列数', dataIndex: 'series', key: 'series', render: (v: { tags: Record<string, string> }[]) => v.length },
    { title: 'Tag Keys', dataIndex: 'tag_keys', key: 'tag_keys', render: (v: string[]) => v.map(t => <Tag key={t}>{t}</Tag>) },
    { title: 'Fields', dataIndex: 'field_names', key: 'field_names', render: (v: string[]) => v.map(f => <Tag key={f} color="green">{f}</Tag>) },
  ];

  return (
    <div>
      <Title level={3} style={{ color: '#fff' }}>仪表盘</Title>
      <Spin spinning={loading}>
        {error && (
          <Card style={{ marginBottom: 16, borderColor: '#ff4d4f' }}>
            <Tag color="red">加载失败</Tag>
            <span style={{ color: '#ff4d4f', marginLeft: 8 }}>{error}</span>
          </Card>
        )}
        <Row gutter={[16, 16]}>
          <Col span={6}>
            <Card><Statistic title="服务总数" value={services.length} prefix={<CloudServerOutlined />} /></Card>
          </Col>
          <Col span={6}>
            <Card><Statistic title="运行中" value={runningCount} prefix={<CheckCircleOutlined />} valueStyle={{ color: '#52c41a' }} /></Card>
          </Col>
          <Col span={6}>
            <Card><Statistic title="异常" value={errorCount} prefix={<WarningOutlined />} valueStyle={{ color: errorCount > 0 ? '#ff4d4f' : '#52c41a' }} /></Card>
          </Col>
          <Col span={6}>
            <Card><Statistic title="数据点总数" value={health?.total_data_points ?? 0} prefix={<DatabaseOutlined />} /></Card>
          </Col>
        </Row>

        <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
          <Col span={6}>
            <Card><Statistic title="业务 Measurement" value={businessMeasurements.length} valueStyle={{ color: '#1890ff' }} /></Card>
          </Col>
          <Col span={6}>
            <Card><Statistic title="业务序列数" value={totalBusinessSeries} valueStyle={{ color: '#1890ff' }} /></Card>
          </Col>
          <Col span={6}>
            <Card><Statistic title="系统 Measurement" value={sysMeasurements.length} valueStyle={{ color: '#faad14' }} /></Card>
          </Col>
          <Col span={6}>
            <Card><Statistic title="系统序列数" value={totalSysSeries} valueStyle={{ color: '#faad14' }} /></Card>
          </Col>
        </Row>

        <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
          <Col span={12}>
            <Card title="服务状态">
              {services.map(svc => (
                <div key={svc.name} style={{ marginBottom: 8, display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ color: '#fff' }}>{svc.name}</span>
                  <Tag color={svc.status === 'Running' ? 'green' : svc.status === 'Error' ? 'red' : 'default'}>
                    {svc.status}
                  </Tag>
                </div>
              ))}
              {services.length === 0 && <span style={{ color: '#999' }}>暂无服务</span>}
            </Card>
          </Col>
          <Col span={12}>
            <Card title="实时指标">
              {lastMessage ? (
                <Row gutter={[16, 16]}>
                  <Col span={12}><Statistic title="写入速率" value={lastMessage.write_rate.toFixed(1)} suffix="/s" /></Col>
                  <Col span={12}><Statistic title="读取速率" value={lastMessage.read_rate.toFixed(1)} suffix="/s" /></Col>
                  <Col span={12}><Statistic title="内存使用" value={(lastMessage.memory_bytes / 1024 / 1024).toFixed(1)} suffix="MB" /></Col>
                  <Col span={12}><Statistic title="磁盘使用" value={(lastMessage.disk_bytes / 1024 / 1024).toFixed(1)} suffix="MB" /></Col>
                  <Col span={12}><Statistic title="L0文件数" value={lastMessage.l0_file_count} /></Col>
                  <Col span={12}><Statistic title="压缩计数" value={lastMessage.compaction_count} /></Col>
                </Row>
              ) : (
                <span style={{ color: '#999' }}>等待数据...</span>
              )}
            </Card>
          </Col>
        </Row>

        <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
          <Col span={24}>
            <Card title={<span><ExperimentOutlined style={{ marginRight: 8 }} />多业务数据生成</span>}>
              <Space wrap style={{ marginBottom: 16 }}>
                <Text style={{ color: '#aaa' }}>场景:</Text>
                <Select value={scenario} onChange={setScenario} options={SCENARIOS} style={{ width: 160 }} />
                <Text style={{ color: '#aaa' }}>每序列点数:</Text>
                <InputNumber min={10} max={10000} value={pointsPerSeries} onChange={v => setPointsPerSeries(v || 60)} style={{ width: 120 }} />
                <Text style={{ color: '#aaa' }}>保留温/冷CF在RocksDB:</Text>
                <Switch checked={skipAutoDemote} onChange={setSkipAutoDemote} checkedChildren="手动降级" unCheckedChildren="自动降级" />
                <Button type="primary" onClick={handleGenerate} loading={generating}>
                  生成数据
                </Button>
              </Space>
              {genResult && (
                <div style={{ marginBottom: 16 }}>
                  <Tag color="green">生成完成</Tag>
                  <Text style={{ color: '#aaa', marginLeft: 8 }}>
                    总数据点: <Text strong style={{ color: '#fff' }}>{String(genResult.total_points)}</Text>
                  </Text>
                  <Text style={{ color: '#aaa', marginLeft: 16 }}>
                    耗时: <Text strong style={{ color: '#fff' }}>{Number(genResult.elapsed_secs).toFixed(2)}s</Text>
                  </Text>
                  <Text style={{ color: '#aaa', marginLeft: 16 }}>
                    写入速率: <Text strong style={{ color: '#fff' }}>{Number(genResult.rate_per_sec).toFixed(0)}/s</Text>
                  </Text>
                  {genResult.lifecycle && (
                    <div style={{ marginTop: 12, padding: 12, background: '#1a1a1a', borderRadius: 8 }}>
                      <Text style={{ color: '#aaa', fontSize: 13 }}>数据分层结果:</Text>
                      <div style={{ marginTop: 8, display: 'flex', gap: 24 }}>
                        <span>
                          <Tag color="#ff4d4f">🔥 热数据</Tag>
                          <Text style={{ color: '#fff' }}>{String(genResult.lifecycle.hot_days)}天 · RocksDB</Text>
                        </span>
                        <span>
                          <Tag color="#faad14">🌤️ 温数据</Tag>
                          <Text style={{ color: '#fff' }}>{String(genResult.lifecycle.warm_days)}天 · {genResult.lifecycle.auto_demote.skipped ? 'RocksDB (待降级)' : `${String(genResult.lifecycle.auto_demote.demoted_to_warm ?? 0)}个CF已降级`}</Text>
                        </span>
                        <span>
                          <Tag color="#1890ff">❄️ 冷数据</Tag>
                          <Text style={{ color: '#fff' }}>{String(genResult.lifecycle.cold_days)}天 · {genResult.lifecycle.auto_demote.skipped ? 'RocksDB (待降级)' : `${String(genResult.lifecycle.auto_demote.demoted_to_cold ?? 0)}个CF已降级`}</Text>
                        </span>
                      </div>
                      {!genResult.lifecycle.auto_demote.skipped && Array.isArray(genResult.lifecycle.auto_demote.warm_details) && genResult.lifecycle.auto_demote.warm_details.length > 0 && (
                        <div style={{ marginTop: 8 }}>
                          <Text style={{ color: '#faad14', fontSize: 12 }}>温数据降级详情:</Text>
                          {genResult.lifecycle.auto_demote.warm_details.slice(0, 5).map((d, i) => (
                            <div key={i} style={{ fontSize: 11, color: '#aaa', marginLeft: 8 }}>{d}</div>
                          ))}
                          {genResult.lifecycle.auto_demote.warm_details.length > 5 && (
                            <Text style={{ fontSize: 11, color: '#666' }}>...还有 {genResult.lifecycle.auto_demote.warm_details.length - 5} 项</Text>
                          )}
                        </div>
                      )}
                      {!genResult.lifecycle.auto_demote.skipped && Array.isArray(genResult.lifecycle.auto_demote.cold_details) && genResult.lifecycle.auto_demote.cold_details.length > 0 && (
                        <div style={{ marginTop: 8 }}>
                          <Text style={{ color: '#1890ff', fontSize: 12 }}>冷数据降级详情:</Text>
                          {genResult.lifecycle.auto_demote.cold_details.slice(0, 5).map((d, i) => (
                            <div key={i} style={{ fontSize: 11, color: '#aaa', marginLeft: 8 }}>{d}</div>
                          ))}
                          {genResult.lifecycle.auto_demote.cold_details.length > 5 && (
                            <Text style={{ fontSize: 11, color: '#666' }}>...还有 {genResult.lifecycle.auto_demote.cold_details.length - 5} 项</Text>
                          )}
                        </div>
                      )}
                      {genResult.lifecycle.auto_demote.skipped && (
                        <div style={{ marginTop: 8 }}>
                          <Tag color="orange">⚠️ 自动降级已跳过</Tag>
                          <Text style={{ color: '#aaa', fontSize: 12, marginLeft: 8 }}>请前往「数据生命周期」页面手动降级温/冷数据</Text>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}
              {businessMeasurements.length > 0 && (
                <Table
                  dataSource={businessMeasurements.map(m => ({ ...m, key: m.name }))}
                  columns={measurementColumns}
                  size="small"
                  pagination={false}
                  scroll={{ y: 300 }}
                />
              )}
            </Card>
          </Col>
        </Row>

        {health && (
          <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
            <Col span={24}>
              <Card title="健康状态">
                <Row gutter={[16, 16]}>
                  <Col span={6}><Statistic title="健康" value={health.healthy ? '正常' : '异常'} valueStyle={{ color: health.healthy ? '#52c41a' : '#ff4d4f' }} /></Col>
                  <Col span={6}><Statistic title="存储" value={health.storage_ok ? '正常' : '异常'} valueStyle={{ color: health.storage_ok ? '#52c41a' : '#ff4d4f' }} /></Col>
                  <Col span={6}>
                    <div style={{ textAlign: 'center' }}>
                      <Text style={{ color: '#aaa', fontSize: 14 }}>内存使用率</Text>
                      <Progress type="circle" percent={Math.round(health.memory_usage_pct)} size={60}
                        strokeColor={health.memory_usage_pct > 80 ? '#ff4d4f' : '#52c41a'} />
                    </div>
                  </Col>
                  <Col span={6}>
                    <div style={{ textAlign: 'center' }}>
                      <Text style={{ color: '#aaa', fontSize: 14 }}>磁盘使用率</Text>
                      <Progress type="circle" percent={Math.round(health.disk_usage_pct)} size={60}
                        strokeColor={health.disk_usage_pct > 80 ? '#ff4d4f' : '#52c41a'} />
                    </div>
                  </Col>
                </Row>
              </Card>
            </Col>
          </Row>
        )}
      </Spin>
    </div>
  );
};

export default Dashboard;
