import React, { useEffect, useState } from 'react';
import { Row, Col, Card, Statistic, Tag, Typography, Spin } from 'antd';
import {
  DatabaseOutlined,
  CloudServerOutlined,
  CheckCircleOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { api, useWebSocket, type HealthStatus, type ServiceInfo } from '../api';

const { Title } = Typography;

const Dashboard: React.FC = () => {
  const [services, setServices] = useState<ServiceInfo[]>([]);
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const { lastMessage } = useWebSocket();

  useEffect(() => {
    const fetchData = async () => {
      try {
        const svcList = await api.services.list();
        setServices(svcList);
        if (svcList.length > 0) {
          const h = await api.metrics.health(svcList[0].name);
          setHealth(h);
        }
      } catch (e) {
        console.error(e);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  const runningCount = services.filter(s => s.status === 'Running').length;
  const errorCount = services.filter(s => s.status === 'Error').length;

  return (
    <div>
      <Title level={3} style={{ color: '#fff' }}>仪表盘</Title>
      <Spin spinning={loading}>
        <Row gutter={[16, 16]}>
          <Col span={6}>
            <Card>
              <Statistic title="服务总数" value={services.length} prefix={<CloudServerOutlined />} />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic title="运行中" value={runningCount} prefix={<CheckCircleOutlined />}
                valueStyle={{ color: '#52c41a' }} />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic title="异常" value={errorCount} prefix={<WarningOutlined />}
                valueStyle={{ color: errorCount > 0 ? '#ff4d4f' : '#52c41a' }} />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic title="数据点总数" value={health?.total_data_points ?? 0}
                prefix={<DatabaseOutlined />} />
            </Card>
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
                  <Col span={12}>
                    <Statistic title="写入速率" value={lastMessage.write_rate.toFixed(1)} suffix="/s" />
                  </Col>
                  <Col span={12}>
                    <Statistic title="读取速率" value={lastMessage.read_rate.toFixed(1)} suffix="/s" />
                  </Col>
                  <Col span={12}>
                    <Statistic title="内存使用" value={(lastMessage.memory_bytes / 1024 / 1024).toFixed(1)} suffix="MB" />
                  </Col>
                  <Col span={12}>
                    <Statistic title="磁盘使用" value={(lastMessage.disk_bytes / 1024 / 1024).toFixed(1)} suffix="MB" />
                  </Col>
                  <Col span={12}>
                    <Statistic title="L0文件数" value={lastMessage.l0_file_count} />
                  </Col>
                  <Col span={12}>
                    <Statistic title="压缩计数" value={lastMessage.compaction_count} />
                  </Col>
                </Row>
              ) : (
                <span style={{ color: '#999' }}>等待数据...</span>
              )}
            </Card>
          </Col>
        </Row>

        {health && (
          <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
            <Col span={24}>
              <Card title="健康状态">
                <Row gutter={[16, 16]}>
                  <Col span={6}>
                    <Statistic title="健康" value={health.healthy ? '正常' : '异常'}
                      valueStyle={{ color: health.healthy ? '#52c41a' : '#ff4d4f' }} />
                  </Col>
                  <Col span={6}>
                    <Statistic title="存储" value={health.storage_ok ? '正常' : '异常'}
                      valueStyle={{ color: health.storage_ok ? '#52c41a' : '#ff4d4f' }} />
                  </Col>
                  <Col span={6}>
                    <Statistic title="内存使用率" value={health.memory_usage_pct.toFixed(1)} suffix="%" />
                  </Col>
                  <Col span={6}>
                    <Statistic title="磁盘使用率" value={health.disk_usage_pct.toFixed(1)} suffix="%" />
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
