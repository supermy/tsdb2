import React, { useState } from 'react';
import { Card, Form, Input, InputNumber, Button, Tabs, Typography, Descriptions, message, Row, Col } from 'antd';
import { PlayCircleOutlined } from '@ant-design/icons';
import { api, type BenchResult } from '../api';

const { Title } = Typography;
const { TextArea } = Input;

const Testing: React.FC = () => {
  const [sqlResult, setSqlResult] = useState<BenchResult | null>(null);
  const [writeResult, setWriteResult] = useState<BenchResult | null>(null);
  const [readResult, setReadResult] = useState<BenchResult | null>(null);
  const [sqlLoading, setSqlLoading] = useState(false);
  const [writeLoading, setWriteLoading] = useState(false);
  const [readLoading, setReadLoading] = useState(false);

  const onSql = async (values: Record<string, unknown>) => {
    setSqlLoading(true);
    try {
      const result = await api.test.sql(String(values.service), String(values.sql));
      setSqlResult(result);
      message.success('SQL 执行完成');
    } catch (e: unknown) { message.error(e instanceof Error ? e.message : String(e)); }
    finally { setSqlLoading(false); }
  };

  const onWriteBench = async (values: Record<string, unknown>) => {
    setWriteLoading(true);
    try {
      const result = await api.test.writeBench(String(values.service), String(values.measurement), Number(values.total_points), Number(values.workers), Number(values.batch_size));
      setWriteResult(result);
      message.success('写入基准测试完成');
    } catch (e: unknown) { message.error(e instanceof Error ? e.message : String(e)); }
    finally { setWriteLoading(false); }
  };

  const onReadBench = async (values: Record<string, unknown>) => {
    setReadLoading(true);
    try {
      const result = await api.test.readBench(String(values.service), String(values.measurement), Number(values.queries), Number(values.workers));
      setReadResult(result);
      message.success('读取基准测试完成');
    } catch (e: unknown) { message.error(e instanceof Error ? e.message : String(e)); }
    finally { setReadLoading(false); }
  };

  const renderBenchResult = (result: BenchResult | null) => {
    if (!result) return null;
    return (
      <Card style={{ marginTop: 16 }} size="small">
        <Descriptions column={3} size="small">
          <Descriptions.Item label="总数据点">{result.total_points.toLocaleString()}</Descriptions.Item>
          <Descriptions.Item label="耗时">{result.elapsed_secs.toFixed(3)}s</Descriptions.Item>
          <Descriptions.Item label="速率">{result.rate_per_sec.toFixed(1)}/s</Descriptions.Item>
          <Descriptions.Item label="平均延迟">{result.avg_latency_us.toFixed(1)}μs</Descriptions.Item>
          <Descriptions.Item label="P99延迟">{result.p99_latency_us.toFixed(1)}μs</Descriptions.Item>
        </Descriptions>
      </Card>
    );
  };

  const tabItems = [
    {
      key: 'sql',
      label: 'SQL 查询',
      children: (
        <Form layout="vertical" onFinish={onSql} initialValues={{ service: 'default', sql: 'SELECT 1' }}>
          <Form.Item name="service" label="服务" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="sql" label="SQL" rules={[{ required: true }]}>
            <TextArea rows={6} placeholder="SELECT * FROM cpu_usage WHERE timestamp > now() - interval '1 hour'" />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit" icon={<PlayCircleOutlined />} loading={sqlLoading}>执行</Button>
          </Form.Item>
          {renderBenchResult(sqlResult)}
        </Form>
      ),
    },
    {
      key: 'write',
      label: '写入基准测试',
      children: (
        <Form layout="vertical" onFinish={onWriteBench} initialValues={{ service: 'default', measurement: 'bench_test', total_points: 10000, workers: 4, batch_size: 100 }}>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="service" label="服务" rules={[{ required: true }]}>
                <Input />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="measurement" label="Measurement" rules={[{ required: true }]}>
                <Input />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={16}>
            <Col span={8}>
              <Form.Item name="total_points" label="总数据点" rules={[{ required: true }]}>
                <InputNumber min={1} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="workers" label="并发数" rules={[{ required: true }]}>
                <InputNumber min={1} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="batch_size" label="批大小" rules={[{ required: true }]}>
                <InputNumber min={1} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>
          <Form.Item>
            <Button type="primary" htmlType="submit" icon={<PlayCircleOutlined />} loading={writeLoading}>开始测试</Button>
          </Form.Item>
          {renderBenchResult(writeResult)}
        </Form>
      ),
    },
    {
      key: 'read',
      label: '读取基准测试',
      children: (
        <Form layout="vertical" onFinish={onReadBench} initialValues={{ service: 'default', measurement: 'bench_test', queries: 100, workers: 1 }}>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="service" label="服务" rules={[{ required: true }]}>
                <Input />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="measurement" label="Measurement" rules={[{ required: true }]}>
                <Input />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="queries" label="查询次数" rules={[{ required: true }]}>
                <InputNumber min={1} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="workers" label="并发数" rules={[{ required: true }]}>
                <InputNumber min={1} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>
          <Form.Item>
            <Button type="primary" htmlType="submit" icon={<PlayCircleOutlined />} loading={readLoading}>开始测试</Button>
          </Form.Item>
          {renderBenchResult(readResult)}
        </Form>
      ),
    },
  ];

  return (
    <div>
      <Title level={3} style={{ color: '#fff' }}>功能测试</Title>
      <Card>
        <Tabs items={tabItems} />
      </Card>
    </div>
  );
};

export default Testing;
