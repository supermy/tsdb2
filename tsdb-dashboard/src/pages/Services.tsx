import React, { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, InputNumber, Select, Tag, Space, Popconfirm, message, Typography } from 'antd';
import { PlayCircleOutlined, StopOutlined, ReloadOutlined, PlusOutlined, DeleteOutlined, FileTextOutlined } from '@ant-design/icons';
import { api, type ServiceInfo } from '../api';

const { Title } = Typography;

const Services: React.FC = () => {
  const [services, setServices] = useState<ServiceInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [createOpen, setCreateOpen] = useState(false);
  const [logsOpen, setLogsOpen] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const [logsName, setLogsName] = useState('');
  const [form] = Form.useForm();

  const fetchServices = async () => {
    setLoading(true);
    try {
      const list = await api.services.list();
      setServices(list);
    } catch (e: unknown) {
      message.error(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchServices(); }, []);

  const handleStart = async (name: string) => {
    try {
      await api.services.start(name);
      message.success(`${name} 已启动`);
      fetchServices();
    } catch (e: unknown) { message.error(e instanceof Error ? e.message : String(e)); }
  };

  const handleStop = async (name: string) => {
    try {
      await api.services.stop(name);
      message.success(`${name} 已停止`);
      fetchServices();
    } catch (e: unknown) { message.error(e instanceof Error ? e.message : String(e)); }
  };

  const handleRestart = async (name: string) => {
    try {
      await api.services.restart(name);
      message.success(`${name} 已重启`);
      fetchServices();
    } catch (e: unknown) { message.error(e instanceof Error ? e.message : String(e)); }
  };

  const handleDelete = async (name: string) => {
    try {
      await api.services.delete(name);
      message.success(`${name} 已删除`);
      fetchServices();
    } catch (e: unknown) { message.error(e instanceof Error ? e.message : String(e)); }
  };

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      await api.services.create(values);
      message.success('服务已创建');
      setCreateOpen(false);
      form.resetFields();
      fetchServices();
    } catch (e: unknown) { message.error(e instanceof Error ? e.message : String(e)); }
  };

  const handleLogs = async (name: string) => {
    try {
      const data = await api.services.logs(name, 200);
      setLogs(data);
      setLogsName(name);
      setLogsOpen(true);
    } catch (e: unknown) { message.error(e instanceof Error ? e.message : String(e)); }
  };

  const formatUptime = (secs: number | null) => {
    if (!secs) return '-';
    const h = Math.floor(secs / 3600);
    const m = Math.floor((secs % 3600) / 60);
    const s = secs % 60;
    return `${h}h ${m}m ${s}s`;
  };

  const columns = [
    { title: '名称', dataIndex: 'name', key: 'name' },
    { title: '状态', dataIndex: 'status', key: 'status', render: (s: string) => (
      <Tag color={s === 'Running' ? 'green' : s === 'Error' ? 'red' : 'default'}>{s}</Tag>
    )},
    { title: '端口', dataIndex: 'port', key: 'port' },
    { title: '引擎', dataIndex: 'engine', key: 'engine' },
    { title: '配置', dataIndex: 'config', key: 'config' },
    { title: 'PID', dataIndex: 'pid', key: 'pid', render: (p: number | null) => p ?? '-' },
    { title: '运行时间', dataIndex: 'uptime_secs', key: 'uptime', render: formatUptime },
    { title: '操作', key: 'actions', render: (_: unknown, record: ServiceInfo) => (
      <Space>
        {record.status !== 'Running' && (
          <Button type="primary" size="small" icon={<PlayCircleOutlined />} onClick={() => handleStart(record.name)}>启动</Button>
        )}
        {record.status === 'Running' && (
          <Button danger size="small" icon={<StopOutlined />} onClick={() => handleStop(record.name)}>停止</Button>
        )}
        {record.status === 'Running' && (
          <Button size="small" icon={<ReloadOutlined />} onClick={() => handleRestart(record.name)}>重启</Button>
        )}
        <Button size="small" icon={<FileTextOutlined />} onClick={() => handleLogs(record.name)}>日志</Button>
        <Popconfirm title={`确定删除 ${record.name}?`} onConfirm={() => handleDelete(record.name)}>
          <Button size="small" danger icon={<DeleteOutlined />}>删除</Button>
        </Popconfirm>
      </Space>
    )},
  ];

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={3} style={{ color: '#fff', margin: 0 }}>服务管理</Title>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateOpen(true)}>创建服务</Button>
      </div>

      <Table dataSource={services} columns={columns} rowKey="name" loading={loading} />

      <Modal title="创建服务" open={createOpen} onOk={handleCreate} onCancel={() => setCreateOpen(false)}>
        <Form form={form} layout="vertical">
          <Form.Item name="name" label="服务名称" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="port" label="端口" rules={[{ required: true }]}>
            <InputNumber min={1024} max={65535} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="engine" label="存储引擎" rules={[{ required: true }]}>
            <Select options={[{ value: 'rocksdb', label: 'RocksDB' }, { value: 'arrow', label: 'Arrow' }]} />
          </Form.Item>
          <Form.Item name="config" label="配置名称" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="data_dir" label="数据目录" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
        </Form>
      </Modal>

      <Modal title={`${logsName} 日志`} open={logsOpen} onCancel={() => setLogsOpen(false)} footer={null} width={800}>
        <pre style={{ maxHeight: 500, overflow: 'auto', background: '#1a1a1a', padding: 12, borderRadius: 4, color: '#ccc', fontSize: 12 }}>
          {logs.length > 0 ? logs.join('\n') : '暂无日志'}
        </pre>
      </Modal>
    </div>
  );
};

export default Services;
