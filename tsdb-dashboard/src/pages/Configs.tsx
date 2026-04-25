import React, { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, Select, Space, Popconfirm, message, Typography } from 'antd';
import { PlusOutlined, DeleteOutlined, SwapOutlined, CheckOutlined } from '@ant-design/icons';
import { api, type ConfigProfile, type ConfigDiff } from '../api';

const { Title, Text } = Typography;
const { TextArea } = Input;

const Configs: React.FC = () => {
  const [profiles, setProfiles] = useState<ConfigProfile[]>([]);
  const [loading, setLoading] = useState(false);
  const [saveOpen, setSaveOpen] = useState(false);
  const [editOpen, setEditOpen] = useState(false);
  const [compareOpen, setCompareOpen] = useState(false);
  const [editingProfile, setEditingProfile] = useState<ConfigProfile | null>(null);
  const [diffs, setDiffs] = useState<ConfigDiff[]>([]);
  const [form] = Form.useForm();
  const [compareForm] = Form.useForm();

  const fetchProfiles = async () => {
    setLoading(true);
    try {
      const list = await api.configs.list();
      setProfiles(list);
    } catch (e: any) { message.error(e.message); }
    finally { setLoading(false); }
  };

  useEffect(() => { fetchProfiles(); }, []);

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      await api.configs.save(values.name, values.content);
      message.success('配置已保存');
      setSaveOpen(false);
      form.resetFields();
      fetchProfiles();
    } catch (e: any) { message.error(e.message); }
  };

  const handleEdit = (profile: ConfigProfile) => {
    setEditingProfile(profile);
    form.setFieldsValue({ name: profile.name, content: profile.content });
    setEditOpen(true);
  };

  const handleEditSave = async () => {
    try {
      const values = await form.validateFields();
      await api.configs.save(editingProfile!.name, values.content);
      message.success('配置已更新');
      setEditOpen(false);
      fetchProfiles();
    } catch (e: any) { message.error(e.message); }
  };

  const handleDelete = async (name: string) => {
    try {
      await api.configs.delete(name);
      message.success('配置已删除');
      fetchProfiles();
    } catch (e: any) { message.error(e.message); }
  };

  const handleCompare = async () => {
    try {
      const values = await compareForm.validateFields();
      const result = await api.configs.compare(values.profile_a, values.profile_b);
      setDiffs(result);
    } catch (e: any) { message.error(e.message); }
  };

  const columns = [
    { title: '名称', dataIndex: 'name', key: 'name' },
    { title: '描述', dataIndex: 'description', key: 'description' },
    { title: '内容预览', dataIndex: 'content', key: 'content', render: (c: string) => (
      <Text ellipsis style={{ maxWidth: 300 }}>{c}</Text>
    )},
    { title: '操作', key: 'actions', render: (_: any, record: ConfigProfile) => (
      <Space>
        <Button size="small" onClick={() => handleEdit(record)}>编辑</Button>
        <Popconfirm title={`确定删除 ${record.name}?`} onConfirm={() => handleDelete(record.name)}>
          <Button size="small" danger icon={<DeleteOutlined />}>删除</Button>
        </Popconfirm>
      </Space>
    )},
  ];

  const profileOptions = profiles.map(p => ({ value: p.name, label: p.name }));

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={3} style={{ color: '#fff', margin: 0 }}>配置管理</Title>
        <Space>
          <Button icon={<SwapOutlined />} onClick={() => setCompareOpen(true)}>对比配置</Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => setSaveOpen(true)}>新建配置</Button>
        </Space>
      </div>

      <Table dataSource={profiles} columns={columns} rowKey="name" loading={loading} />

      <Modal title="新建配置" open={saveOpen} onOk={handleSave} onCancel={() => setSaveOpen(false)} width={600}>
        <Form form={form} layout="vertical">
          <Form.Item name="name" label="配置名称" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="content" label="配置内容 (INI)" rules={[{ required: true }]}>
            <TextArea rows={12} placeholder="[rocksdb]&#10;max_open_files=1024&#10;write_buffer_size=67108864" />
          </Form.Item>
        </Form>
      </Modal>

      <Modal title={`编辑: ${editingProfile?.name}`} open={editOpen} onOk={handleEditSave} onCancel={() => setEditOpen(false)} width={600}>
        <Form form={form} layout="vertical">
          <Form.Item name="content" label="配置内容" rules={[{ required: true }]}>
            <TextArea rows={12} />
          </Form.Item>
        </Form>
      </Modal>

      <Modal title="对比配置" open={compareOpen} onCancel={() => setCompareOpen(false)} footer={null} width={800}>
        <Form form={compareForm} layout="inline" style={{ marginBottom: 16 }}>
          <Form.Item name="profile_a" rules={[{ required: true }]}>
            <Select style={{ width: 200 }} placeholder="配置 A" options={profileOptions} />
          </Form.Item>
          <Form.Item name="profile_b" rules={[{ required: true }]}>
            <Select style={{ width: 200 }} placeholder="配置 B" options={profileOptions} />
          </Form.Item>
          <Form.Item>
            <Button type="primary" icon={<CheckOutlined />} onClick={handleCompare}>对比</Button>
          </Form.Item>
        </Form>
        {diffs.length > 0 && (
          <Table dataSource={diffs} columns={[
            { title: '键', dataIndex: 'key', key: 'key' },
            { title: '配置 A', dataIndex: 'value_a', key: 'value_a' },
            { title: '配置 B', dataIndex: 'value_b', key: 'value_b' },
          ]} rowKey="key" size="small" pagination={false} />
        )}
      </Modal>
    </div>
  );
};

export default Configs;
