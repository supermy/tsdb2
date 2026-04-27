import React, { useEffect, useState, useCallback, useMemo } from 'react';
import {
  Card, Table, Button, Modal, Form, Input, Select, Space, Tag, message,
  Popconfirm, Descriptions, Tabs, Typography, Tooltip, InputNumber, Spin,
} from 'antd';
import {
  PlusOutlined, DeleteOutlined, ReloadOutlined, EyeOutlined,
  HistoryOutlined, CompressOutlined, RollbackOutlined, FieldTimeOutlined,
} from '@ant-design/icons';
import { api, type IcebergTableInfo, type IcebergTableDetail, type IcebergSnapshotInfo, type IcebergScanResult } from '../api';
import { fmtBytes } from '../utils';

const { Text, Title } = Typography;

const fmtTime = (ms: number) => new Date(ms).toLocaleString();

const IcebergManager: React.FC = () => {
  const [tables, setTables] = useState<IcebergTableInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [detail, setDetail] = useState<IcebergTableDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [appendModalOpen, setAppendModalOpen] = useState(false);
  const [scanResult, setScanResult] = useState<IcebergScanResult | null>(null);
  const [scanLoading, setScanLoading] = useState(false);
  const [scanLimit, setScanLimit] = useState(100);
  const [form] = Form.useForm();
  const [appendForm] = Form.useForm();

  const fetchTables = useCallback(async () => {
    setLoading(true);
    try {
      const data = await api.iceberg.listTables();
      setTables(data);
    } catch (e: unknown) {
      message.error(`加载表列表失败: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchTables(); }, [fetchTables]);

  const fetchDetail = useCallback(async (name: string) => {
    setDetailLoading(true);
    try {
      const data = await api.iceberg.tableDetail(name);
      setDetail(data);
    } catch (e: unknown) {
      message.error(`加载表详情失败: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setDetailLoading(false);
    }
  }, []);

  useEffect(() => {
    if (selectedTable) {
      fetchDetail(selectedTable);
      setScanResult(null);
    } else {
      setDetail(null);
    }
  }, [selectedTable, fetchDetail]);

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      const schema = {
        fields: (values.fields || []).map((f: { name: string; field_type: string; required: boolean }) => ({
          name: f.name,
          field_type: f.field_type,
          required: f.required || false,
        })),
      };
      await api.iceberg.createTable(values.name, schema, values.partition_type || 'none');
      message.success(`表 ${values.name} 创建成功`);
      setCreateModalOpen(false);
      form.resetFields();
      fetchTables();
    } catch (e: unknown) {
      message.error(`创建失败: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const handleDrop = async (name: string) => {
    try {
      await api.iceberg.dropTable(name);
      message.success(`表 ${name} 已删除`);
      if (selectedTable === name) {
        setSelectedTable(null);
        setDetail(null);
      }
      fetchTables();
    } catch (e: unknown) {
      message.error(`删除失败: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const handleAppend = async () => {
    if (!selectedTable) return;
    try {
      const values = await appendForm.validateFields();
      const datapoints = JSON.parse(values.datapoints);
      if (!Array.isArray(datapoints)) {
        message.error('数据必须是 JSON 数组格式');
        return;
      }
      await api.iceberg.append(selectedTable, datapoints);
      message.success('数据追加成功');
      setAppendModalOpen(false);
      appendForm.resetFields();
      fetchDetail(selectedTable);
    } catch (e: unknown) {
      message.error(`追加数据失败: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const handleScan = async () => {
    if (!selectedTable) return;
    setScanLoading(true);
    try {
      const result = await api.iceberg.scan(selectedTable, scanLimit);
      setScanResult(result);
    } catch (e: unknown) {
      message.error(`扫描失败: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setScanLoading(false);
    }
  };

  const handleRollback = async (snapshotId: number) => {
    if (!selectedTable) return;
    try {
      await api.iceberg.rollback(selectedTable, snapshotId);
      message.success(`已回滚到快照 ${snapshotId}`);
      fetchDetail(selectedTable);
    } catch (e: unknown) {
      message.error(`回滚失败: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const handleCompact = async () => {
    if (!selectedTable) return;
    try {
      await api.iceberg.compact(selectedTable);
      message.success('合并完成');
      fetchDetail(selectedTable);
    } catch (e: unknown) {
      message.error(`合并失败: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const handleExpire = async (keepDays: number) => {
    if (!selectedTable) return;
    try {
      await api.iceberg.expire(selectedTable, keepDays);
      message.success(`已过期 ${keepDays} 天前的快照`);
      fetchDetail(selectedTable);
    } catch (e: unknown) {
      message.error(`过期失败: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const tableColumns = [
    {
      title: '表名', dataIndex: 'name', key: 'name',
      render: (v: string) => <Button type="link" size="small" onClick={() => setSelectedTable(v)}>{v}</Button>,
    },
    { title: '版本', dataIndex: 'format_version', key: 'ver', width: 60 },
    { title: '快照数', dataIndex: 'snapshot_count', key: 'snaps', width: 70 },
    { title: '字段数', dataIndex: 'schema_fields', key: 'fields', width: 70 },
    { title: '总记录', dataIndex: 'total_records', key: 'records', width: 100,
      render: (v: number) => v.toLocaleString(),
    },
    { title: '数据文件', dataIndex: 'total_data_files', key: 'files', width: 80 },
    { title: '分区', dataIndex: 'partition_spec', key: 'part', width: 150, ellipsis: true,
      render: (v: string) => v === '[]' ? <Tag>无分区</Tag> : <Text style={{ fontSize: 11 }}>{v}</Text>,
    },
    { title: '更新时间', dataIndex: 'last_updated_ms', key: 'updated', width: 160,
      render: (v: number) => v > 0 ? fmtTime(v) : '-',
    },
    {
      title: '操作', key: 'action', width: 80,
      render: (_: unknown, record: IcebergTableInfo) => (
        <Popconfirm title={`确定删除表 ${record.name}？`} onConfirm={() => handleDrop(record.name)}>
          <Button size="small" danger icon={<DeleteOutlined />}>删除</Button>
        </Popconfirm>
      ),
    },
  ];

  const snapshotColumns = [
    { title: '快照ID', dataIndex: 'snapshot_id', key: 'id', width: 120,
      render: (v: number) => <Text copyable style={{ fontSize: 11, fontFamily: 'monospace' }}>{v}</Text>,
    },
    { title: '操作', dataIndex: 'operation', key: 'op', width: 80,
      render: (v: string) => {
        const color = v === 'append' ? 'green' : v === 'replace' ? 'orange' : 'blue';
        return <Tag color={color}>{v}</Tag>;
      },
    },
    { title: '序列号', dataIndex: 'sequence_number', key: 'seq', width: 70 },
    { title: '时间', dataIndex: 'timestamp_ms', key: 'ts', width: 160,
      render: (v: number) => fmtTime(v),
    },
    { title: '新增文件', dataIndex: 'added_data_files', key: 'adf', width: 80 },
    { title: '新增记录', dataIndex: 'added_records', key: 'ar', width: 90,
      render: (v: number) => v.toLocaleString(),
    },
    { title: '总文件', dataIndex: 'total_data_files', key: 'tdf', width: 70 },
    { title: '总记录', dataIndex: 'total_records', key: 'tr', width: 90,
      render: (v: number) => v.toLocaleString(),
    },
    {
      title: '操作', key: 'action', width: 80,
      render: (_: unknown, record: IcebergSnapshotInfo) => (
        <Tooltip title="回滚到此快照">
          <Button size="small" icon={<RollbackOutlined />}
            onClick={() => handleRollback(record.snapshot_id)}>回滚</Button>
        </Tooltip>
      ),
    },
  ];

  const dataFileColumns = [
    { title: '文件路径', dataIndex: 'file_path', key: 'path', ellipsis: true,
      render: (v: string) => <Text copyable style={{ fontSize: 11, fontFamily: 'monospace' }}>{v}</Text>,
    },
    { title: '格式', dataIndex: 'file_format', key: 'fmt', width: 70 },
    { title: '记录数', dataIndex: 'record_count', key: 'rows', width: 80,
      render: (v: number) => v.toLocaleString(),
    },
    { title: '大小', dataIndex: 'file_size_in_bytes', key: 'size', width: 90,
      render: (v: number) => fmtBytes(v),
    },
  ];

  const schemaColumns = [
    { title: 'ID', dataIndex: 'id', key: 'id', width: 50 },
    { title: '名称', dataIndex: 'name', key: 'name', width: 150 },
    { title: '类型', dataIndex: 'field_type', key: 'type', width: 150,
      render: (v: string) => <Tag>{v}</Tag>,
    },
    { title: '必填', dataIndex: 'required', key: 'req', width: 60,
      render: (v: boolean) => v ? <Tag color="red">是</Tag> : <Tag>否</Tag>,
    },
    { title: '说明', dataIndex: 'doc', key: 'doc', ellipsis: true,
      render: (v: string | null) => v || '-',
    },
  ];

  const sampleAppendData = useMemo(() => JSON.stringify([
    { measurement: "cpu", timestamp: 1700000000000000, tags: { host: "server01" }, fields: { usage: 0.85, idle: 0.15 } },
    { measurement: "cpu", timestamp: 1700000001000000, tags: { host: "server02" }, fields: { usage: 0.72, idle: 0.28 } },
  ], null, 2), []);

  return (
    <div>
      <Title level={4} style={{ color: '#fff', marginBottom: 16 }}>
        <span style={{ marginRight: 8 }}>🧊</span>Iceberg 表管理
      </Title>

      <Card size="small" style={{ background: '#1a1a1a', borderColor: '#333', marginBottom: 16 }}>
        <Space>
          <Button icon={<ReloadOutlined />} onClick={fetchTables} loading={loading}>刷新</Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateModalOpen(true)}>创建表</Button>
          {selectedTable && (
            <>
              <Button icon={<PlusOutlined />} onClick={() => setAppendModalOpen(true)}>追加数据</Button>
              <Button icon={<EyeOutlined />} onClick={handleScan} loading={scanLoading}>扫描数据</Button>
              <Button icon={<CompressOutlined />} onClick={handleCompact}>合并小文件</Button>
              <Popconfirm title="过期多少天前的快照？" onConfirm={() => handleExpire(7)}
                description="将保留最近7天的快照">
                <Button icon={<FieldTimeOutlined />}>过期快照</Button>
              </Popconfirm>
              <Button onClick={() => { setSelectedTable(null); setDetail(null); setScanResult(null); }}>返回列表</Button>
            </>
          )}
        </Space>
      </Card>

      {!selectedTable ? (
        <Card size="small" style={{ background: '#1a1a1a', borderColor: '#333' }}>
          <Table dataSource={tables} columns={tableColumns} rowKey="name" loading={loading}
            size="small" pagination={false} />
        </Card>
      ) : (
        <Spin spinning={detailLoading}>
        <div>
          {detail && (
            <>
              <Card size="small" title={`表: ${detail.name}`} style={{ background: '#1a1a1a', borderColor: '#333', marginBottom: 12 }}>
                <Descriptions size="small" column={3} labelStyle={{ color: '#aaa' }} contentStyle={{ color: '#fff' }}>
                  <Descriptions.Item label="UUID"><Text copyable style={{ fontSize: 11 }}>{detail.table_uuid}</Text></Descriptions.Item>
                  <Descriptions.Item label="版本">V{detail.format_version}</Descriptions.Item>
                  <Descriptions.Item label="当前快照">{detail.current_snapshot_id >= 0 ? detail.current_snapshot_id : '无'}</Descriptions.Item>
                  <Descriptions.Item label="位置"><Text copyable style={{ fontSize: 11 }}>{detail.location}</Text></Descriptions.Item>
                  <Descriptions.Item label="序列号">{detail.last_sequence_number}</Descriptions.Item>
                  <Descriptions.Item label="更新时间">{detail.last_updated_ms > 0 ? fmtTime(detail.last_updated_ms) : '-'}</Descriptions.Item>
                </Descriptions>
              </Card>

              <Tabs defaultActiveKey="snapshots" items={[
                {
                  key: 'snapshots',
                  label: <span><HistoryOutlined /> 快照历史</span>,
                  children: (
                    <Table dataSource={detail.snapshots} columns={snapshotColumns} rowKey="snapshot_id"
                      size="small" pagination={{ pageSize: 10 }} />
                  ),
                },
                {
                  key: 'schema',
                  label: <span>📐 Schema</span>,
                  children: (
                    <div>
                      {detail.schema_history.map((s, idx) => (
                        <Card key={s.schema_id} size="small" title={`Schema v${s.schema_id}${idx === detail.schema_history.length - 1 ? ' (当前)' : ''}`}
                          style={{ background: '#1a1a1a', borderColor: '#333', marginBottom: 8 }}>
                          <Table dataSource={s.fields} columns={schemaColumns} rowKey="id" size="small" pagination={false} />
                        </Card>
                      ))}
                    </div>
                  ),
                },
                {
                  key: 'partition',
                  label: <span>🗂️ 分区规范</span>,
                  children: (
                    <Table dataSource={detail.partition_specs} columns={[
                      { title: '规范ID', dataIndex: 'spec_id', key: 'id', width: 80 },
                      { title: '字段', key: 'fields', render: (_: unknown, record: typeof detail.partition_specs[0]) => (
                        <Space>
                          {record.fields.map((f, i) => (
                            <Tag key={i}>source={f.source_id} field={f.field_id} {f.transform}</Tag>
                          ))}
                        </Space>
                      )},
                    ]} rowKey="spec_id" size="small" pagination={false} />
                  ),
                },
                {
                  key: 'files',
                  label: <span>📁 数据文件</span>,
                  children: (
                    <Table dataSource={detail.data_files} columns={dataFileColumns} rowKey="file_path"
                      size="small" pagination={{ pageSize: 10 }} />
                  ),
                },
                {
                  key: 'scan',
                  label: <span><EyeOutlined /> 数据扫描</span>,
                  children: scanResult ? (
                    <div>
                      <Space style={{ marginBottom: 8 }}>
                        <Text style={{ color: '#aaa' }}>共 {scanResult.total_rows} 行, {scanResult.columns.length} 列</Text>
                        <Button size="small" onClick={handleScan} loading={scanLoading}>重新扫描</Button>
                        <Text style={{ color: '#aaa' }}>限制:</Text>
                        <InputNumber size="small" min={1} max={10000} value={scanLimit}
                          onChange={v => setScanLimit(v || 100)} />
                      </Space>
                      <Table dataSource={scanResult.rows.map((r, i) => ({ ...r, _key: i }))}
                        columns={scanResult.columns.map(col => ({
                          title: col, dataIndex: col, key: col, ellipsis: true, width: 150,
                          render: (v: unknown) => {
                            if (v === null || v === undefined) return <Text type="secondary">null</Text>;
                            if (typeof v === 'object') return <Text style={{ fontSize: 11 }}>{JSON.stringify(v)}</Text>;
                            return <Text style={{ fontSize: 11 }}>{String(v)}</Text>;
                          },
                        }))}
                        rowKey="_key" size="small" pagination={{ pageSize: 20 }} scroll={{ x: 'max-content' }} />
                    </div>
                  ) : (
                    <div style={{ textAlign: 'center', padding: 40 }}>
                      <Button type="primary" onClick={handleScan} loading={scanLoading}>
                        扫描表数据
                      </Button>
                      <div style={{ marginTop: 8 }}>
                        <Text style={{ color: '#aaa' }}>限制行数: </Text>
                        <InputNumber min={1} max={10000} value={scanLimit}
                          onChange={v => setScanLimit(v || 100)} />
                      </div>
                    </div>
                  ),
                },
              ]} />
            </>
          )}
        </div>
        </Spin>
      )}

      <Modal title="创建 Iceberg 表" open={createModalOpen} onOk={handleCreate} onCancel={() => setCreateModalOpen(false)} width={600}>
        <Form form={form} layout="vertical">
          <Form.Item name="name" label="表名" rules={[{ required: true, message: '请输入表名' }]}>
            <Input placeholder="例如: cpu_metrics" />
          </Form.Item>
          <Form.Item name="partition_type" label="分区方式" initialValue="none">
            <Select options={[
              { value: 'none', label: '无分区' },
              { value: 'day', label: '按天分区 (Day)' },
              { value: 'hour', label: '按小时分区 (Hour)' },
              { value: 'identity', label: '按标签分区 (Identity)' },
            ]} />
          </Form.Item>
          <Form.Item label="字段定义">
            <Form.List name="fields" initialValue={[
              { name: 'tag_host', field_type: 'string', required: false },
              { name: 'value', field_type: 'double', required: false },
            ]}>
              {(fields, { add, remove }) => (
                <>
                  {fields.map(({ key, name, ...restField }) => (
                    <Space key={key} style={{ display: 'flex', marginBottom: 8 }} align="baseline">
                      <Form.Item {...restField} name={[name, 'name']} rules={[{ required: true }]}>
                        <Input placeholder="字段名" style={{ width: 140 }} />
                      </Form.Item>
                      <Form.Item {...restField} name={[name, 'field_type']} initialValue="double">
                        <Select style={{ width: 120 }} options={[
                          { value: 'double', label: 'Double' },
                          { value: 'long', label: 'Long' },
                          { value: 'string', label: 'String' },
                          { value: 'boolean', label: 'Boolean' },
                        ]} />
                      </Form.Item>
                      <Form.Item {...restField} name={[name, 'required']} valuePropName="checked" initialValue={false}>
                        <Select style={{ width: 80 }} options={[
                          { value: false, label: '可选' },
                          { value: true, label: '必填' },
                        ]} />
                      </Form.Item>
                      <Button type="text" danger onClick={() => remove(name)}>删除</Button>
                    </Space>
                  ))}
                  <Button type="dashed" onClick={() => add()} block icon={<PlusOutlined />}>添加字段</Button>
                </>
              )}
            </Form.List>
          </Form.Item>
        </Form>
      </Modal>

      <Modal title={`追加数据到 ${selectedTable}`} open={appendModalOpen} onOk={handleAppend}
        onCancel={() => setAppendModalOpen(false)} width={700}>
        <Form form={appendForm} layout="vertical">
          <Form.Item name="datapoints" label="数据 (JSON)" rules={[{ required: true, message: '请输入数据' }]}
            initialValue={sampleAppendData}>
            <Input.TextArea rows={12} style={{ fontFamily: 'monospace', fontSize: 12 }} />
          </Form.Item>
          <div style={{ color: '#888', fontSize: 12 }}>
            格式: {'[{"measurement": "表名", "timestamp": 微秒时间戳, "tags": {"key": "value"}, "fields": {"key": 数值}}]'}
          </div>
        </Form>
      </Modal>
    </div>
  );
};

export default IcebergManager;
