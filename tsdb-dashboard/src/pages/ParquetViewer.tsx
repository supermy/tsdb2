import React, { useEffect, useState, useCallback } from 'react';
import { Card, Typography, Table, Tag, Button, Space, Modal, Spin, InputNumber, Row, Col, Statistic, message } from 'antd';
import { api, type ParquetFileInfo, type ParquetPreview } from '../api';
import { fmtBytes } from '../utils';

const { Title, Text } = Typography;

const tierTag = (tier: string) => {
  switch (tier) {
    case 'warm': return <Tag color="#faad14">🌤️ 温数据</Tag>;
    case 'cold': return <Tag color="#1890ff">❄️ 冷数据</Tag>;
    case 'archive': return <Tag color="#52c41a">📦 归档</Tag>;
    default: return <Tag>{tier || '未知'}</Tag>;
  }
};

const ParquetViewer: React.FC = () => {
  const [files, setFiles] = useState<ParquetFileInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [preview, setPreview] = useState<ParquetPreview | null>(null);
  const [previewModal, setPreviewModal] = useState(false);
  const [previewLimit, setPreviewLimit] = useState(50);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [filterTier, setFilterTier] = useState<string>('all');

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const list = await api.parquet.list();
      setFiles(list);
    } catch (e) { console.error(e); message.error('加载Parquet文件列表失败'); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handlePreview = async (path: string) => {
    setPreviewLoading(true);
    try {
      const result = await api.parquet.preview(path, previewLimit);
      setPreview(result);
      setPreviewModal(true);
    } catch (e) { console.error(e); message.error('预览数据失败'); }
    finally { setPreviewLoading(false); }
  };

  const filteredFiles = filterTier === 'all' ? files : files.filter(f => f.tier === filterTier);

  const warmCount = files.filter(f => f.tier === 'warm').length;
  const coldCount = files.filter(f => f.tier === 'cold').length;
  const archiveCount = files.filter(f => f.tier === 'archive').length;
  const warmSize = files.filter(f => f.tier === 'warm').reduce((s, f) => s + f.file_size, 0);
  const coldSize = files.filter(f => f.tier === 'cold').reduce((s, f) => s + f.file_size, 0);
  const archiveSize = files.filter(f => f.tier === 'archive').reduce((s, f) => s + f.file_size, 0);

  const columns = [
    { title: '层级', dataIndex: 'tier', key: 'tier', width: 100,
      render: (v: string) => tierTag(v)
    },
    { title: 'Measurement', dataIndex: 'measurement', key: 'measurement', width: 130,
      render: (v: string) => v ? <Tag color="blue">{v}</Tag> : <Text type="secondary">-</Text>
    },
    { title: '文件路径', dataIndex: 'path', key: 'path', width: 500,
      render: (v: string) => <Text copyable style={{ fontSize: 12, fontFamily: 'monospace' }}>{v}</Text>
    },
    { title: '大小', dataIndex: 'file_size', key: 'size', width: 90,
      render: (v: number) => fmtBytes(v)
    },
    { title: '行数', dataIndex: 'num_rows', key: 'rows', width: 90,
      render: (v: number) => v.toLocaleString()
    },
    { title: '列数', dataIndex: 'num_columns', key: 'cols', width: 60 },
    { title: '压缩', dataIndex: 'compression', key: 'comp', width: 90,
      render: (v: string) => <Tag color="blue">{v}</Tag>
    },
    { title: '修改时间', dataIndex: 'modified', key: 'modified', width: 160 },
    { title: '操作', key: 'action', width: 80,
      render: (_: unknown, record: ParquetFileInfo) => (
        <Button size="small" type="primary" onClick={() => handlePreview(record.path)} loading={previewLoading}>预览</Button>
      ),
    },
  ];

  const previewColumns = preview ? preview.columns.map(col => ({
    title: col,
    dataIndex: col,
    key: col,
    width: 150,
    render: (v: unknown) => <Text style={{ fontSize: 12 }}>{String(v ?? '')}</Text>,
  })) : [];

  return (
    <Spin spinning={loading}>
      <div>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
          <Title level={3} style={{ color: '#fff', margin: 0 }}>Parquet 数据查看</Title>
          <Space>
            <Text style={{ color: '#aaa' }}>预览行数:</Text>
            <InputNumber min={10} max={500} value={previewLimit} onChange={v => setPreviewLimit(v || 50)} style={{ width: 80 }} />
            <Button onClick={fetchData}>刷新</Button>
          </Space>
        </div>

        <Card size="small" style={{ marginBottom: 16 }}>
          <Row gutter={16}>
            <Col span={8} style={{ cursor: 'pointer' }} onClick={() => setFilterTier(filterTier === 'warm' ? 'all' : 'warm')}>
              <Statistic title="🌤️ 温数据 Parquet" value={warmCount} suffix="个文件"
                valueStyle={{ color: '#faad14', fontSize: 16 }} />
              <Text type="secondary" style={{ fontSize: 12 }}>{fmtBytes(warmSize)}</Text>
            </Col>
            <Col span={8} style={{ cursor: 'pointer' }} onClick={() => setFilterTier(filterTier === 'cold' ? 'all' : 'cold')}>
              <Statistic title="❄️ 冷数据 Parquet" value={coldCount} suffix="个文件"
                valueStyle={{ color: '#1890ff', fontSize: 16 }} />
              <Text type="secondary" style={{ fontSize: 12 }}>{fmtBytes(coldSize)}</Text>
            </Col>
            <Col span={8} style={{ cursor: 'pointer' }} onClick={() => setFilterTier(filterTier === 'archive' ? 'all' : 'archive')}>
              <Statistic title="📦 归档 Parquet" value={archiveCount} suffix="个文件"
                valueStyle={{ color: '#52c41a', fontSize: 16 }} />
              <Text type="secondary" style={{ fontSize: 12 }}>{fmtBytes(archiveSize)}</Text>
            </Col>
          </Row>
        </Card>

        <Card title={
          <Space>
            <span>Parquet 文件 ({filteredFiles.length})</span>
            {filterTier !== 'all' && (
              <Tag color={filterTier === 'warm' ? '#faad14' : filterTier === 'cold' ? '#1890ff' : '#52c41a'}>
                筛选: {filterTier === 'warm' ? '温数据' : filterTier === 'cold' ? '冷数据' : '归档'}
              </Tag>
            )}
            {filterTier !== 'all' && <Button size="small" onClick={() => setFilterTier('all')}>清除筛选</Button>}
          </Space>
        } size="small">
          <Table
            dataSource={filteredFiles}
            columns={columns}
            rowKey="path"
            size="small"
            pagination={{ pageSize: 20 }}
            scroll={{ x: 1200 }}
            locale={{ emptyText: '暂无 Parquet 文件。可通过"数据生命周期"页面将数据降级为温/冷数据生成。' }}
          />
        </Card>

        <Modal
          title={`数据预览 - ${preview?.path?.split('/').pop() || ''}`}
          open={previewModal}
          onCancel={() => setPreviewModal(false)}
          footer={null}
          width={1000}
        >
          {preview && (
            <div>
              <div style={{ marginBottom: 8, color: '#666', fontSize: 12 }}>
                总行数: {preview.total_rows} | 显示: {preview.rows.length} | 列: {preview.columns.join(', ')}
              </div>
              <Table
                dataSource={preview.rows.map((r, i) => ({ _key: i, ...r }))}
                columns={previewColumns}
                rowKey="_key"
                size="small"
                pagination={{ pageSize: 10 }}
                scroll={{ x: preview.columns.length * 150 }}
              />
            </div>
          )}
        </Modal>
      </div>
    </Spin>
  );
};

export default ParquetViewer;
