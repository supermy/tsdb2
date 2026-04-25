import React, { useEffect, useState, useCallback } from 'react';
import { Card, Typography, Table, Tag, Button, Space, Modal, Spin, InputNumber } from 'antd';
import { api, type ParquetFileInfo, type ParquetPreview } from '../api';

const { Title, Text } = Typography;

const fmtBytes = (bytes: number): string => {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`;
};

const ParquetViewer: React.FC = () => {
  const [files, setFiles] = useState<ParquetFileInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [preview, setPreview] = useState<ParquetPreview | null>(null);
  const [previewModal, setPreviewModal] = useState(false);
  const [previewLimit, setPreviewLimit] = useState(50);
  const [previewLoading, setPreviewLoading] = useState(false);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const list = await api.parquet.list();
      setFiles(list);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handlePreview = async (path: string) => {
    setPreviewLoading(true);
    try {
      const result = await api.parquet.preview(path, previewLimit);
      setPreview(result);
      setPreviewModal(true);
    } catch (e) { console.error(e); }
    finally { setPreviewLoading(false); }
  };

  const columns = [
    { title: '文件路径', dataIndex: 'path', key: 'path', width: 350,
      render: (v: string) => <Text copyable style={{ fontSize: 12, fontFamily: 'monospace' }}>{v.split('/').pop()}</Text>
    },
    { title: '大小', dataIndex: 'file_size', key: 'size', width: 100,
      render: (v: number) => fmtBytes(v)
    },
    { title: '行数', dataIndex: 'num_rows', key: 'rows', width: 100,
      render: (v: number) => v.toLocaleString()
    },
    { title: '列数', dataIndex: 'num_columns', key: 'cols', width: 70 },
    { title: 'Row Groups', dataIndex: 'num_row_groups', key: 'rg', width: 90 },
    { title: '压缩', dataIndex: 'compression', key: 'comp', width: 100,
      render: (v: string) => <Tag color="blue">{v}</Tag>
    },
    { title: '修改时间', dataIndex: 'modified', key: 'modified', width: 170 },
    { title: '操作', key: 'action', width: 100,
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

        <Card title={`Parquet 文件 (${files.length})`} size="small">
          <Table
            dataSource={files}
            columns={columns}
            rowKey="path"
            size="small"
            pagination={{ pageSize: 20 }}
            scroll={{ x: 1100 }}
            locale={{ emptyText: '暂无 Parquet 文件。可通过"数据生命周期"页面归档数据生成。' }}
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
