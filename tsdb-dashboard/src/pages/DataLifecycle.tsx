import React, { useEffect, useState, useCallback } from 'react';
import { Card, Row, Col, Statistic, Typography, Table, Tag, Button, Space, Progress, Modal, InputNumber, Spin } from 'antd';
import { api, type LifecycleStatus } from '../api';

const { Title, Text } = Typography;

const fmtBytes = (bytes: number): string => {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`;
};

const tierColor = (tier: string) => {
  switch (tier) {
    case 'hot': return '#ff4d4f';
    case 'warm': return '#faad14';
    case 'cold': return '#1890ff';
    default: return '#999';
  }
};

const tierLabel = (tier: string) => {
  switch (tier) {
    case 'hot': return '🔥 热数据';
    case 'warm': return '🌤️ 温数据';
    case 'cold': return '❄️ 冷数据';
    default: return tier;
  }
};

const DataLifecycle: React.FC = () => {
  const [status, setStatus] = useState<LifecycleStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [archiveDays, setArchiveDays] = useState(30);
  const [archiving, setArchiving] = useState(false);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const s = await api.lifecycle.status();
      setStatus(s);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handleArchive = async () => {
    setArchiving(true);
    try {
      const result = await api.lifecycle.archive(archiveDays);
      Modal.success({
        title: '归档完成',
        content: result.archived.length > 0
          ? `已归档 ${result.archived.length} 个 CF:\n${result.archived.join('\n')}`
          : '没有需要归档的数据',
      });
      await fetchData();
    } catch (e) {
      Modal.error({ title: '归档失败', content: String(e) });
    }
    finally { setArchiving(false); }
  };

  const cfColumns = [
    { title: 'CF', dataIndex: 'cf_name', key: 'cf', width: 250,
      render: (v: string) => <Text copyable style={{ fontSize: 12, fontFamily: 'monospace' }}>{v}</Text>
    },
    { title: 'Measurement', dataIndex: 'measurement', key: 'measurement', width: 150,
      render: (v: string) => <Tag color="blue">{v}</Tag>
    },
    { title: '日期', dataIndex: 'date', key: 'date', width: 100 },
    { title: '年龄(天)', dataIndex: 'age_days', key: 'age', width: 80 },
    { title: 'SST 大小', dataIndex: 'sst_size', key: 'sst', width: 100,
      render: (v: number) => fmtBytes(v)
    },
    { title: '键数', dataIndex: 'num_keys', key: 'keys', width: 100,
      render: (v: number) => v.toLocaleString()
    },
    { title: '层级', dataIndex: 'tier', key: 'tier', width: 100,
      render: (v: string) => <Tag color={tierColor(v)}>{tierLabel(v)}</Tag>
    },
  ];

  const archiveColumns = [
    { title: '文件名', dataIndex: 'name', key: 'name', width: 300 },
    { title: '大小', dataIndex: 'size', key: 'size', width: 100,
      render: (v: number) => fmtBytes(v)
    },
    { title: '修改时间', dataIndex: 'modified', key: 'modified', width: 180 },
  ];

  const partitionColumns = [
    { title: '日期', dataIndex: 'date', key: 'date', width: 100 },
    { title: '文件数', dataIndex: 'files', key: 'files', width: 80,
      render: (v: string[]) => v.length
    },
    { title: '总大小', dataIndex: 'total_size', key: 'size', width: 100,
      render: (v: number) => fmtBytes(v)
    },
    { title: '层级', dataIndex: 'tier', key: 'tier', width: 100,
      render: (v: string) => <Tag color={tierColor(v)}>{tierLabel(v)}</Tag>
    },
    { title: '文件列表', dataIndex: 'files', key: 'file_list',
      render: (v: string[]) => <Text style={{ fontSize: 11, color: '#aaa' }}>{v.join(', ')}</Text>
    },
  ];

  const total = (status?.total_hot_bytes || 0) + (status?.total_warm_bytes || 0) + (status?.total_cold_bytes || 0) + (status?.total_archive_bytes || 0);
  const hotPct = total > 0 ? (status?.total_hot_bytes || 0) / total * 100 : 0;
  const warmPct = total > 0 ? (status?.total_warm_bytes || 0) / total * 100 : 0;
  const coldPct = total > 0 ? (status?.total_cold_bytes || 0) / total * 100 : 0;
  const archivePct = total > 0 ? (status?.total_archive_bytes || 0) / total * 100 : 0;

  return (
    <Spin spinning={loading}>
      <div>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
          <Title level={3} style={{ color: '#fff', margin: 0 }}>数据生命周期</Title>
          <Space>
            <Text style={{ color: '#aaa' }}>归档超过</Text>
            <InputNumber min={1} max={365} value={archiveDays} onChange={v => setArchiveDays(v || 30)} suffix="天" style={{ width: 120 }} />
            <Text style={{ color: '#aaa' }}>的数据</Text>
            <Button type="primary" danger onClick={handleArchive} loading={archiving}>执行归档</Button>
            <Button onClick={fetchData}>刷新</Button>
          </Space>
        </div>

        {status && (
          <>
            <Card title="数据分布概览" size="small" style={{ marginBottom: 16 }}>
              <Row gutter={[16, 16]}>
                <Col span={6}>
                  <Statistic title="🔥 热数据 (≤3天)" value={fmtBytes(status.total_hot_bytes)} valueStyle={{ color: '#ff4d4f', fontSize: 18 }} />
                  <Progress percent={Math.round(hotPct)} strokeColor="#ff4d4f" size="small" />
                  <Text type="secondary" style={{ fontSize: 12 }}>{status.hot_cfs.length} 个 CF</Text>
                </Col>
                <Col span={6}>
                  <Statistic title="🌤️ 温数据 (4-14天)" value={fmtBytes(status.total_warm_bytes)} valueStyle={{ color: '#faad14', fontSize: 18 }} />
                  <Progress percent={Math.round(warmPct)} strokeColor="#faad14" size="small" />
                  <Text type="secondary" style={{ fontSize: 12 }}>{status.warm_cfs.length} 个 CF</Text>
                </Col>
                <Col span={6}>
                  <Statistic title="❄️ 冷数据 (>14天)" value={fmtBytes(status.total_cold_bytes)} valueStyle={{ color: '#1890ff', fontSize: 18 }} />
                  <Progress percent={Math.round(coldPct)} strokeColor="#1890ff" size="small" />
                  <Text type="secondary" style={{ fontSize: 12 }}>{status.cold_cfs.length} 个 CF</Text>
                </Col>
                <Col span={6}>
                  <Statistic title="📦 归档数据" value={fmtBytes(status.total_archive_bytes)} valueStyle={{ color: '#52c41a', fontSize: 18 }} />
                  <Progress percent={Math.round(archivePct)} strokeColor="#52c41a" size="small" />
                  <Text type="secondary" style={{ fontSize: 12 }}>{status.archive_files.length} 个文件</Text>
                </Col>
              </Row>
              <div style={{ marginTop: 16 }}>
                <div style={{ display: 'flex', height: 24, borderRadius: 4, overflow: 'hidden' }}>
                  {hotPct > 0 && <div style={{ width: `${hotPct}%`, background: '#ff4d4f' }} title={`热 ${fmtBytes(status.total_hot_bytes)}`} />}
                  {warmPct > 0 && <div style={{ width: `${warmPct}%`, background: '#faad14' }} title={`温 ${fmtBytes(status.total_warm_bytes)}`} />}
                  {coldPct > 0 && <div style={{ width: `${coldPct}%`, background: '#1890ff' }} title={`冷 ${fmtBytes(status.total_cold_bytes)}`} />}
                  {archivePct > 0 && <div style={{ width: `${archivePct}%`, background: '#52c41a' }} title={`归档 ${fmtBytes(status.total_archive_bytes)}`} />}
                </div>
              </div>
            </Card>

            <Card title="数据层级过渡规则" size="small" style={{ marginBottom: 16 }}>
              <Row gutter={[16, 16]}>
                <Col span={8} style={{ textAlign: 'center' }}>
                  <div style={{ padding: 16, background: '#2a1215', borderRadius: 8 }}>
                    <div style={{ fontSize: 24 }}>🔥</div>
                    <div style={{ color: '#ff4d4f', fontWeight: 'bold', fontSize: 16 }}>热数据</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>≤ 3 天</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>RocksDB 内存 + SST</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>SNAPPY 压缩</div>
                  </div>
                </Col>
                <Col span={8} style={{ textAlign: 'center' }}>
                  <div style={{ padding: 16, background: '#2b2111', borderRadius: 8 }}>
                    <div style={{ fontSize: 24 }}>🌤️</div>
                    <div style={{ color: '#faad14', fontWeight: 'bold', fontSize: 16 }}>温数据</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>4-14 天</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>RocksDB SST only</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>自动降级压缩</div>
                  </div>
                </Col>
                <Col span={8} style={{ textAlign: 'center' }}>
                  <div style={{ padding: 16, background: '#111d2b', borderRadius: 8 }}>
                    <div style={{ fontSize: 24 }}>❄️</div>
                    <div style={{ color: '#1890ff', fontWeight: 'bold', fontSize: 16 }}>冷数据</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>{'> 14 天'}</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>归档 → Parquet</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>ZSTD 压缩</div>
                  </div>
                </Col>
              </Row>
            </Card>

            <Card title={`热数据 CF (${status.hot_cfs.length})`} size="small" style={{ marginBottom: 16 }}>
              <Table dataSource={status.hot_cfs} columns={cfColumns} rowKey="cf_name" size="small" pagination={{ pageSize: 10 }} />
            </Card>

            <Card title={`温数据 CF (${status.warm_cfs.length})`} size="small" style={{ marginBottom: 16 }}>
              <Table dataSource={status.warm_cfs} columns={cfColumns} rowKey="cf_name" size="small" pagination={{ pageSize: 10 }} />
            </Card>

            <Card title={`冷数据 CF (${status.cold_cfs.length})`} size="small" style={{ marginBottom: 16 }}>
              <Table dataSource={status.cold_cfs} columns={cfColumns} rowKey="cf_name" size="small" pagination={{ pageSize: 10 }} />
            </Card>

            {status.archive_files.length > 0 && (
              <Card title={`归档文件 (${status.archive_files.length})`} size="small" style={{ marginBottom: 16 }}>
                <Table dataSource={status.archive_files} columns={archiveColumns} rowKey="name" size="small" pagination={{ pageSize: 10 }} />
              </Card>
            )}

            {status.parquet_partitions.length > 0 && (
              <Card title={`Parquet 分区 (${status.parquet_partitions.length})`} size="small">
                <Table dataSource={status.parquet_partitions} columns={partitionColumns} rowKey="date" size="small" pagination={{ pageSize: 10 }} />
              </Card>
            )}
          </>
        )}
      </div>
    </Spin>
  );
};

export default DataLifecycle;
