import React, { useEffect, useState, useCallback } from 'react';
import { Card, Row, Col, Statistic, Typography, Table, Tag, Button, Space, Progress, Modal, InputNumber, Spin, Popconfirm, message } from 'antd';
import { api, type LifecycleStatus, type DataTierInfo } from '../api';
import { fmtBytes } from '../utils';

const { Title, Text } = Typography;

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

const storageLabel = (storage: string, tier: string) => {
  switch (storage) {
    case 'rocksdb': return 'RocksDB';
    case 'parquet': return tier === 'cold' ? 'Parquet (ZSTD)' : 'Parquet (SNAPPY)';
    default: return storage;
  }
};

const DataLifecycle: React.FC = () => {
  const [status, setStatus] = useState<LifecycleStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [archiveDays, setArchiveDays] = useState(30);
  const [archiving, setArchiving] = useState(false);
  const [selectedHotCfs, setSelectedHotCfs] = useState<string[]>([]);
  const [selectedWarmCfs, setSelectedWarmCfs] = useState<string[]>([]);
  const [demoting, setDemoting] = useState(false);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const s = await api.lifecycle.status();
      setStatus(s);
    } catch (e: unknown) {
      message.error(e instanceof Error ? e.message : String(e));
    }
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
          ? `已归档 ${result.archived.length} 个分区:\n${result.archived.join('\n')}`
          : '没有需要归档的数据',
      });
      await fetchData();
    } catch (e) {
      Modal.error({ title: '归档失败', content: String(e) });
    }
    finally { setArchiving(false); }
  };

  const handleDemoteToWarm = async () => {
    if (selectedHotCfs.length === 0) {
      message.warning('请先选择要降级的热数据');
      return;
    }
    setDemoting(true);
    try {
      const result = await api.lifecycle.demoteToWarm(selectedHotCfs);
      Modal.success({
        title: '降级为温数据完成',
        content: result.demoted.length > 0
          ? `已降级 ${result.demoted.length} 个分区:\n${result.demoted.join('\n')}\n\n操作: 导出 Parquet + 从 RocksDB 删除 CF`
          : '没有需要降级的数据',
      });
      setSelectedHotCfs([]);
      await fetchData();
    } catch (e) {
      Modal.error({ title: '降级失败', content: String(e) });
    }
    finally { setDemoting(false); }
  };

  const handleDemoteToCold = async () => {
    const cfs = [...selectedWarmCfs];
    if (cfs.length === 0) {
      message.warning('请先选择要降级的温数据');
      return;
    }
    setDemoting(true);
    try {
      const result = await api.lifecycle.demoteToCold(cfs);
      Modal.success({
        title: '降级为冷数据完成',
        content: result.demoted.length > 0
          ? `已降级 ${result.demoted.length} 个分区:\n${result.demoted.join('\n')}\n\n操作: 移动 Parquet 到 cold 目录`
          : '没有需要降级的数据',
      });
      setSelectedWarmCfs([]);
      await fetchData();
    } catch (e) {
      Modal.error({ title: '降级失败', content: String(e) });
    }
    finally { setDemoting(false); }
  };

  const makeHotColumns = () => [
    Table.SELECTION_COLUMN,
    { title: 'CF 名称', dataIndex: 'cf_name', key: 'cf', width: 220,
      render: (v: string) => <Text copyable style={{ fontSize: 12, fontFamily: 'monospace' }}>{v}</Text>
    },
    { title: 'Measurement', dataIndex: 'measurement', key: 'measurement', width: 130,
      render: (v: string) => <Tag color="blue">{v}</Tag>
    },
    { title: '日期', dataIndex: 'date', key: 'date', width: 90 },
    { title: '年龄(天)', dataIndex: 'age_days', key: 'age', width: 70 },
    { title: 'SST 大小', dataIndex: 'sst_size', key: 'sst', width: 90,
      render: (v: number) => fmtBytes(v)
    },
    { title: '键数', dataIndex: 'num_keys', key: 'keys', width: 80,
      render: (v: number) => v.toLocaleString()
    },
    { title: '可降级', key: 'demote_eligible', width: 100,
      render: (_: unknown, record: DataTierInfo) => {
        if (record.demote_eligible === 'cold') return <Tag color="#1890ff">可→冷</Tag>;
        if (record.demote_eligible === 'warm') return <Tag color="#faad14">可→温</Tag>;
        return <Tag color="#666">不可降级</Tag>;
      }
    },
    { title: '数据路径', dataIndex: 'path', key: 'path', width: 300,
      render: (v: string) => <Text copyable style={{ fontSize: 11, fontFamily: 'monospace', color: '#aaa' }}>{v}</Text>
    },
    {
      title: '操作', key: 'action', width: 160,
      render: (_: unknown, record: DataTierInfo) => {
        if (record.demote_eligible === 'none') return <Text style={{ color: '#666', fontSize: 12 }}>年龄不足</Text>;
        return (
          <Space size={4}>
            {record.demote_eligible === 'warm' && (
              <Popconfirm title={`确定将 ${record.cf_name} 降级为温数据？数据将从 RocksDB 导出为 Parquet (SNAPPY) 并删除 CF`}
                onConfirm={() => api.lifecycle.demoteToWarm([record.cf_name])
                  .then(() => { message.success('降级为温数据成功'); fetchData(); })
                  .catch((e: unknown) => message.error(`降级失败: ${e instanceof Error ? e.message : String(e)}`))}>
                <Button size="small" type="primary" style={{ background: '#faad14', borderColor: '#faad14' }}>→ 温</Button>
              </Popconfirm>
            )}
            {record.demote_eligible === 'cold' && (
              <Popconfirm title={`确定将 ${record.cf_name} 降级为冷数据？数据将从 RocksDB 导出为 Parquet (ZSTD) 并删除 CF`}
                onConfirm={() => api.lifecycle.demoteToCold([record.cf_name])
                  .then(() => { message.success('降级为冷数据成功'); fetchData(); })
                  .catch((e: unknown) => message.error(`降级失败: ${e instanceof Error ? e.message : String(e)}`))}>
                <Button size="small" danger>→ 冷</Button>
              </Popconfirm>
            )}
          </Space>
        );
      },
    },
  ];

  const makeParquetColumns = (tier: 'warm' | 'cold') => [
    Table.SELECTION_COLUMN,
    { title: '分区', dataIndex: 'cf_name', key: 'cf', width: 220,
      render: (v: string) => <Text copyable style={{ fontSize: 12, fontFamily: 'monospace' }}>{v}</Text>
    },
    { title: 'Measurement', dataIndex: 'measurement', key: 'measurement', width: 130,
      render: (v: string) => <Tag color="blue">{v}</Tag>
    },
    { title: '日期', dataIndex: 'date', key: 'date', width: 90 },
    { title: '年龄(天)', dataIndex: 'age_days', key: 'age', width: 70 },
    { title: '数据大小', dataIndex: 'sst_size', key: 'size', width: 90,
      render: (v: number) => fmtBytes(v)
    },
    { title: '行数', dataIndex: 'num_keys', key: 'rows', width: 80,
      render: (v: number) => v.toLocaleString()
    },
    { title: '存储', key: 'storage', width: 130,
      render: (_: unknown, record: DataTierInfo) => <Tag color={tierColor(tier)}>{storageLabel(record.storage, tier)}</Tag>
    },
    { title: '数据路径', dataIndex: 'path', key: 'path', width: 400,
      render: (v: string) => <Text copyable style={{ fontSize: 11, fontFamily: 'monospace', color: '#aaa' }}>{v}</Text>
    },
    {
      title: '操作', key: 'action', width: 120,
      render: (_: unknown, record: DataTierInfo) => {
        if (tier === 'warm') {
          return (
            <Popconfirm title={`确定将 ${record.cf_name} 降级为冷数据？Parquet 文件将从 warm 移动到 cold 目录`}
              onConfirm={() => api.lifecycle.demoteToCold([record.cf_name])
                .then(() => { message.success('降级为冷数据成功'); fetchData(); })
                .catch((e: unknown) => message.error(`降级失败: ${e instanceof Error ? e.message : String(e)}`))}>
              <Button size="small" danger>→ 冷</Button>
            </Popconfirm>
          );
        }
        return <Tag color="#1890ff">冷存储</Tag>;
      },
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
            <InputNumber min={7} max={365} value={archiveDays} onChange={v => setArchiveDays(v || 30)} suffix="天" style={{ width: 120 }} />
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
                  <Statistic title="🔥 热数据 (RocksDB)" value={fmtBytes(status.total_hot_bytes)} valueStyle={{ color: '#ff4d4f', fontSize: 18 }} />
                  <Progress percent={Math.round(hotPct)} strokeColor="#ff4d4f" size="small" />
                  <Text type="secondary" style={{ fontSize: 12 }}>{status.hot_cfs.length} 个 CF · RocksDB</Text>
                </Col>
                <Col span={6}>
                  <Statistic title="🌤️ 温数据 (Parquet SNAPPY)" value={fmtBytes(status.total_warm_bytes)} valueStyle={{ color: '#faad14', fontSize: 18 }} />
                  <Progress percent={Math.round(warmPct)} strokeColor="#faad14" size="small" />
                  <Text type="secondary" style={{ fontSize: 12 }}>{status.warm_cfs.length} 个分区 · Parquet</Text>
                </Col>
                <Col span={6}>
                  <Statistic title="❄️ 冷数据 (Parquet ZSTD)" value={fmtBytes(status.total_cold_bytes)} valueStyle={{ color: '#1890ff', fontSize: 18 }} />
                  <Progress percent={Math.round(coldPct)} strokeColor="#1890ff" size="small" />
                  <Text type="secondary" style={{ fontSize: 12 }}>{status.cold_cfs.length} 个分区 · Parquet</Text>
                </Col>
                <Col span={6}>
                  <Statistic title="📦 归档数据" value={fmtBytes(status.total_archive_bytes)} valueStyle={{ color: '#52c41a', fontSize: 18 }} />
                  <Progress percent={Math.round(archivePct)} strokeColor="#52c41a" size="small" />
                  <Text type="secondary" style={{ fontSize: 12 }}>{status.archive_files.length} 个文件</Text>
                </Col>
              </Row>
              <div style={{ marginTop: 16 }}>
                <div style={{ display: 'flex', height: 24, borderRadius: 4, overflow: 'hidden' }}>
                  {hotPct > 0 && <div style={{ width: `${hotPct}%`, background: '#ff4d4f' }} title={`热 (RocksDB) ${fmtBytes(status.total_hot_bytes)}`} />}
                  {warmPct > 0 && <div style={{ width: `${warmPct}%`, background: '#faad14' }} title={`温 (Parquet) ${fmtBytes(status.total_warm_bytes)}`} />}
                  {coldPct > 0 && <div style={{ width: `${coldPct}%`, background: '#1890ff' }} title={`冷 (Parquet) ${fmtBytes(status.total_cold_bytes)}`} />}
                  {archivePct > 0 && <div style={{ width: `${archivePct}%`, background: '#52c41a' }} title={`归档 ${fmtBytes(status.total_archive_bytes)}`} />}
                </div>
              </div>
            </Card>

            <Card title="数据层级过渡规则 & 手动操作" size="small" style={{ marginBottom: 16 }}>
              <Row gutter={[16, 16]}>
                <Col span={8} style={{ textAlign: 'center' }}>
                  <div style={{ padding: 16, background: '#2a1215', borderRadius: 8 }}>
                    <div style={{ fontSize: 24 }}>🔥</div>
                    <div style={{ color: '#ff4d4f', fontWeight: 'bold', fontSize: 16 }}>热数据</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>RocksDB 中所有 CF</div>
                    <div style={{ color: '#ff4d4f', fontSize: 12, fontWeight: 'bold' }}>RocksDB</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>LSM-Tree · 高速读写</div>
                    <div style={{ marginTop: 8, fontSize: 11, color: '#999' }}>age &gt; 3天 可降级为温数据</div>
                    <div style={{ marginTop: 4 }}>
                      <Button size="small" type="primary" style={{ background: '#faad14', borderColor: '#faad14' }}
                        disabled={selectedHotCfs.length === 0} loading={demoting}
                        onClick={handleDemoteToWarm}>
                        选中 → 温 ({selectedHotCfs.length})
                      </Button>
                    </div>
                  </div>
                </Col>
                <Col span={8} style={{ textAlign: 'center' }}>
                  <div style={{ padding: 16, background: '#2b2111', borderRadius: 8 }}>
                    <div style={{ fontSize: 24 }}>🌤️</div>
                    <div style={{ color: '#faad14', fontWeight: 'bold', fontSize: 16 }}>温数据</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>已降级到 Parquet</div>
                    <div style={{ color: '#faad14', fontSize: 12, fontWeight: 'bold' }}>Parquet (SNAPPY)</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>列式存储 · 可查询</div>
                    <div style={{ marginTop: 8, fontSize: 11, color: '#999' }}>可继续降级为冷数据</div>
                    <div style={{ marginTop: 4 }}>
                      <Button size="small" danger
                        disabled={selectedWarmCfs.length === 0} loading={demoting}
                        onClick={handleDemoteToCold}>
                        选中 → 冷 ({selectedWarmCfs.length})
                      </Button>
                    </div>
                  </div>
                </Col>
                <Col span={8} style={{ textAlign: 'center' }}>
                  <div style={{ padding: 16, background: '#111d2b', borderRadius: 8 }}>
                    <div style={{ fontSize: 24 }}>❄️</div>
                    <div style={{ color: '#1890ff', fontWeight: 'bold', fontSize: 16 }}>冷数据</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>已降级到冷存储</div>
                    <div style={{ color: '#1890ff', fontSize: 12, fontWeight: 'bold' }}>Parquet (ZSTD)</div>
                    <div style={{ color: '#aaa', fontSize: 12 }}>高压缩比 · 可查询</div>
                    <div style={{ marginTop: 8 }}>
                      <Tag color="#52c41a">长期存储</Tag>
                    </div>
                  </div>
                </Col>
              </Row>
            </Card>

            <Card title={
              <Space>
                <span>🔥 热数据 — RocksDB ({status.hot_cfs.length})</span>
                {selectedHotCfs.length > 0 && (
                  <Button size="small" type="primary" style={{ background: '#faad14', borderColor: '#faad14' }}
                    loading={demoting} onClick={handleDemoteToWarm}>
                    批量 → 温数据 ({selectedHotCfs.length})
                  </Button>
                )}
              </Space>
            } size="small" style={{ marginBottom: 16 }}>
              <Table
                dataSource={status.hot_cfs}
                columns={makeHotColumns()}
                rowKey="cf_name"
                size="small"
                pagination={{ pageSize: 10 }}
                rowSelection={{
                  selectedRowKeys: selectedHotCfs,
                  onChange: (keys) => setSelectedHotCfs(keys as string[]),
                }}
              />
            </Card>

            <Card title={
              <Space>
                <span>🌤️ 温数据 — Parquet SNAPPY ({status.warm_cfs.length})</span>
                {selectedWarmCfs.length > 0 && (
                  <Button size="small" danger loading={demoting} onClick={handleDemoteToCold}>
                    批量 → 冷数据 ({selectedWarmCfs.length})
                  </Button>
                )}
              </Space>
            } size="small" style={{ marginBottom: 16 }}>
              <Table
                dataSource={status.warm_cfs}
                columns={makeParquetColumns('warm')}
                rowKey="cf_name"
                size="small"
                pagination={{ pageSize: 10 }}
                rowSelection={{
                  selectedRowKeys: selectedWarmCfs,
                  onChange: (keys) => setSelectedWarmCfs(keys as string[]),
                }}
              />
            </Card>

            <Card title={
              <Space>
                <span>❄️ 冷数据 — Parquet ZSTD ({status.cold_cfs.length})</span>
              </Space>
            } size="small" style={{ marginBottom: 16 }}>
              <Table
                dataSource={status.cold_cfs}
                columns={makeParquetColumns('cold')}
                rowKey="cf_name"
                size="small"
                pagination={{ pageSize: 10 }}
              />
            </Card>

            {status.archive_files.length > 0 && (
              <Card title={`📦 归档文件 (${status.archive_files.length})`} size="small" style={{ marginBottom: 16 }}>
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
