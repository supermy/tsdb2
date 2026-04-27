import React, { useEffect, useRef, useState, useCallback } from 'react';
import { Card, Row, Col, Statistic, Typography, Table, Tag, Button, Space, Modal, Spin, Progress, Select, InputNumber, Input, Tabs, Collapse, message } from 'antd';
import { api, type RocksdbOverview, type CfDetail, type KvEntry, type KvScanResult, type SeriesSchema, type MeasurementSchema } from '../api';
import { fmtBytes } from '../utils';

const { Title, Text } = Typography;

const truncateHex = (hex: string, maxLen: number = 40): string => {
  if (hex.length <= maxLen) return hex;
  return hex.slice(0, maxLen) + '...';
};

const RocksdbStats: React.FC = () => {
  const [overview, setOverview] = useState<RocksdbOverview | null>(null);
  const [cfList, setCfList] = useState<string[]>([]);
  const [cfDetails, setCfDetails] = useState<CfDetail[]>([]);
  const [selectedCf, setSelectedCf] = useState<CfDetail | null>(null);
  const [statsModal, setStatsModal] = useState(false);
  const [statsText, setStatsText] = useState('');
  const [loading, setLoading] = useState(false);
  const [compacting, setCompacting] = useState(false);

  const [schema, setSchema] = useState<SeriesSchema | null>(null);
  const [schemaLoading, setSchemaLoading] = useState(false);

  const [kvCf, setKvCf] = useState('');
  const [kvPrefix, setKvPrefix] = useState('');
  const [kvLimit, setKvLimit] = useState(20);
  const [kvResult, setKvResult] = useState<KvScanResult | null>(null);
  const [kvLoading, setKvLoading] = useState(false);
  const [kvDetailEntry, setKvDetailEntry] = useState<KvEntry | null>(null);
  const [kvDetailModal, setKvDetailModal] = useState(false);
  const [kvGetKey, setKvGetKey] = useState('');
  const [kvGetResult, setKvGetResult] = useState<KvEntry | null>(null);
  const [kvGetLoading, setKvGetLoading] = useState(false);

  const kvCfInitialized = useRef(false);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [ov, cfs] = await Promise.all([
        api.rocksdb.stats(),
        api.rocksdb.cfList(),
      ]);
      setOverview(ov);
      setCfList(cfs);
      if (!kvCfInitialized.current && cfs.length > 0) {
        setKvCf(cfs[0]);
        kvCfInitialized.current = true;
      }

      const details = await Promise.all(
        cfs.slice(0, 50).map(cf => api.rocksdb.cfDetail(cf).catch(() => null))
      );
      setCfDetails(details.filter((d): d is CfDetail => d !== null));
    } catch (e) { console.error(e); message.error('加载数据失败'); }
    finally { setLoading(false); }
  }, []);

  const fetchSchema = useCallback(async () => {
    setSchemaLoading(true);
    try {
      const s = await api.rocksdb.seriesSchema();
      setSchema(s);
    } catch (e) { console.error(e); message.error('加载Schema失败'); }
    finally { setSchemaLoading(false); }
  }, []);

  useEffect(() => { fetchData(); fetchSchema(); }, [fetchData, fetchSchema]);

  const handleCompact = async (cf?: string) => {
    setCompacting(true);
    try {
      await api.rocksdb.compact(cf);
      message.success(cf ? `CF ${cf} 压缩完成` : '全部压缩完成');
      await fetchData();
    } catch (e) { console.error(e); message.error('压缩失败'); }
    finally { setCompacting(false); }
  };

  const showStats = (text: string) => {
    setStatsText(text);
    setStatsModal(true);
  };

  const handleKvScan = async () => {
    if (!kvCf) return;
    setKvLoading(true);
    try {
      const result = await api.rocksdb.kvScan(kvCf, kvPrefix || undefined, undefined, kvLimit);
      setKvResult(result);
    } catch (e) { console.error(e); message.error('扫描失败'); setKvResult(null); }
    finally { setKvLoading(false); }
  };

  const handleKvScanForCf = async (cf: string) => {
    setKvCf(cf);
    setKvLoading(true);
    try {
      const result = await api.rocksdb.kvScan(cf, kvPrefix || undefined, undefined, kvLimit);
      setKvResult(result);
    } catch (e) { console.error(e); message.error('扫描失败'); setKvResult(null); }
    finally { setKvLoading(false); }
  };

  const handleKvNext = async () => {
    if (!kvCf || !kvResult || kvResult.entries.length === 0) return;
    const lastKey = kvResult.entries[kvResult.entries.length - 1].key_hex;
    setKvLoading(true);
    try {
      const result = await api.rocksdb.kvScan(kvCf, kvPrefix || undefined, lastKey, kvLimit + 1);
      if (result.entries.length > 1 && result.entries[0].key_hex === lastKey) {
        result.entries = result.entries.slice(1);
        result.total_scanned = Math.max(0, result.total_scanned - 1);
      }
      setKvResult(result);
    } catch (e) { console.error(e); message.error('加载下一页失败'); }
    finally { setKvLoading(false); }
  };

  const handleKvGet = async () => {
    if (!kvCf || !kvGetKey) return;
    setKvGetLoading(true);
    try {
      const result = await api.rocksdb.kvGet(kvCf, kvGetKey);
      setKvGetResult(result);
    } catch (e) {
      console.error(e);
      setKvGetResult(null);
    }
    finally { setKvGetLoading(false); }
  };

  const showKvDetail = (entry: KvEntry) => {
    setKvDetailEntry(entry);
    setKvDetailModal(true);
  };

  const cfColumns = [
    { title: 'CF 名称', dataIndex: 'name', key: 'name', width: 250,
      render: (name: string) => <Text copyable style={{ color: '#1890ff', cursor: 'pointer' }}
        onClick={() => { const d = cfDetails.find(c => c.name === name); if (d) setSelectedCf(d); }}>{name}</Text>
    },
    { title: '预估键数', dataIndex: 'estimate_num_keys', key: 'keys', width: 120,
      render: (v: number) => v.toLocaleString()
    },
    { title: 'SST 大小', dataIndex: 'total_sst_file_size', key: 'sst', width: 120,
      render: (v: number) => fmtBytes(v)
    },
    { title: 'MemTable', dataIndex: 'memtable_size', key: 'mem', width: 100,
      render: (v: number) => fmtBytes(v)
    },
    { title: 'Block Cache', dataIndex: 'block_cache_usage', key: 'cache', width: 100,
      render: (v: number) => fmtBytes(v)
    },
    { title: 'L0', dataIndex: 'num_files_at_level', key: 'l0', width: 60,
      render: (v: number[]) => v[0] || 0
    },
    { title: 'L1', dataIndex: 'num_files_at_level', key: 'l1', width: 60,
      render: (v: number[]) => v[1] || 0
    },
    { title: 'L2', dataIndex: 'num_files_at_level', key: 'l2', width: 60,
      render: (v: number[]) => v[2] || 0
    },
    { title: '压缩', dataIndex: 'compaction_pending', key: 'compact', width: 80,
      render: (v: boolean) => v ? <Tag color="orange">进行中</Tag> : <Tag color="green">空闲</Tag>
    },
    { title: '操作', key: 'action', width: 200,
      render: (_: unknown, record: CfDetail) => (
        <Space size={4}>
          <Button size="small" onClick={() => handleCompact(record.name)} loading={compacting}>压缩</Button>
          <Button size="small" onClick={() => showStats(record.stats_text)}>统计</Button>
          <Button size="small" type="primary" onClick={() => handleKvScanForCf(record.name)}>数据</Button>
        </Space>
      ),
    },
  ];

  const kvColumns = [
    { title: '#', key: 'idx', width: 40,
      render: (_: unknown, __: unknown, i: number) => i + 1
    },
    { title: 'Key (hex)', dataIndex: 'key_hex', key: 'key_hex', width: 280,
      render: (v: string, record: KvEntry) => (
        <Text copyable style={{ color: '#1890ff', cursor: 'pointer', fontSize: 12, fontFamily: 'monospace' }}
          onClick={() => showKvDetail(record)}>
          {truncateHex(v, 48)}
        </Text>
      )
    },
    { title: '维度 (Tags)', dataIndex: 'decoded', key: 'tags', width: 200,
      render: (v: KvEntry['decoded']) => {
        if (!v || !v.tags) return <Text type="secondary" style={{ fontSize: 12 }}>-</Text>;
        const tags = v.tags as Record<string, string>;
        return <Space size={4} wrap>
          {Object.entries(tags).map(([k, val]) => (
            <Tag key={k} color="blue" style={{ fontSize: 11 }}>{k}={val}</Tag>
          ))}
        </Space>;
      }
    },
    { title: '时间戳', dataIndex: 'decoded', key: 'timestamp', width: 160,
      render: (v: KvEntry['decoded']) => {
        if (!v || !v.timestamp) return '-';
        return <Text style={{ fontSize: 12, fontFamily: 'monospace' }}>{String(v.timestamp)}</Text>;
      }
    },
    { title: '指标 (Fields)', dataIndex: 'decoded', key: 'fields', width: 250,
      render: (v: KvEntry['decoded']) => {
        if (!v || !v.fields) return <Text type="secondary" style={{ fontSize: 12 }}>-</Text>;
        const fields = v.fields as Record<string, unknown>;
        return <Space size={4} wrap>
          {Object.entries(fields).map(([k, val]) => (
            <Tag key={k} color="green" style={{ fontSize: 11 }}>{k}={String(val)}</Tag>
          ))}
        </Space>;
      }
    },
    { title: 'Value Size', dataIndex: 'value_size', key: 'value_size', width: 80,
      render: (v: number) => fmtBytes(v)
    },
    { title: '操作', key: 'action', width: 60,
      render: (_: unknown, record: KvEntry) => (
        <Button size="small" onClick={() => showKvDetail(record)}>详情</Button>
      ),
    },
  ];

  const seriesColumns = [
    { title: 'Tags Hash', dataIndex: 'tags_hash', key: 'tags_hash', width: 200,
      render: (v: string) => <Text copyable style={{ fontFamily: 'monospace', fontSize: 12, color: '#1890ff' }}>{v}</Text>
    },
    { title: '维度 (Tags)', dataIndex: 'tags', key: 'tags',
      render: (v: Record<string, string>) => (
        <Space size={4} wrap>
          {Object.entries(v).map(([k, val]) => (
            <Tag key={k} color="blue" style={{ fontSize: 11 }}>{k}={val}</Tag>
          ))}
        </Space>
      )
    },
  ];

  return (
    <Spin spinning={loading}>
      <div>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
          <Title level={3} style={{ color: '#fff', margin: 0 }}>RocksDB 数据库统计</Title>
          <Space>
            <Button onClick={() => handleCompact()} loading={compacting}>全部压缩</Button>
            <Button onClick={() => overview && showStats(overview.stats_text)}>全局统计</Button>
            <Button onClick={() => { fetchData(); fetchSchema(); }}>刷新</Button>
          </Space>
        </div>

        {overview && (
          <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
            <Col span={4}>
              <Card size="small"><Statistic title="CF 总数" value={overview.total_cf_count} /></Card>
            </Col>
            <Col span={4}>
              <Card size="small"><Statistic title="时序 CF" value={overview.ts_cf_count} /></Card>
            </Col>
            <Col span={4}>
              <Card size="small"><Statistic title="总键数" value={overview.total_keys} /></Card>
            </Col>
            <Col span={4}>
              <Card size="small"><Statistic title="SST 总大小" value={fmtBytes(overview.total_sst_size_bytes)} valueStyle={{ fontSize: 16 }} /></Card>
            </Col>
            <Col span={4}>
              <Card size="small"><Statistic title="MemTable" value={fmtBytes(overview.total_memtable_bytes)} valueStyle={{ fontSize: 16 }} /></Card>
            </Col>
            <Col span={4}>
              <Card size="small">
                <Statistic title="L0 文件" value={overview.l0_file_count}
                  valueStyle={{ color: overview.l0_file_count > 16 ? '#faad14' : '#52c41a' }} />
                {overview.compaction_pending && <Tag color="orange" style={{ marginTop: 4 }}>压缩中</Tag>}
              </Card>
            </Col>
          </Row>
        )}

        <Card title="指标与维度对应关系" size="small" style={{ marginBottom: 16 }}
          extra={<Button size="small" onClick={fetchSchema} loading={schemaLoading}>刷新</Button>}>
          {schema && schema.measurements.length > 0 ? (
            <div>
              {schema.measurements.some(m => m.name.startsWith('sys_')) && (
                <Card size="small" title="🖥️ 系统指标快捷入口" style={{ marginBottom: 12, background: '#1a1a2e' }}>
                  <Space wrap>
                    {schema.measurements.filter(m => m.name.startsWith('sys_')).map(m => {
                      const icons: Record<string, string> = {
                        sys_cpu: '🖥️ CPU',
                        sys_memory: '💾 内存',
                        sys_disk: '💿 磁盘',
                        sys_network: '🌐 网络',
                        sys_temp: '🌡️ 温度',
                      };
                      const cf = m.column_families[0];
                      return (
                        <Button key={m.name} type="primary" ghost
                          onClick={() => { if (cf) handleKvScanForCf(cf); }}
                          style={{ textAlign: 'left', height: 'auto', padding: '8px 16px' }}>
                          <div>
                            <div style={{ fontWeight: 'bold', fontSize: 13 }}>{icons[m.name] || m.name}</div>
                            <div style={{ fontSize: 11, color: '#aaa', marginTop: 2 }}>
                              {m.field_names.join(', ')}
                            </div>
                          </div>
                        </Button>
                      );
                    })}
                  </Space>
                </Card>
              )}
              <Collapse accordion>
                {schema.measurements.map((m: MeasurementSchema) => (
                  <Collapse.Panel
                    key={m.name}
                    header={
                      <Space size={12}>
                        <Text strong style={{ color: '#1890ff', fontSize: 14 }}>{m.name}</Text>
                        <Tag color="purple">{m.series.length} 序列</Tag>
                        <Tag color="blue">{m.tag_keys.length} 维度</Tag>
                        <Tag color="green">{m.field_names.length} 指标</Tag>
                        <Tag color="orange">{m.column_families.length} CF</Tag>
                        {m.name.startsWith('sys_') && <Tag color="cyan">系统指标</Tag>}
                      </Space>
                    }
                  >
                    <Row gutter={[16, 16]} style={{ marginBottom: 12 }}>
                      <Col span={12}>
                        <Card size="small" title="维度 (Tags)" bodyStyle={{ minHeight: 60 }}>
                          <Space wrap>
                            {m.tag_keys.map(k => (
                              <Tag key={k} color="blue" style={{ fontSize: 13 }}>{k}</Tag>
                            ))}
                            {m.tag_keys.length === 0 && <Text type="secondary">无维度</Text>}
                          </Space>
                        </Card>
                      </Col>
                      <Col span={12}>
                        <Card size="small" title="指标 (Fields)" bodyStyle={{ minHeight: 60 }}>
                          <Space wrap>
                            {m.field_names.map(f => (
                              <Tag key={f} color="green" style={{ fontSize: 13 }}>{f}</Tag>
                            ))}
                            {m.field_names.length === 0 && <Text type="secondary">无指标</Text>}
                          </Space>
                        </Card>
                      </Col>
                    </Row>
                    <Space style={{ marginBottom: 8 }}>
                      {m.column_families[0] && (
                        <Button type="primary" size="small"
                          onClick={() => handleKvScanForCf(m.column_families[0])}>
                          查看数据
                        </Button>
                      )}
                    </Space>
                    <Card size="small" title={`序列 (${m.series.length})`} bodyStyle={{ padding: 0 }}>
                      <Table
                        dataSource={m.series}
                        columns={seriesColumns}
                        rowKey="tags_hash"
                        size="small"
                        pagination={{ pageSize: 10 }}
                      />
                    </Card>
                    <div style={{ marginTop: 8 }}>
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        Column Families: {m.column_families.join(', ')}
                      </Text>
                    </div>
                  </Collapse.Panel>
                ))}
              </Collapse>
            </div>
          ) : (
            <Text type="secondary">{schemaLoading ? '加载中...' : '暂无数据'}</Text>
          )}
        </Card>

        {selectedCf && (
          <Card title={`CF 详情: ${selectedCf.name}`} size="small" style={{ marginBottom: 16 }}
            extra={<Button size="small" onClick={() => setSelectedCf(null)}>关闭</Button>}>
            <Row gutter={[16, 16]}>
              <Col span={4}>
                <Statistic title="预估键数" value={selectedCf.estimate_num_keys.toLocaleString()} />
              </Col>
              <Col span={4}>
                <Statistic title="SST 大小" value={fmtBytes(selectedCf.total_sst_file_size)} valueStyle={{ fontSize: 16 }} />
              </Col>
              <Col span={4}>
                <Statistic title="MemTable" value={fmtBytes(selectedCf.memtable_size)} valueStyle={{ fontSize: 16 }} />
              </Col>
              <Col span={4}>
                <Statistic title="Block Cache" value={fmtBytes(selectedCf.block_cache_usage)} valueStyle={{ fontSize: 16 }} />
              </Col>
              <Col span={8}>
                <div style={{ marginBottom: 8, color: '#aaa', fontSize: 12 }}>各层文件数</div>
                {selectedCf.num_files_at_level.map((count, level) => (
                  <div key={level} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                    <Text style={{ color: '#aaa', width: 30, fontSize: 12 }}>L{level}</Text>
                    <Progress percent={Math.min(count, 100)} size="small"
                      style={{ flex: 1 }}
                      strokeColor={count > 20 ? '#ff4d4f' : count > 10 ? '#faad14' : '#52c41a'}
                      format={() => `${count}`}
                    />
                  </div>
                ))}
              </Col>
            </Row>
          </Card>
        )}

        <Card title={`Column Families (${cfList.length})`} size="small" style={{ marginBottom: 16 }}>
          <Table
            dataSource={cfDetails}
            columns={cfColumns}
            rowKey="name"
            size="small"
            pagination={{ pageSize: 20 }}
            scroll={{ x: 1200 }}
          />
        </Card>

        <Card title="Key/Value 数据浏览" size="small">
          <Tabs items={[
            {
              key: 'scan',
              label: '扫描浏览',
              children: (
                <div>
                  <Space wrap style={{ marginBottom: 12 }}>
                    <Text style={{ color: '#aaa' }}>CF:</Text>
                    <Select value={kvCf} onChange={setKvCf} style={{ width: 280 }}
                      options={cfList.map(c => ({ value: c, label: c }))} />
                    <Text style={{ color: '#aaa' }}>前缀(hex):</Text>
                    <Input value={kvPrefix} onChange={e => setKvPrefix(e.target.value)}
                      placeholder="如: 00000000" style={{ width: 160 }} />
                    <Text style={{ color: '#aaa' }}>条数:</Text>
                    <InputNumber min={1} max={200} value={kvLimit} onChange={v => setKvLimit(v || 20)} style={{ width: 80 }} />
                    <Button type="primary" onClick={handleKvScan} loading={kvLoading}>扫描</Button>
                    {kvResult?.has_more && (
                      <Button onClick={handleKvNext} loading={kvLoading}>下一页</Button>
                    )}
                  </Space>

                  {kvResult && (
                    <div style={{ marginBottom: 8 }}>
                      <Text style={{ color: '#aaa', fontSize: 12 }}>
                        CF: {kvResult.cf} | 扫描: {kvResult.total_scanned} 条 | 显示: {kvResult.entries.length} 条
                        {kvResult.has_more && ' | 还有更多'}
                      </Text>
                    </div>
                  )}

                  <Table
                    dataSource={kvResult?.entries || []}
                    columns={kvColumns}
                    rowKey="key_hex"
                    size="small"
                    pagination={false}
                    scroll={{ x: 1100 }}
                    loading={kvLoading}
                    locale={{ emptyText: '点击"扫描"浏览数据' }}
                  />
                </div>
              ),
            },
            {
              key: 'get',
              label: '精确查询',
              children: (
                <div>
                  <Space wrap style={{ marginBottom: 12 }}>
                    <Text style={{ color: '#aaa' }}>CF:</Text>
                    <Select value={kvCf} onChange={setKvCf} style={{ width: 280 }}
                      options={cfList.map(c => ({ value: c, label: c }))} />
                    <Text style={{ color: '#aaa' }}>Key(hex):</Text>
                    <Input value={kvGetKey} onChange={e => setKvGetKey(e.target.value)}
                      placeholder="输入hex编码的key" style={{ width: 300 }} />
                    <Button type="primary" onClick={handleKvGet} loading={kvGetLoading}>查询</Button>
                  </Space>

                  {kvGetResult && (
                    <Card size="small" style={{ background: '#1a1a1a' }}>
                      <Row gutter={[16, 12]}>
                        <Col span={12}>
                          <div style={{ marginBottom: 8, color: '#aaa', fontSize: 12 }}>Key (hex)</div>
                          <pre style={{ margin: 0, color: '#1890ff', fontSize: 12, fontFamily: 'monospace', wordBreak: 'break-all' }}>
                            {kvGetResult.key_hex}
                          </pre>
                        </Col>
                        <Col span={12}>
                          <div style={{ marginBottom: 8, color: '#aaa', fontSize: 12 }}>Key (ascii)</div>
                          <pre style={{ margin: 0, color: '#aaa', fontSize: 12, fontFamily: 'monospace', wordBreak: 'break-all' }}>
                            {kvGetResult.key_ascii}
                          </pre>
                        </Col>
                        <Col span={12}>
                          <div style={{ marginBottom: 8, color: '#aaa', fontSize: 12 }}>Value (hex) [{fmtBytes(kvGetResult.value_size)}]</div>
                          <pre style={{ margin: 0, color: '#fa8c16', fontSize: 12, fontFamily: 'monospace', wordBreak: 'break-all', maxHeight: 200, overflow: 'auto' }}>
                            {kvGetResult.value_hex}
                          </pre>
                        </Col>
                        <Col span={12}>
                          <div style={{ marginBottom: 8, color: '#aaa', fontSize: 12 }}>Value (ascii)</div>
                          <pre style={{ margin: 0, color: '#aaa', fontSize: 12, fontFamily: 'monospace', wordBreak: 'break-all', maxHeight: 200, overflow: 'auto' }}>
                            {kvGetResult.value_ascii}
                          </pre>
                        </Col>
                        {kvGetResult.decoded && (
                          <Col span={24}>
                            <div style={{ marginBottom: 8, color: '#aaa', fontSize: 12 }}>解码数据</div>
                            <pre style={{ margin: 0, color: '#52c41a', fontSize: 12, fontFamily: 'monospace', wordBreak: 'break-all', maxHeight: 300, overflow: 'auto', background: '#0d1117', padding: 12, borderRadius: 4 }}>
                              {JSON.stringify(kvGetResult.decoded, null, 2)}
                            </pre>
                          </Col>
                        )}
                      </Row>
                    </Card>
                  )}
                </div>
              ),
            },
          ]} />
        </Card>

        <Modal
          title="Key/Value 详情"
          open={kvDetailModal}
          onCancel={() => setKvDetailModal(false)}
          footer={null}
          width={800}
        >
          {kvDetailEntry && (
            <div>
              <Row gutter={[16, 12]}>
                <Col span={12}>
                  <div style={{ marginBottom: 8, color: '#666', fontSize: 12 }}>Key (hex)</div>
                  <pre style={{ margin: 0, color: '#1890ff', fontSize: 12, fontFamily: 'monospace', wordBreak: 'break-all' }}>
                    {kvDetailEntry.key_hex}
                  </pre>
                </Col>
                <Col span={12}>
                  <div style={{ marginBottom: 8, color: '#666', fontSize: 12 }}>Key (ascii)</div>
                  <pre style={{ margin: 0, color: '#666', fontSize: 12, fontFamily: 'monospace', wordBreak: 'break-all' }}>
                    {kvDetailEntry.key_ascii}
                  </pre>
                </Col>
                <Col span={24}>
                  <div style={{ marginBottom: 8, color: '#666', fontSize: 12 }}>Value (hex) [{fmtBytes(kvDetailEntry.value_size)}]</div>
                  <pre style={{ margin: 0, color: '#fa8c16', fontSize: 12, fontFamily: 'monospace', wordBreak: 'break-all', maxHeight: 200, overflow: 'auto', background: '#f5f5f5', padding: 8, borderRadius: 4 }}>
                    {kvDetailEntry.value_hex}
                  </pre>
                </Col>
                <Col span={24}>
                  <div style={{ marginBottom: 8, color: '#666', fontSize: 12 }}>Value (ascii)</div>
                  <pre style={{ margin: 0, color: '#666', fontSize: 12, fontFamily: 'monospace', wordBreak: 'break-all', maxHeight: 200, overflow: 'auto', background: '#f5f5f5', padding: 8, borderRadius: 4 }}>
                    {kvDetailEntry.value_ascii}
                  </pre>
                </Col>
                {kvDetailEntry.decoded && (
                  <Col span={24}>
                    <div style={{ marginBottom: 8, color: '#666', fontSize: 12 }}>解码数据</div>
                    <pre style={{ margin: 0, color: '#52c41a', fontSize: 12, fontFamily: 'monospace', wordBreak: 'break-all', maxHeight: 300, overflow: 'auto', background: '#f5f5f5', padding: 12, borderRadius: 4 }}>
                      {JSON.stringify(kvDetailEntry.decoded, null, 2)}
                    </pre>
                  </Col>
                )}
              </Row>
            </div>
          )}
        </Modal>

        <Modal
          title="RocksDB 统计详情"
          open={statsModal}
          onCancel={() => setStatsModal(false)}
          footer={null}
          width={800}
        >
          <pre style={{ maxHeight: 500, overflow: 'auto', background: '#1a1a1a', padding: 16, color: '#ccc', fontSize: 12, borderRadius: 4 }}>
            {statsText || '无统计数据'}
          </pre>
        </Modal>
      </div>
    </Spin>
  );
};

export default RocksdbStats;
