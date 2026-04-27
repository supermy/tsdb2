import React, { useEffect, useState, useCallback } from 'react';
import { Card, Row, Col, Typography, Table, Tag, Button, Space, Input } from 'antd';
import { api, type SqlResult } from '../api';

const { Title, Text } = Typography;

const SQL_EXAMPLES = [
  'SELECT * FROM sys_cpu LIMIT 10',
  'SELECT * FROM sys_memory LIMIT 10',
  'SELECT measurement, COUNT(*) as cnt FROM sys_cpu GROUP BY measurement',
  'SELECT * FROM bench_test LIMIT 20',
  'SHOW TABLES',
];

const SqlConsole: React.FC = () => {
  const [sql, setSql] = useState('SELECT * FROM sys_cpu LIMIT 10');
  const [result, setResult] = useState<SqlResult | null>(null);
  const [tables, setTables] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [history, setHistory] = useState<string[]>(() => {
    try {
      const saved = localStorage.getItem('tsdb2_sql_history');
      return saved ? JSON.parse(saved) : [];
    } catch { return []; }
  });

  const fetchTables = useCallback(async () => {
    try {
      const t = await api.sql.tables();
      setTables(t);
    } catch (e) { console.error(e); }
  }, []);

  useEffect(() => { fetchTables(); }, [fetchTables]);

  const executeSql = async (sqlStr?: string) => {
    const query = sqlStr || sql;
    if (!query.trim()) return;
    setLoading(true);
    try {
      const r = await api.sql.execute(query);
      setResult(r);
      setHistory(prev => {
        const updated = [query, ...prev.filter(h => h !== query)].slice(0, 20);
        try { localStorage.setItem('tsdb2_sql_history', JSON.stringify(updated)); } catch { /* ignore */ }
        return updated;
      });
    } catch (e: unknown) {
      const errMsg = e instanceof Error ? e.message : String(e);
      setResult({ sql: query, columns: ['error'], rows: [{ error: errMsg }], total_rows: 0, elapsed_ms: 0, truncated: false });
    }
    finally { setLoading(false); }
  };

  const columns = result ? result.columns.map(col => ({
    title: col,
    dataIndex: col,
    key: col,
    width: 160,
    render: (v: unknown) => <Text style={{ fontSize: 12 }}>{String(v ?? 'null')}</Text>,
  })) : [];

  return (
    <div>
      <Title level={3} style={{ color: '#fff', marginBottom: 16 }}>SQL 执行控制台</Title>

      <Row gutter={[16, 16]}>
        <Col span={6}>
          <Card title="可用表" size="small" style={{ marginBottom: 16 }}>
            {tables.map(t => (
              <div key={t} style={{ marginBottom: 4 }}>
                <Tag color="blue" style={{ cursor: 'pointer' }} onClick={() => setSql(`SELECT * FROM "${t}" LIMIT 10`)}>{t}</Tag>
              </div>
            ))}
            {tables.length === 0 && <Text type="secondary">无可用表</Text>}
          </Card>
          <Card title="示例 SQL" size="small">
            {SQL_EXAMPLES.map((s, i) => (
              <div key={i} style={{ marginBottom: 4 }}>
                <Text style={{ fontSize: 12, color: '#1890ff', cursor: 'pointer' }} onClick={() => { setSql(s); executeSql(s); }}>
                  {s}
                </Text>
              </div>
            ))}
          </Card>
          {history.length > 0 && (
            <Card title="执行历史" size="small" style={{ marginTop: 16 }}>
              {history.slice(0, 10).map((h, i) => (
                <div key={i} style={{ marginBottom: 4 }}>
                  <Text style={{ fontSize: 11, color: '#aaa', cursor: 'pointer' }} onClick={() => setSql(h)}>
                    {h.length > 50 ? h.slice(0, 50) + '...' : h}
                  </Text>
                </div>
              ))}
            </Card>
          )}
        </Col>
        <Col span={18}>
          <Card size="small">
            <Space.Compact style={{ width: '100%', marginBottom: 12 }}>
              <Input.TextArea
                value={sql}
                onChange={e => setSql(e.target.value)}
                rows={3}
                placeholder="输入 SQL 语句"
                style={{ fontFamily: 'monospace', fontSize: 13 }}
                onPressEnter={e => { if (e.ctrlKey || e.metaKey) executeSql(); }}
              />
              <Button type="primary" onClick={() => executeSql()} loading={loading} style={{ height: 'auto', minHeight: 80 }}>
                执行
              </Button>
            </Space.Compact>
            <div style={{ fontSize: 11, color: '#666', marginBottom: 8 }}>Ctrl+Enter 执行</div>
          </Card>

          {result && (
            <Card size="small" style={{ marginTop: 16 }}>
              <div style={{ marginBottom: 8, display: 'flex', justifyContent: 'space-between' }}>
                <Space>
                  <Tag color="green">{result.total_rows} 行</Tag>
                  <Tag color="blue">{result.columns.length} 列</Tag>
                  <Tag color="orange">{result.elapsed_ms.toFixed(1)} ms</Tag>
                  {result.truncated && <Tag color="red">结果已截断（上限5000行）</Tag>}
                </Space>
                <Text style={{ fontSize: 11, color: '#666', maxWidth: 400, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {result.sql}
                </Text>
              </div>
              <Table
                dataSource={result.rows.map((r, i) => ({ _key: i, ...r }))}
                columns={columns}
                rowKey="_key"
                size="small"
                pagination={{ pageSize: 20 }}
                scroll={{ x: result.columns.length * 160 }}
                loading={loading}
              />
            </Card>
          )}
        </Col>
      </Row>
    </div>
  );
};

export default SqlConsole;
