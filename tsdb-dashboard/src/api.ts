import { useEffect, useRef, useState } from 'react';

const API_BASE = '/api';

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(text || `HTTP ${res.status}`);
  }
  const json = await res.json();
  if (!json.success) throw new Error(json.error || 'Request failed');
  return json.data as T;
}

export interface ServiceInfo {
  name: string;
  status: 'Running' | 'Stopped' | 'Error';
  port: number;
  config: string;
  data_dir: string;
  engine: string;
  enable_iceberg: boolean;
  pid: number | null;
  uptime_secs: number | null;
}

export interface ConfigProfile {
  name: string;
  content: string;
  description: string;
}

export interface ConfigDiff {
  key: string;
  value_a: string;
  value_b: string;
}

export interface HealthStatus {
  service: string;
  healthy: boolean;
  storage_ok: boolean;
  memory_usage_pct: number;
  disk_usage_pct: number;
  l0_file_count: number;
  compaction_idle: boolean;
  measurements_count: number;
  total_data_points: number;
  cpu_pct: number;
  sys_memory_pct: number;
  sys_disk_pct: number;
  load_avg_1m: number;
  uptime_secs: number;
  cpu_temp_c: number;
  gpu_temp_c: number;
}

export interface MetricsSnapshot {
  timestamp_ms: number;
  write_rate: number;
  read_rate: number;
  write_latency_us: number;
  read_latency_us: number;
  memory_bytes: number;
  disk_bytes: number;
  l0_file_count: number;
  compaction_count: number;
  cpu_pct: number;
  sys_memory_total_bytes: number;
  sys_memory_used_bytes: number;
  sys_memory_pct: number;
  sys_disk_total_bytes: number;
  sys_disk_used_bytes: number;
  sys_disk_pct: number;
  load_avg_1m: number;
  net_rx_bytes: number;
  net_tx_bytes: number;
  cpu_temp_c: number;
  gpu_temp_c: number;
}

export interface Alert {
  level: 'Info' | 'Warning' | 'Critical';
  message: string;
  timestamp_ms: number;
}

export interface BenchResult {
  operation: string;
  total_points: number;
  elapsed_secs: number;
  rate_per_sec: number;
  avg_latency_us: number;
  p99_latency_us: number;
}

export interface CollectorStatus {
  running: boolean;
  interval_secs: number;
  snapshots_collected: number;
  last_collect_ms: number | null;
}

export interface RocksdbOverview {
  total_cf_count: number;
  ts_cf_count: number;
  measurements: string[];
  total_sst_size_bytes: number;
  total_memtable_bytes: number;
  total_block_cache_bytes: number;
  total_keys: number;
  l0_file_count: number;
  compaction_pending: boolean;
  stats_text: string;
}

export interface CfDetail {
  name: string;
  estimate_num_keys: number;
  total_sst_file_size: number;
  num_files_at_level: number[];
  memtable_size: number;
  block_cache_usage: number;
  compaction_pending: boolean;
  stats_text: string;
}

export interface KvEntry {
  key_hex: string;
  key_ascii: string;
  value_hex: string;
  value_ascii: string;
  value_size: number;
  decoded: Record<string, unknown> | null;
}

export interface KvScanResult {
  cf: string;
  entries: KvEntry[];
  total_scanned: number;
  has_more: boolean;
}

export interface SeriesSchema {
  measurements: MeasurementSchema[];
}

export interface MeasurementSchema {
  name: string;
  column_families: string[];
  series: SeriesInfo[];
  tag_keys: string[];
  field_names: string[];
}

export interface SeriesInfo {
  tags_hash: string;
  tags: Record<string, string>;
}

export interface ParquetFileInfo {
  path: string;
  file_size: number;
  modified: string;
  num_rows: number;
  num_columns: number;
  num_row_groups: number;
  columns: ColumnMeta[];
  compression: string;
  created_by: string;
}

export interface ColumnMeta {
  name: string;
  data_type: string;
  compressed_size: number;
  uncompressed_size: number;
}

export interface ParquetPreview {
  path: string;
  columns: string[];
  rows: Record<string, unknown>[];
  total_rows: number;
}

export interface SqlResult {
  sql: string;
  columns: string[];
  rows: Record<string, unknown>[];
  total_rows: number;
  elapsed_ms: number;
}

export interface LifecycleStatus {
  hot_cfs: DataTierInfo[];
  warm_cfs: DataTierInfo[];
  cold_cfs: DataTierInfo[];
  archive_files: ArchiveFileInfo[];
  parquet_partitions: ParquetPartitionInfo[];
  total_hot_bytes: number;
  total_warm_bytes: number;
  total_cold_bytes: number;
  total_archive_bytes: number;
}

export interface DataTierInfo {
  cf_name: string;
  measurement: string;
  date: string;
  age_days: number;
  sst_size: number;
  num_keys: number;
  tier: string;
}

export interface ArchiveFileInfo {
  name: string;
  size: number;
  modified: string;
}

export interface ParquetPartitionInfo {
  date: string;
  files: string[];
  total_size: number;
  tier: string;
}

export const api = {
  services: {
    list: () => request<ServiceInfo[]>('/services'),
    get: (name: string) => request<ServiceInfo>(`/services/${name}`),
    create: (data: Omit<ServiceInfo, 'status' | 'pid' | 'uptime_secs'> & { enable_iceberg?: boolean }) =>
      request<ServiceInfo>('/services', { method: 'POST', body: JSON.stringify(data) }),
    delete: (name: string) => request<{ deleted: string }>(`/services/${name}`, { method: 'DELETE' }),
    start: (name: string) => request<ServiceInfo>(`/services/${name}/start`, { method: 'POST' }),
    stop: (name: string) => request<ServiceInfo>(`/services/${name}/stop`, { method: 'POST' }),
    restart: (name: string) => request<ServiceInfo>(`/services/${name}/restart`, { method: 'POST' }),
    logs: (name: string, lines?: number) =>
      request<string[]>(`/services/${name}/logs${lines ? `?lines=${lines}` : ''}`),
  },
  configs: {
    list: () => request<ConfigProfile[]>('/configs'),
    get: (name: string) => request<ConfigProfile>(`/configs/${name}`),
    save: (name: string, content: string) =>
      request<ConfigProfile>('/configs', { method: 'POST', body: JSON.stringify({ name, content }) }),
    delete: (name: string) => request<{ deleted: string }>(`/configs/${name}`, { method: 'DELETE' }),
    apply: (service: string, profile: string) =>
      request<any>('/configs/apply', { method: 'POST', body: JSON.stringify({ service, profile }) }),
    compare: (profileA: string, profileB: string) =>
      request<ConfigDiff[]>('/configs/compare', { method: 'POST', body: JSON.stringify({ profile_a: profileA, profile_b: profileB }) }),
  },
  test: {
    sql: (service: string, sql: string) =>
      request<BenchResult>('/test/sql', { method: 'POST', body: JSON.stringify({ service, sql }) }),
    writeBench: (service: string, measurement: string, totalPoints: number, workers: number, batchSize: number) =>
      request<BenchResult>('/test/write-bench', { method: 'POST', body: JSON.stringify({ service, measurement, total_points: totalPoints, workers, batch_size: batchSize }) }),
    readBench: (service: string, measurement: string, queries: number, workers: number) =>
      request<BenchResult>('/test/read-bench', { method: 'POST', body: JSON.stringify({ service, measurement, queries, workers }) }),
  },
  metrics: {
    health: (service: string) => request<HealthStatus>(`/metrics/health?service=${service}`),
    stats: (service?: string) => request<MetricsSnapshot>(`/metrics/stats${service ? `?service=${service}` : ''}`),
    timeseries: (service?: string, metric?: string, rangeSecs?: number) =>
      request<MetricsSnapshot[]>(`/metrics/timeseries?${new URLSearchParams({ service: service || 'default', metric: metric || 'all', range_secs: String(rangeSecs || 60) })}`),
    alerts: (service: string) => request<Alert[]>(`/metrics/alerts?service=${service}`),
  },
  collector: {
    status: () => request<CollectorStatus>('/collector/status'),
    configure: (intervalSecs: number, enabled: boolean) =>
      request<CollectorStatus>('/collector/configure', { method: 'POST', body: JSON.stringify({ interval_secs: intervalSecs, enabled }) }),
    start: () => request<CollectorStatus>('/collector/start', { method: 'POST' }),
    stop: () => request<CollectorStatus>('/collector/stop', { method: 'POST' }),
  },
  rocksdb: {
    stats: () => request<RocksdbOverview>('/rocksdb/stats'),
    cfList: () => request<string[]>('/rocksdb/cf-list'),
    cfDetail: (name: string) => request<CfDetail>(`/rocksdb/cf-detail/${name}`),
    compact: (cf?: string) =>
      request<{ compacted: boolean }>('/rocksdb/compact', { method: 'POST', body: JSON.stringify({ cf: cf || null }) }),
    kvScan: (cf: string, prefix?: string, startKey?: string, limit?: number) =>
      request<KvScanResult>(`/rocksdb/kv-scan?${new URLSearchParams(Object.entries({ cf, prefix: prefix || '', start_key: startKey || '', limit: String(limit || 20) }).filter(([_, v]) => v))}`),
    kvGet: (cf: string, key: string) =>
      request<KvEntry>(`/rocksdb/kv-get?${new URLSearchParams({ cf, key })}`),
    seriesSchema: () => request<SeriesSchema>('/rocksdb/series-schema'),
  },
  parquet: {
    list: () => request<ParquetFileInfo[]>('/parquet/list'),
    fileDetail: (path: string) => request<ParquetFileInfo>(`/parquet/file-detail?${new URLSearchParams({ path })}`),
    preview: (path: string, limit?: number) => request<ParquetPreview>(`/parquet/preview?${new URLSearchParams({ path, limit: String(limit || 50) })}`),
  },
  sql: {
    execute: (sql: string) => request<SqlResult>('/sql/execute', { method: 'POST', body: JSON.stringify({ sql }) }),
    tables: () => request<string[]>('/sql/tables'),
  },
  lifecycle: {
    status: () => request<LifecycleStatus>('/lifecycle/status'),
    archive: (olderThanDays: number) => request<{ archived: string[] }>('/lifecycle/archive', { method: 'POST', body: JSON.stringify({ older_than_days: olderThanDays }) }),
  },
};

export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const [lastMessage, setLastMessage] = useState<MetricsSnapshot | null>(null);

  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${protocol}//${window.location.host}/api/ws`);
    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.success && data.data) {
          setLastMessage(data.data);
        }
      } catch { /* ignore parse errors */ }
    };
    wsRef.current = ws;
    return () => ws.close();
  }, []);

  return { lastMessage };
}
