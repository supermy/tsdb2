import { useEffect, useRef, useState } from 'react';

const API_BASE = '/api';

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 30000);
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      ...options,
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        ...options?.headers,
      },
    });
    if (!res.ok) {
      const text = await res.text().catch(() => res.statusText);
      throw new Error(text || `HTTP ${res.status}`);
    }
    const json = await res.json();
    if (!json.success) throw new Error(json.error || 'Request failed');
    return json.data as T;
  } finally {
    clearTimeout(timeoutId);
  }
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
  tier: string;
  measurement: string;
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
  truncated: boolean;
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
  path: string;
  storage: string;
  demote_eligible: string;
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

export interface LifecycleResult {
  hot_days: number;
  warm_days: number;
  cold_days: number;
  auto_demote: {
    skipped: boolean;
    note?: string;
    demoted_to_warm?: number;
    demoted_to_cold?: number;
    warm_details?: string[];
    cold_details?: string[];
  };
}

export interface GenResult {
  total_points: number;
  elapsed_secs: number;
  rate_per_sec: number;
  lifecycle: LifecycleResult;
}

export interface IcebergTableInfo {
  name: string;
  format_version: number;
  table_uuid: string;
  location: string;
  current_snapshot_id: number;
  snapshot_count: number;
  schema_fields: number;
  partition_spec: string;
  last_updated_ms: number;
  total_records: number;
  total_data_files: number;
}

export interface IcebergSnapshotInfo {
  snapshot_id: number;
  parent_snapshot_id: number | null;
  sequence_number: number;
  timestamp_ms: number;
  operation: string;
  added_data_files: number;
  deleted_data_files: number;
  added_records: number;
  deleted_records: number;
  total_data_files: number;
  total_records: number;
}

export interface IcebergFieldInfo {
  id: number;
  name: string;
  field_type: string;
  required: boolean;
  doc: string | null;
}

export interface IcebergSchemaInfo {
  schema_id: number;
  fields: IcebergFieldInfo[];
}

export interface IcebergPartitionFieldInfo {
  source_id: number;
  field_id: number;
  transform: string;
}

export interface IcebergPartitionSpecInfo {
  spec_id: number;
  fields: IcebergPartitionFieldInfo[];
}

export interface IcebergDataFileInfo {
  file_path: string;
  file_format: string;
  record_count: number;
  file_size_in_bytes: number;
  partition: Record<string, unknown>;
}

export interface IcebergTableDetail {
  name: string;
  format_version: number;
  table_uuid: string;
  location: string;
  current_snapshot_id: number;
  current_schema_id: number;
  default_spec_id: number;
  last_sequence_number: number;
  last_updated_ms: number;
  properties: Record<string, string>;
  snapshots: IcebergSnapshotInfo[];
  schema_history: IcebergSchemaInfo[];
  partition_specs: IcebergPartitionSpecInfo[];
  data_files: IcebergDataFileInfo[];
}

export interface IcebergScanResult {
  table: string;
  columns: string[];
  rows: Record<string, unknown>[];
  total_rows: number;
}

export const api = {
  services: {
    list: () => request<ServiceInfo[]>('/services'),
    get: (name: string) => request<ServiceInfo>(`/services/${encodeURIComponent(name)}`),
    create: (data: Omit<ServiceInfo, 'status' | 'pid' | 'uptime_secs'> & { enable_iceberg?: boolean }) =>
      request<ServiceInfo>('/services', { method: 'POST', body: JSON.stringify(data) }),
    delete: (name: string) => request<{ deleted: string }>(`/services/${encodeURIComponent(name)}`, { method: 'DELETE' }),
    start: (name: string) => request<ServiceInfo>(`/services/${encodeURIComponent(name)}/start`, { method: 'POST' }),
    stop: (name: string) => request<ServiceInfo>(`/services/${encodeURIComponent(name)}/stop`, { method: 'POST' }),
    restart: (name: string) => request<ServiceInfo>(`/services/${encodeURIComponent(name)}/restart`, { method: 'POST' }),
    logs: (name: string, lines?: number) =>
      request<string[]>(`/services/${encodeURIComponent(name)}/logs${lines ? `?lines=${lines}` : ''}`),
  },
  configs: {
    list: () => request<ConfigProfile[]>('/configs'),
    get: (name: string) => request<ConfigProfile>(`/configs/${encodeURIComponent(name)}`),
    save: (name: string, content: string) =>
      request<ConfigProfile>('/configs', { method: 'POST', body: JSON.stringify({ name, content }) }),
    delete: (name: string) => request<{ deleted: string }>(`/configs/${encodeURIComponent(name)}`, { method: 'DELETE' }),
    apply: (service: string, profile: string) =>
      request<Record<string, unknown>>('/configs/apply', { method: 'POST', body: JSON.stringify({ service, profile }) }),
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
    generateBusinessData: (scenario: string, pointsPerSeries?: number, skipAutoDemote?: boolean) =>
      request<GenResult>('/test/generate-business-data', { method: 'POST', body: JSON.stringify({ scenario, points_per_series: pointsPerSeries || 60, skip_auto_demote: skipAutoDemote || false }) }),
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
    cfDetail: (name: string) => request<CfDetail>(`/rocksdb/cf-detail/${encodeURIComponent(name)}`),
    compact: (cf?: string) =>
      request<{ compacted: boolean }>('/rocksdb/compact', { method: 'POST', body: JSON.stringify({ cf: cf || null }) }),
    kvScan: (cf: string, prefix?: string, startKey?: string, limit?: number) =>
      request<KvScanResult>(`/rocksdb/kv-scan?${new URLSearchParams(Object.entries({ cf, prefix: prefix || '', start_key: startKey || '', limit: String(limit || 20) }).filter(([, v]) => v !== ''))}`),
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
    demoteToWarm: (cfNames: string[]) => request<{ demoted: string[] }>('/lifecycle/demote-to-warm', { method: 'POST', body: JSON.stringify({ cf_names: cfNames }) }),
    demoteToCold: (cfNames: string[]) => request<{ demoted: string[] }>('/lifecycle/demote-to-cold', { method: 'POST', body: JSON.stringify({ cf_names: cfNames }) }),
  },
  iceberg: {
    listTables: () => request<IcebergTableInfo[]>('/iceberg/tables'),
    createTable: (name: string, schema: { fields: { name: string; field_type: string; required: boolean }[] }, partitionType: string) =>
      request<{ message: string }>('/iceberg/tables', { method: 'POST', body: JSON.stringify({ name, schema, partition_type: partitionType }) }),
    tableDetail: (name: string) => request<IcebergTableDetail>(`/iceberg/tables/${encodeURIComponent(name)}`),
    dropTable: (name: string) => request<{ message: string }>(`/iceberg/tables/${encodeURIComponent(name)}`, { method: 'DELETE' }),
    append: (name: string, datapoints: Record<string, unknown>[]) =>
      request<{ message: string }>(`/iceberg/tables/${encodeURIComponent(name)}/append`, { method: 'POST', body: JSON.stringify({ datapoints }) }),
    scan: (name: string, limit?: number) => request<IcebergScanResult>(`/iceberg/tables/${encodeURIComponent(name)}/scan${limit ? `?limit=${limit}` : ''}`),
    snapshots: (name: string) => request<IcebergSnapshotInfo[]>(`/iceberg/tables/${encodeURIComponent(name)}/snapshots`),
    rollback: (name: string, snapshotId: number) =>
      request<{ message: string }>(`/iceberg/tables/${encodeURIComponent(name)}/rollback`, { method: 'POST', body: JSON.stringify({ snapshot_id: snapshotId }) }),
    compact: (name: string) => request<{ message: string }>(`/iceberg/tables/${encodeURIComponent(name)}/compact`, { method: 'POST' }),
    expire: (name: string, keepDays: number) =>
      request<{ message: string }>(`/iceberg/tables/${encodeURIComponent(name)}/expire`, { method: 'POST', body: JSON.stringify({ keep_days: keepDays }) }),
    updateSchema: (name: string, changes: Record<string, unknown>[]) =>
      request<{ message: string }>(`/iceberg/tables/${encodeURIComponent(name)}/schema`, { method: 'POST', body: JSON.stringify({ changes }) }),
  },
};

export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const [lastMessage, setLastMessage] = useState<MetricsSnapshot | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    let stopped = false;
    const connect = () => {
      if (stopped) return;
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
      ws.onclose = () => {
        if (!stopped) {
          reconnectTimerRef.current = setTimeout(connect, 3000);
        }
      };
      ws.onerror = () => {
        ws.close();
      };
      wsRef.current = ws;
    };
    connect();
    return () => {
      stopped = true;
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
      }
      wsRef.current?.close();
    };
  }, []);

  return { lastMessage };
}
