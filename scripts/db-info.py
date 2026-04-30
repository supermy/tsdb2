#!/usr/bin/env python3
import sys, json, subprocess

def curl_json(url):
    try:
        r = subprocess.run(['curl', '-sf', url], capture_output=True, text=True, timeout=5)
        return json.loads(r.stdout) if r.stdout else {}
    except:
        return {}

def main():
    if len(sys.argv) < 3:
        print("Usage: db-info.py <base_url> <rocksdb|parquet|lifecycle|schema>")
        sys.exit(1)

    base_url = sys.argv[1]
    cmd = sys.argv[2]

    if cmd == 'rocksdb':
        stats = curl_json(f'{base_url}/api/rocksdb/stats').get('data', {})

        print()
        print("── 总体统计 ──")
        if stats:
            print(f"  CF 总数:        {stats.get('total_cf_count', '?')}")
            print(f"  时序 CF 数:     {stats.get('ts_cf_count', '?')}")
            measurements = stats.get('measurements', [])
            print(f"  指标名称:       {', '.join(measurements) if measurements else '(无)'}")
            print(f"  SST 总大小:     {stats.get('total_sst_size_bytes', 0):,} bytes")
            print(f"  MemTable 大小:  {stats.get('total_memtable_bytes', 0):,} bytes")
            print(f"  Block Cache:    {stats.get('total_block_cache_bytes', 0):,} bytes")
            print(f"  总键数:         {stats.get('total_keys', 0):,}")
            print(f"  L0 文件数:      {stats.get('l0_file_count', 0)}")
            print(f"  待压缩:         {'是' if stats.get('compaction_pending', False) else '否'}")
        else:
            print("  ❌ 无法连接服务器，请先执行 'make start'")

        print()
        print("── Column Family 列表 ──")
        cfs = curl_json(f'{base_url}/api/rocksdb/cf-list').get('data', [])
        if isinstance(cfs, list) and len(cfs) > 0:
            for cf in cfs:
                if isinstance(cf, str):
                    print(f"  - {cf}")
        else:
            print("  (无 CF)")

        print()
        print("── 各 CF 详细信息 ──")
        if isinstance(cfs, list):
            ts_names = [n for n in cfs if isinstance(n, str) and n.startswith('ts_')]
            for n in ts_names[:10]:
                detail = curl_json(f'{base_url}/api/rocksdb/cf-detail/{n}').get('data', {})
                if detail:
                    print(f"  {n}:")
                    print(f"    键数: {detail.get('estimate_num_keys', 0):,}  SST大小: {detail.get('total_sst_file_size', 0):,} bytes")
                    levels = detail.get('num_files_at_level', [])
                    if levels:
                        print(f"    层级文件数: {levels}")
                else:
                    print(f"  {n}: (详细信息不可用)")

    elif cmd == 'parquet':
        print()
        print("── Parquet 文件概览 ──")
        files = curl_json(f'{base_url}/api/parquet/list').get('data', {})
        if isinstance(files, dict):
            for tier, items in files.items():
                count = len(items) if isinstance(items, list) else items
                print(f"  {tier}: {count}")
        elif isinstance(files, list):
            print(f"  文件数: {len(files)}")
            for f in files[:10]:
                if isinstance(f, dict):
                    print(f"    {f.get('path','?')}  rows={f.get('num_rows',0):,}  size={f.get('file_size',0):,}B  tier={f.get('tier','?')}")
        else:
            print("  (无 Parquet 文件)")

    elif cmd == 'lifecycle':
        print()
        print("── 数据生命周期 ──")
        status = curl_json(f'{base_url}/api/lifecycle/status').get('data', {})
        if status:
            engine = status.get('engine_type', '?')
            hot = status.get('hot_cfs', [])
            warm = status.get('warm_cfs', [])
            cold = status.get('cold_cfs', [])
            archive = status.get('archive_files', [])
            tier_label = '活跃' if engine == 'arrow' else '热'
            print(f"  引擎类型: {engine}")
            print(f"  {tier_label}数据: {len(hot)} 个分区  {status.get('total_hot_bytes', 0):,} bytes")
            for cf in hot[:8]:
                if isinstance(cf, dict):
                    name = cf.get('cf_name', cf.get('path', '?'))
                    keys = cf.get('num_keys', cf.get('file_count', '?'))
                    size = cf.get('sst_size', cf.get('total_size', 0))
                    age = cf.get('age_days', '?')
                    demote = cf.get('demote_eligible', 'none')
                    print(f"    {name:40s} keys={keys}  size={size:,}B  age={age}天  可降级={demote}")
            if len(hot) > 8:
                print(f"    ... 还有 {len(hot)-8} 个{tier_label}数据分区")

            print(f"  温数据:   {len(warm)} 个分区  {status.get('total_warm_bytes', 0):,} bytes")
            for cf in warm[:5]:
                if isinstance(cf, dict):
                    name = cf.get('cf_name', cf.get('path', '?'))
                    size = cf.get('sst_size', cf.get('total_size', 0))
                    print(f"    {name:40s} size={size:,}B")
            if len(warm) > 5:
                print(f"    ... 还有 {len(warm)-5} 个温数据分区")

            print(f"  冷数据:   {len(cold)} 个分区  {status.get('total_cold_bytes', 0):,} bytes")
            for cf in cold[:5]:
                if isinstance(cf, dict):
                    name = cf.get('cf_name', cf.get('path', '?'))
                    size = cf.get('sst_size', cf.get('total_size', 0))
                    print(f"    {name:40s} size={size:,}B")
            if len(cold) > 5:
                print(f"    ... 还有 {len(cold)-5} 个冷数据分区")

            print(f"  归档:     {len(archive)} 个文件  {status.get('total_archive_bytes', 0):,} bytes")
        else:
            print("  ❌ 无法连接服务器，请先执行 'make start'")

    elif cmd == 'schema':
        print()
        print("── Schema 信息 ──")
        schemas = curl_json(f'{base_url}/api/rocksdb/series-schema').get('data', {})
        if isinstance(schemas, dict):
            measurements = schemas.get('measurements', [])
            if measurements:
                for m in measurements:
                    if isinstance(m, dict):
                        name = m.get('name', '?')
                        cfs = m.get('column_families', [])
                        tag_keys = m.get('tag_keys', [])
                        fields = m.get('field_names', [])
                        series = m.get('series', [])
                        print(f"  指标: {name}")
                        print(f"    CF: {', '.join(cfs)}")
                        print(f"    Tag Keys: {', '.join(tag_keys)}")
                        print(f"    Fields: {', '.join(fields)}")
                        print(f"    Series 数量: {len(series)}")
                        print()
            else:
                print("  (无 Schema 数据)")
        else:
            print("  ❌ 无法连接服务器，请先执行 'make start'")

if __name__ == '__main__':
    main()
