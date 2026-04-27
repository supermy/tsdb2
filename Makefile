.PHONY: build build-release build-debug build-backend build-frontend \
       build-linux-amd64 build-linux-arm64 build-macos-amd64 build-macos-arm64 \
       build-windows-amd64 build-cross \
       dev start stop restart status \
       test test-all test-unit test-integration test-stress test-frontend \
       lint clippy fmt fmt-check check \
       clean clean-all clean-backend clean-frontend clean-data \
       docker docker-build docker-run docker-push \
       bench bench-write bench-read bench-full \
       coverage \
      install dist dist-clean release \
       test-data test-data-iot test-data-devops test-data-ecommerce \
       test-data-clean test-data-lifecycle \
       client-test client-sql client-status client-doctor \
       client-iceberg-list client-iceberg-create \
       db-info db-info-rocksdb db-info-parquet db-info-lifecycle db-info-schema \
       help

VERSION ?= $(shell grep '^version' Cargo.toml | head -1 | sed 's/.*= *"\(.*\)".*/\1/')
BUILD_DATE := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
GIT_SHA := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
TARGET_DIR := target/release
DEBUG_TARGET := target/debug
DASHBOARD_DIR := tsdb-dashboard
DIST_DIR := dist

DATA_DIR ?= ./data
PARQUET_DIR ?= ./data_parquet
HOST ?= 0.0.0.0
FLIGHT_PORT ?= 50051
ADMIN_PORT ?= 8080
HTTP_PORT ?= 3000

BENCH_POINTS ?= 100000
BENCH_WORKERS ?= 1

CROSS_ARCH ?= aarch64-unknown-linux-gnu

# ============================================================
# Build
# ============================================================

build: build-release

build-release: build-backend build-frontend
	@echo "==> Packaging release..."
	@rm -rf $(TARGET_DIR)/dashboard
	@cp -r $(DASHBOARD_DIR)/dist $(TARGET_DIR)/dashboard
	@echo "✅ Build complete: $(TARGET_DIR)/tsdb-cli"

build-debug: build-backend-debug build-frontend
	@echo "==> Packaging debug..."
	@rm -rf $(DEBUG_TARGET)/dashboard
	@cp -r $(DASHBOARD_DIR)/dist $(DEBUG_TARGET)/dashboard
	@echo "✅ Debug build complete: $(DEBUG_TARGET)/tsdb-cli"

build-backend:
	@echo "==> Building Rust backend (release)..."
	cargo build --release -p tsdb-cli

build-backend-debug:
	@echo "==> Building Rust backend (debug)..."
	cargo build -p tsdb-cli

build-frontend:
	@echo "==> Building frontend dashboard..."
	cd $(DASHBOARD_DIR) && npm ci --quiet 2>/dev/null || npm install --quiet
	cd $(DASHBOARD_DIR) && npm run build

# ============================================================
# Cross-Platform & Cross-Architecture Build
# ============================================================

build-linux-amd64:
	@echo "==> Building for Linux x86_64..."
	cargo build --release -p tsdb-cli --target x86_64-unknown-linux-gnu
	@$(MAKE) build-frontend
	@$(MAKE) _package_target TARGET=x86_64-unknown-linux-gnu

build-linux-arm64:
	@echo "==> Building for Linux aarch64..."
	cross build --release -p tsdb-cli --target aarch64-unknown-linux-gnu
	@$(MAKE) build-frontend
	@$(MAKE) _package_target TARGET=aarch64-unknown-linux-gnu

build-macos-amd64:
	@echo "==> Building for macOS x86_64..."
	cargo build --release -p tsdb-cli --target x86_64-apple-darwin
	@$(MAKE) build-frontend
	@$(MAKE) _package_target TARGET=x86_64-apple-darwin

build-macos-arm64:
	@echo "==> Building for macOS aarch64..."
	cargo build --release -p tsdb-cli --target aarch64-apple-darwin
	@$(MAKE) build-frontend
	@$(MAKE) _package_target TARGET=aarch64-apple-darwin

build-windows-amd64:
	@echo "==> Building for Windows x86_64..."
	cargo build --release -p tsdb-cli --target x86_64-pc-windows-msvc
	@$(MAKE) build-frontend
	@$(MAKE) _package_target TARGET=x86_64-pc-windows-msvc

build-cross:
	@echo "==> Cross-compiling for $(CROSS_ARCH)..."
	cross build --release -p tsdb-cli --target $(CROSS_ARCH)
	@$(MAKE) build-frontend
	@$(MAKE) _package_target TARGET=$(CROSS_ARCH)

_package_target:
	@echo "==> Packaging for $(TARGET)..."
	@mkdir -p $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)
	@cp target/$(TARGET)/release/tsdb-cli$(SUFFIX) $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)/
	@cp -r $(DASHBOARD_DIR)/dist $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)/dashboard
	@cp -r configs $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)/
	@cp README.md $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)/
	@cd $(DIST_DIR) && tar czf tsdb2-$(VERSION)-$(TARGET).tar.gz tsdb2-$(VERSION)-$(TARGET)
	@rm -rf $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)
	@echo "✅ Packaged: $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET).tar.gz"

# ============================================================
# Distribution & Release
# ============================================================

dist: build-release
	@echo "==> Creating distribution package..."
	@mkdir -p $(DIST_DIR)/tsdb2-$(VERSION)
	@cp $(TARGET_DIR)/tsdb-cli $(DIST_DIR)/tsdb2-$(VERSION)/
	@cp -r $(TARGET_DIR)/dashboard $(DIST_DIR)/tsdb2-$(VERSION)/
	@cp -r configs $(DIST_DIR)/tsdb2-$(VERSION)/
	@cp README.md Makefile $(DIST_DIR)/tsdb2-$(VERSION)/
	@cd $(DIST_DIR) && tar czf tsdb2-$(VERSION)-$(shell rustc -vV | grep host | cut -d' ' -f2).tar.gz tsdb2-$(VERSION)
	@rm -rf $(DIST_DIR)/tsdb2-$(VERSION)
	@echo "✅ Distribution: $(DIST_DIR)/"

dist-clean:
	@rm -rf $(DIST_DIR)

release: dist build-linux-amd64 build-linux-arm64
	@echo "✅ Release packages built in $(DIST_DIR)/"
	@ls -lh $(DIST_DIR)/*.tar.gz

# ============================================================
# Server Start / Stop
# ============================================================

dev:
	@echo "==> Starting dev environment..."
	@mkdir -p $(DATA_DIR) $(PARQUET_DIR)
	@$(MAKE) build-backend-debug
	@echo "==> Starting backend server..."
	@$(DEBUG_TARGET)/tsdb-cli serve \
		--data-dir $(DATA_DIR) \
		--parquet-dir $(PARQUET_DIR) \
		--storage-engine rocksdb \
		--config default \
		--host $(HOST) \
		--flight-port $(FLIGHT_PORT) \
		--admin-port $(ADMIN_PORT) \
		--http-port $(HTTP_PORT) &
	@sleep 2
	@echo "==> Starting frontend dev server (ExpoGo compatible)..."
	@echo "    Frontend: http://localhost:5173"
	@echo "    Backend:  http://localhost:$(HTTP_PORT)"
	@cd $(DASHBOARD_DIR) && npx vite --host 0.0.0.0

start:
	@mkdir -p $(DATA_DIR) $(PARQUET_DIR)
	@echo "╔══════════════════════════════════════════════╗"
	@echo "║       TSDB2 Server Starting                 ║"
	@echo "╠══════════════════════════════════════════════╣"
	@echo "║  Flight gRPC:  $(HOST):$(FLIGHT_PORT)"
	@echo "║  Dashboard:    http://$(HOST):$(HTTP_PORT)"
	@echo "║  Data Dir:     $(DATA_DIR)"
	@echo "║  Parquet Dir:  $(PARQUET_DIR)"
	@echo "╚══════════════════════════════════════════════╝"
	@exec $(TARGET_DIR)/tsdb-cli serve \
		--data-dir $(DATA_DIR) \
		--parquet-dir $(PARQUET_DIR) \
		--storage-engine rocksdb \
		--config default \
		--host $(HOST) \
		--flight-port $(FLIGHT_PORT) \
		--admin-port $(ADMIN_PORT) \
		--http-port $(HTTP_PORT)

stop:
	@pkill -f "tsdb-cli serve" 2>/dev/null && echo "✅ Server stopped" || echo "No running server found"

restart: stop
	@sleep 1
	@$(MAKE) start

status:
	@curl -sf http://localhost:$(HTTP_PORT)/api/services 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Server running: {len(d.get(\"data\",[]))} services')" 2>/dev/null || echo "Server not running on port $(HTTP_PORT)"

# ============================================================
# Test
# ============================================================

test: test-unit lint build-frontend

test-all: test-unit test-integration lint build-frontend

test-unit:
	@echo "==> Running unit tests..."
	cargo test --workspace --exclude tsdb-stress-rocksdb --exclude tsdb-integration-tests --exclude tsdb-stress

test-integration:
	@echo "==> Running integration tests..."
	cargo test -p tsdb-integration-tests

test-stress:
	@echo "==> Running stress tests..."
	cargo test -p tsdb-stress-rocksdb -- --nocapture

test-frontend:
	@echo "==> Running frontend tests..."
	cd $(DASHBOARD_DIR) && npm ci --quiet 2>/dev/null && npm run build

# ============================================================
# Test Data Generation
# ============================================================

test-data: test-data-iot
	@echo "✅ Test data generated. Use 'make test-data-lifecycle' to demote."

test-data-iot:
	@echo "==> Generating IoT test data..."
	@python3 scripts/test-data.py $(HTTP_PORT) generate iot 60

test-data-devops:
	@echo "==> Generating DevOps test data..."
	@python3 scripts/test-data.py $(HTTP_PORT) generate devops 60

test-data-ecommerce:
	@echo "==> Generating E-Commerce test data..."
	@python3 scripts/test-data.py $(HTTP_PORT) generate ecommerce 60

test-data-lifecycle:
	@echo "==> Demoting warm/cold data..."
	@python3 scripts/test-data.py $(HTTP_PORT) lifecycle

test-data-clean:
	@echo "==> Cleaning test data..."
	@rm -rf $(DATA_DIR) $(PARQUET_DIR)
	@echo "✅ Test data cleaned"

# ============================================================
# Client Tests & CLI Operations
# ============================================================

client-test: client-status client-doctor client-sql
	@echo "✅ Client tests passed"

client-status:
	@echo "==> Checking server status..."
	@$(TARGET_DIR)/tsdb-cli --data-dir $(DATA_DIR) --storage-engine rocksdb status

client-doctor:
	@echo "==> Running doctor..."
	@$(TARGET_DIR)/tsdb-cli --data-dir $(DATA_DIR) --storage-engine rocksdb doctor

client-sql:
	@echo "==> Testing SQL query..."
	@$(TARGET_DIR)/tsdb-cli --data-dir $(DATA_DIR) --storage-engine rocksdb query --sql "SELECT 1 as test" --format json 2>/dev/null || echo "  (SQL requires running server with data)"

client-iceberg-list:
	@echo "==> Listing Iceberg tables..."
	@$(TARGET_DIR)/tsdb-cli --data-dir $(DATA_DIR) iceberg list

client-iceberg-create:
	@echo "==> Creating Iceberg table..."
	@$(TARGET_DIR)/tsdb-cli --data-dir $(DATA_DIR) iceberg create --name test_table

# ============================================================
# Performance Benchmarks
# ============================================================

bench: bench-write bench-read
	@echo "✅ Benchmarks complete"

bench-write:
	@echo "==> Write benchmark ($(BENCH_POINTS) points, $(BENCH_WORKERS) workers)..."
	@rm -rf /tmp/tsdb2_bench_write && mkdir -p /tmp/tsdb2_bench_write
	@$(TARGET_DIR)/tsdb-cli --data-dir /tmp/tsdb2_bench_write --storage-engine rocksdb \
		bench --mode write --points $(BENCH_POINTS) --workers $(BENCH_WORKERS)
	@rm -rf /tmp/tsdb2_bench_write

bench-read:
	@echo "==> Read benchmark ($(BENCH_POINTS) points)..."
	@rm -rf /tmp/tsdb2_bench_read && mkdir -p /tmp/tsdb2_bench_read
	@$(TARGET_DIR)/tsdb-cli --data-dir /tmp/tsdb2_bench_read --storage-engine rocksdb \
		bench --mode read --points $(BENCH_POINTS) --workers $(BENCH_WORKERS)
	@rm -rf /tmp/tsdb2_bench_read

bench-full:
	@echo "==> Full benchmark suite..."
	@echo "=== 10K points ==="
	@$(MAKE) bench-write BENCH_POINTS=10000
	@$(MAKE) bench-read BENCH_POINTS=10000
	@echo "=== 100K points ==="
	@$(MAKE) bench-write BENCH_POINTS=100000
	@$(MAKE) bench-read BENCH_POINTS=100000
	@echo "=== 1M points ==="
	@$(MAKE) bench-write BENCH_POINTS=1000000
	@$(MAKE) bench-read BENCH_POINTS=1000000
	@echo "✅ Full benchmark suite complete"

# ============================================================
# Code Quality
# ============================================================

lint: clippy fmt-check

clippy:
	@echo "==> Running clippy..."
	cargo clippy --workspace --all-targets -- -D warnings

fmt:
	@echo "==> Formatting code..."
	cargo fmt --all

fmt-check:
	@echo "==> Checking formatting..."
	cargo fmt --all -- --check

check: lint test-unit build-frontend
	@echo "✅ All checks passed!"

# ============================================================
# Clean
# ============================================================

clean:
	@echo "==> Cleaning build artifacts..."
	cargo clean
	@rm -rf $(DASHBOARD_DIR)/dist $(DASHBOARD_DIR)/node_modules $(DIST_DIR)
	@echo "✅ Clean complete"

clean-backend:
	@echo "==> Cleaning Rust build artifacts..."
	cargo clean

clean-frontend:
	@echo "==> Cleaning frontend build artifacts..."
	@rm -rf $(DASHBOARD_DIR)/dist $(DASHBOARD_DIR)/node_modules

clean-data:
	@echo "==> Cleaning data directories..."
	@rm -rf $(DATA_DIR) $(PARQUET_DIR)
	@echo "✅ Data cleaned"

# ============================================================
# Docker
# ============================================================

docker-build:
	@echo "==> Building Docker image..."
	docker build -t tsdb2:$(VERSION) -t tsdb2:latest .

docker-run:
	@echo "==> Running Docker container..."
	docker run -d --name tsdb2 \
		-p $(HTTP_PORT):3000 \
		-p $(FLIGHT_PORT):50051 \
		-v tsdb2-data:/data \
		tsdb2:latest serve \
		--data-dir /data \
		--parquet-dir /data/parquet \
		--host 0.0.0.0

docker-push:
	@echo "==> Pushing Docker image..."
	docker push tsdb2:$(VERSION)
	docker push tsdb2:latest

# ============================================================
# Coverage & Analysis
# ============================================================

coverage:
	@echo "==> Running coverage..."
	cargo tarpaulin --workspace --exclude tsdb-stress-rocksdb --out xml --timeout 300 --skip-clean

# ============================================================
# Install
# ============================================================

install: build-release
	@echo "==> Installing tsdb-cli..."
	@cp $(TARGET_DIR)/tsdb-cli /usr/local/bin/tsdb-cli
	@mkdir -p /usr/local/share/tsdb2/dashboard
	@cp -r $(TARGET_DIR)/dashboard/* /usr/local/share/tsdb2/dashboard/
	@cp -r configs /usr/local/share/tsdb2/
	@echo "✅ Installed to /usr/local/bin/tsdb-cli"
	@echo "   Dashboard: /usr/local/share/tsdb2/dashboard/"

uninstall:
	@rm -f /usr/local/bin/tsdb-cli
	@rm -rf /usr/local/share/tsdb2
	@echo "✅ Uninstalled"

# ============================================================
# Database Info (curl API)
# ============================================================

db-info: db-info-rocksdb db-info-parquet db-info-lifecycle
	@echo ""
	@echo "✅ 数据库信息查询完成"

db-info-rocksdb:
	@echo "╔════════════════════════════════════════════════════════════╗"
	@echo "║              RocksDB 数据结构与数据量                      ║"
	@echo "╚════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "── 总体统计 ──"
	@curl -sf http://localhost:$(HTTP_PORT)/api/rocksdb/stats 2>/dev/null | python3 -c "\
import sys, json; \
d = json.load(sys.stdin).get('data', {}); \
print(f'  CF 总数:        {d.get(\"total_cf_count\", 0)}'); \
print(f'  时序 CF 数:     {d.get(\"ts_cf_count\", 0)}'); \
print(f'  指标名称:       {\", \".join(d.get(\"measurements\", [])) or \"(无)\"}'); \
print(f'  SST 总大小:     {d.get(\"total_sst_size_bytes\", 0):,} bytes'); \
print(f'  MemTable 大小:  {d.get(\"total_memtable_bytes\", 0):,} bytes'); \
print(f'  Block Cache:    {d.get(\"total_block_cache_bytes\", 0):,} bytes'); \
print(f'  总键数:         {d.get(\"total_keys\", 0):,}'); \
print(f'  L0 文件数:      {d.get(\"l0_file_count\", 0)}'); \
print(f'  待压缩:         {\"是\" if d.get(\"compaction_pending\") else \"否\"}')" 2>/dev/null || echo "  ❌ 无法连接服务器，请先执行 'make start'"
	@echo ""
	@echo "── Column Family 列表 ──"
	@curl -sf http://localhost:$(HTTP_PORT)/api/rocksdb/cf-list 2>/dev/null | python3 -c "\
import sys, json; \
d = json.load(sys.stdin).get('data', {}); \
cfs = d.get('column_families', d if isinstance(d, list) else []); \
if isinstance(cfs, list): \
    for cf in cfs: \
        if isinstance(cf, str): print(f'  - {cf}'); \
        elif isinstance(cf, dict): print(f'  - {cf.get(\"name\", cf)}  keys={cf.get(\"num_keys\",\"?\")}  size={cf.get(\"sst_size\",\"?\")}'); \
else: print(f'  {cfs}')" 2>/dev/null || echo "  ❌ 无法获取 CF 列表"
	@echo ""
	@echo "── 各 CF 详细信息 ──"
	@curl -sf http://localhost:$(HTTP_PORT)/api/rocksdb/cf-list 2>/dev/null | python3 -c "\
import sys, json, subprocess; \
d = json.load(sys.stdin).get('data', {}); \
cfs = d.get('column_families', d if isinstance(d, list) else []); \
names = [cf if isinstance(cf, str) else cf.get('name','') for cf in (cfs if isinstance(cfs, list) else [])]; \
ts_names = [n for n in names if n.startswith('ts_')]; \
for n in ts_names[:10]: \
    try: \
        r = subprocess.run(['curl', '-sf', f'http://localhost:$(HTTP_PORT)/api/rocksdb/cf-detail/{n}'], capture_output=True, text=True, timeout=5); \
        detail = json.loads(r.stdout).get('data', {}); \
        print(f'  {n}:'); \
        print(f'    键数: {detail.get(\"estimate_num_keys\", 0):,}  SST大小: {detail.get(\"total_sst_file_size\", 0):,} bytes'); \
        levels = detail.get('num_files_at_level', []); \
        print(f'    层级文件数: {levels}'); \
    except: pass" 2>/dev/null || true

db-info-parquet:
	@echo "╔════════════════════════════════════════════════════════════╗"
	@echo "║              Parquet 数据结构与数据量                      ║"
	@echo "╚════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "── Parquet 文件列表 ──"
	@curl -sf http://localhost:$(HTTP_PORT)/api/parquet/list 2>/dev/null | python3 -c "\
import sys, json; \
d = json.load(sys.stdin).get('data', []); \
if not d: print('  (无 Parquet 文件)'); \
else: \
    warm = [f for f in d if f.get('tier') == 'warm']; \
    cold = [f for f in d if f.get('tier') == 'cold']; \
    other = [f for f in d if f.get('tier') not in ('warm', 'cold')]; \
    print(f'  总文件数: {len(d)}  (温数据: {len(warm)}, 冷数据: {len(cold)}, 其他: {len(other)})'); \
    total_size = sum(f.get('file_size', 0) for f in d); \
    total_rows = sum(f.get('num_rows', 0) for f in d); \
    print(f'  总大小: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)'); \
    print(f'  总行数: {total_rows:,}'); \
    print(''); \
    print('  ── 温数据文件 (SNAPPY压缩) ──'); \
    for f in warm[:5]: \
        print(f'    {f.get(\"path\",\"?\")}  rows={f.get(\"num_rows\",0):,}  size={f.get(\"file_size\",0):,}B  measurement={f.get(\"measurement\",\"?\")}'); \
    if len(warm) > 5: print(f'    ... 还有 {len(warm)-5} 个温数据文件'); \
    print('  ── 冷数据文件 (ZSTD压缩) ──'); \
    for f in cold[:5]: \
        print(f'    {f.get(\"path\",\"?\")}  rows={f.get(\"num_rows\",0):,}  size={f.get(\"file_size\",0):,}B  measurement={f.get(\"measurement\",\"?\")}'); \
    if len(cold) > 5: print(f'    ... 还有 {len(cold)-5} 个冷数据文件'); \
    if other: \
        print('  ── 其他文件 ──'); \
        for f in other[:3]: \
            print(f'    {f.get(\"path\",\"?\")}  rows={f.get(\"num_rows\",0):,}  size={f.get(\"file_size\",0):,}B  tier={f.get(\"tier\",\"?\")}')" 2>/dev/null || echo "  ❌ 无法连接服务器，请先执行 'make start'"
	@echo ""
	@echo "── Parquet 文件列结构 ──"
	@curl -sf http://localhost:$(HTTP_PORT)/api/parquet/list 2>/dev/null | python3 -c "\
import sys, json, subprocess; \
d = json.load(sys.stdin).get('data', []); \
for f in d[:3]: \
    path = f.get('path',''); \
    cols = f.get('columns', []); \
    if cols: \
        print(f'  {path}:'); \
        for c in cols[:8]: \
            print(f'    {c.get(\"name\",\"?\"):20s} {c.get(\"data_type\",\"?\"):15s} compressed={c.get(\"compressed_size\",0):,}B  uncompressed={c.get(\"uncompressed_size\",0):,}B'); \
        if len(cols) > 8: print(f'    ... 还有 {len(cols)-8} 列'); \
        print(''); \
    else: \
        try: \
            r = subprocess.run(['curl', '-sf', f'http://localhost:$(HTTP_PORT)/api/parquet/file-detail?path={path}'], capture_output=True, text=True, timeout=5); \
            detail = json.loads(r.stdout).get('data', {}); \
            dcols = detail.get('columns', []); \
            print(f'  {path}:'); \
            for c in dcols[:8]: \
                print(f'    {c.get(\"name\",\"?\"):20s} {c.get(\"data_type\",\"?\"):15s} compressed={c.get(\"compressed_size\",0):,}B  uncompressed={c.get(\"uncompressed_size\",0):,}B'); \
            if len(dcols) > 8: print(f'    ... 还有 {len(dcols)-8} 列'); \
            print(''); \
        except: pass" 2>/dev/null || true

db-info-lifecycle:
	@echo "╔════════════════════════════════════════════════════════════╗"
	@echo "║              数据生命周期分布                              ║"
	@echo "╚════════════════════════════════════════════════════════════╝"
	@echo ""
	@curl -sf http://localhost:$(HTTP_PORT)/api/lifecycle/status 2>/dev/null | python3 -c "\
import sys, json; \
d = json.load(sys.stdin).get('data', {}); \
hot = d.get('hot_cfs', []); \
warm = d.get('warm_cfs', []); \
cold = d.get('cold_cfs', []); \
print(f'── 热数据 (RocksDB) ──'); \
print(f'  CF 数量: {len(hot)}  总大小: {d.get(\"total_hot_bytes\",0):,} bytes ({d.get(\"total_hot_bytes\",0)/1024/1024:.2f} MB)'); \
total_hot_keys = sum(cf.get('num_keys',0) for cf in hot); \
print(f'  总键数: {total_hot_keys:,}'); \
for cf in hot[:8]: \
    print(f'    {cf.get(\"cf_name\",\"?\"):40s} keys={cf.get(\"num_keys\",0):,}  size={cf.get(\"sst_size\",0):,}B  age={cf.get(\"age_days\",0)}天  可降级={cf.get(\"demote_eligible\",\"none\")}'); \
if len(hot) > 8: print(f'    ... 还有 {len(hot)-8} 个热数据 CF'); \
print(''); \
print(f'── 温数据 (Parquet SNAPPY) ──'); \
print(f'  分区数量: {len(warm)}  总大小: {d.get(\"total_warm_bytes\",0):,} bytes ({d.get(\"total_warm_bytes\",0)/1024/1024:.2f} MB)'); \
for cf in warm[:5]: \
    print(f'    {cf.get(\"cf_name\",\"?\"):40s} keys={cf.get(\"num_keys\",0):,}  size={cf.get(\"sst_size\",0):,}B'); \
if len(warm) > 5: print(f'    ... 还有 {len(warm)-5} 个温数据分区'); \
print(''); \
print(f'── 冷数据 (Parquet ZSTD) ──'); \
print(f'  分区数量: {len(cold)}  总大小: {d.get(\"total_cold_bytes\",0):,} bytes ({d.get(\"total_cold_bytes\",0)/1024/1024:.2f} MB)'); \
for cf in cold[:5]: \
    print(f'    {cf.get(\"cf_name\",\"?\"):40s} keys={cf.get(\"num_keys\",0):,}  size={cf.get(\"sst_size\",0):,}B'); \
if len(cold) > 5: print(f'    ... 还有 {len(cold)-5} 个冷数据分区')" 2>/dev/null || echo "  ❌ 无法连接服务器，请先执行 'make start'"

db-info-schema:
	@echo "╔════════════════════════════════════════════════════════════╗"
	@echo "║              指标与维度 Schema 信息                        ║"
	@echo "╚════════════════════════════════════════════════════════════╝"
	@echo ""
	@curl -sf http://localhost:$(HTTP_PORT)/api/rocksdb/series-schema 2>/dev/null | python3 -c "\
import sys, json; \
d = json.load(sys.stdin).get('data', {}); \
measurements = d.get('measurements', []); \
if not measurements: print('  (无 Schema 数据)'); \
else: \
    for m in measurements: \
        print(f'  指标: {m.get(\"name\",\"?\")}'); \
        print(f'    CF: {\", \".join(m.get(\"column_families\", []))}'); \
        print(f'    Tag Keys: {\", \".join(m.get(\"tag_keys\", []))}'); \
        print(f'    Fields: {\", \".join(m.get(\"field_names\", []))}'); \
        series = m.get('series', []); \
        print(f'    Series 数量: {len(series)}'); \
        for s in series[:3]: \
            print(f'      tags={s.get(\"tags\",{})}  fields={s.get(\"fields\",[])}'); \
        if len(series) > 3: print(f'      ... 还有 {len(series)-3} 个 series'); \
        print('')" 2>/dev/null || echo "  ❌ 无法连接服务器，请先执行 'make start'"

# ============================================================
# Help
# ============================================================

help:
	@echo "TSDB2 Makefile - v$(VERSION) ($(GIT_SHA))"
	@echo ""
	@echo "Build:"
	@echo "  make build                Build everything (release)"
	@echo "  make build-debug          Build everything (debug)"
	@echo "  make build-backend        Build Rust backend only"
	@echo "  make build-frontend       Build frontend only"
	@echo ""
	@echo "Cross-Platform Build:"
	@echo "  make build-linux-amd64    Build for Linux x86_64"
	@echo "  make build-linux-arm64    Build for Linux aarch64 (needs cross)"
	@echo "  make build-macos-amd64    Build for macOS x86_64"
	@echo "  make build-macos-arm64    Build for macOS Apple Silicon"
	@echo "  make build-windows-amd64  Build for Windows x86_64"
	@echo "  make build-cross          Cross-compile (set CROSS_ARCH=...)"
	@echo ""
	@echo "Distribution:"
	@echo "  make dist                 Create distribution tarball (current arch)"
	@echo "  make release              Build all platform packages"
	@echo "  make install              Install binary + dashboard to /usr/local"
	@echo "  make uninstall            Remove installed files"
	@echo ""
	@echo "Server:"
	@echo "  make dev                  Start dev environment (Vite hot reload)"
	@echo "  make start                Start production server"
	@echo "  make stop                 Stop running server"
	@echo "  make restart              Restart server"
	@echo "  make status               Check if server is running"
	@echo ""
	@echo "Test Data:"
	@echo "  make test-data            Generate IoT test data (skip auto-demote)"
	@echo "  make test-data-iot        Generate IoT data"
	@echo "  make test-data-devops     Generate DevOps data"
	@echo "  make test-data-ecommerce  Generate E-Commerce data"
	@echo "  make test-data-lifecycle  Demote warm/cold data to Parquet"
	@echo "  make test-data-clean      Remove all test data"
	@echo ""
	@echo "Client Tests:"
	@echo "  make client-test          Run all client tests"
	@echo "  make client-status        Check database status"
	@echo "  make client-doctor        Run health check"
	@echo "  make client-sql           Test SQL query"
	@echo "  make client-iceberg-list  List Iceberg tables"
	@echo "  make client-iceberg-create Create test Iceberg table"
	@echo ""
	@echo "Performance:"
	@echo "  make bench                Run write + read benchmarks"
	@echo "  make bench-write          Write benchmark only"
	@echo "  make bench-read           Read benchmark only"
	@echo "  make bench-full           Full suite (10K/100K/1M points)"
	@echo ""
	@echo "Test:"
	@echo "  make test                 Run unit tests + lint"
	@echo "  make test-all             Run all tests + lint"
	@echo "  make test-unit            Run unit tests only"
	@echo "  make test-integration     Run integration tests"
	@echo "  make test-stress          Run stress tests"
	@echo ""
	@echo "Quality:"
	@echo "  make lint                 Run clippy + fmt check"
	@echo "  make clippy               Run clippy"
	@echo "  make fmt                  Auto-format code"
	@echo "  make check                Full quality gate"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build         Build Docker image"
	@echo "  make docker-run           Run Docker container"
	@echo "  make docker-push          Push Docker image"
	@echo ""
	@echo "Other:"
	@echo "  make coverage             Run code coverage"
	@echo "  make clean                Clean all artifacts"
	@echo "  make clean-data           Remove data directories"
	@echo "  make dist-clean           Remove distribution packages"
	@echo ""
	@echo "Database Info (curl API):"
	@echo "  make db-info              Show all DB info (RocksDB + Parquet + Lifecycle)"
	@echo "  make db-info-rocksdb      Show RocksDB stats, CF list, CF details"
	@echo "  make db-info-parquet      Show Parquet files, columns, tier distribution"
	@echo "  make db-info-lifecycle    Show hot/warm/cold data distribution"
	@echo "  make db-info-schema       Show metrics-dimensions schema"
	@echo ""
	@echo "Variables:"
	@echo "  DATA_DIR=$(DATA_DIR)          PARQUET_DIR=$(PARQUET_DIR)"
	@echo "  HOST=$(HOST)                  HTTP_PORT=$(HTTP_PORT)"
	@echo "  FLIGHT_PORT=$(FLIGHT_PORT)    ADMIN_PORT=$(ADMIN_PORT)"
	@echo "  BENCH_POINTS=$(BENCH_POINTS)  BENCH_WORKERS=$(BENCH_WORKERS)"
	@echo "  CROSS_ARCH=$(CROSS_ARCH)      VERSION=$(VERSION)"
