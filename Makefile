.PHONY: build build-release build-debug build-backend build-frontend \
       build-linux-amd64 build-linux-arm64 build-macos-amd64 build-macos-arm64 \
       build-windows-amd64 build-cross \
       dev dev-arrow start start-arrow stop restart status \
       start-server start-server-arrow stop-server server-status \
       test test-all test-unit test-integration test-stress test-frontend \
       test-arrow test-arrow-unit test-arrow-e2e \
       tdd tdd-arrow \
       lint clippy fmt fmt-check check \
       clean clean-backend clean-frontend clean-data \
       docker-build docker-run docker-push \
       bench bench-write bench-read bench-full bench-arrow \
       coverage coverage-arrow \
       install dist dist-clean release uninstall tag \
       test-data test-data-iot test-data-devops test-data-ecommerce \
       test-data-clean test-data-lifecycle \
       client-test client-sql client-status client-doctor \
       client-iceberg-list client-iceberg-create \
       db-info db-info-rocksdb db-info-parquet db-info-lifecycle db-info-schema \
       security deny \
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
LOG_DIR ?= ./logs
LOG_LEVEL ?= info

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
	@echo "✅ Build complete: $(TARGET_DIR)/tsdb-cli + tsdb-server"

build-debug: build-backend-debug build-frontend
	@echo "==> Packaging debug..."
	@rm -rf $(DEBUG_TARGET)/dashboard
	@cp -r $(DASHBOARD_DIR)/dist $(DEBUG_TARGET)/dashboard
	@echo "✅ Debug build complete: $(DEBUG_TARGET)/tsdb-cli + tsdb-server"

build-backend:
	@echo "==> Building Rust backend (release)..."
	cargo build --release -p tsdb-cli --bin tsdb-cli --bin tsdb-server

build-backend-debug:
	@echo "==> Building Rust backend (debug)..."
	cargo build -p tsdb-cli --bin tsdb-cli --bin tsdb-server

build-frontend:
	@echo "==> Building frontend dashboard..."
	cd $(DASHBOARD_DIR) && npm ci --quiet 2>/dev/null || npm install --quiet
	cd $(DASHBOARD_DIR) && npm run build

# ============================================================
# Cross-Platform & Cross-Architecture Build
# ============================================================

build-linux-amd64:
	@echo "==> Building for Linux x86_64..."
	cargo build --release -p tsdb-cli --bin tsdb-cli --bin tsdb-server --target x86_64-unknown-linux-gnu
	@$(MAKE) build-frontend
	@$(MAKE) _package_target TARGET=x86_64-unknown-linux-gnu

build-linux-arm64:
	@echo "==> Building for Linux aarch64..."
	cross build --release -p tsdb-cli --bin tsdb-cli --bin tsdb-server --target aarch64-unknown-linux-gnu
	@$(MAKE) _package_target TARGET=aarch64-unknown-linux-gnu

build-macos-amd64:
	@echo "==> Building for macOS x86_64..."
	cargo build --release -p tsdb-cli --bin tsdb-cli --bin tsdb-server --target x86_64-apple-darwin
	@$(MAKE) _package_target TARGET=x86_64-apple-darwin

build-macos-arm64:
	@echo "==> Building for macOS aarch64..."
	cargo build --release -p tsdb-cli --bin tsdb-cli --bin tsdb-server --target aarch64-apple-darwin
	@$(MAKE) _package_target TARGET=aarch64-apple-darwin

build-windows-amd64:
	@echo "==> Building for Windows x86_64..."
	cargo build --release -p tsdb-cli --bin tsdb-cli --bin tsdb-server --target x86_64-pc-windows-msvc
	@$(MAKE) _package_target TARGET=x86_64-pc-windows-msvc SUFFIX=.exe

build-cross:
	@echo "==> Cross-compiling for $(CROSS_ARCH)..."
	cross build --release -p tsdb-cli --bin tsdb-cli --bin tsdb-server --target $(CROSS_ARCH)
	@$(MAKE) _package_target TARGET=$(CROSS_ARCH)

_package_target:
	@echo "==> Packaging for $(TARGET)..."
	@mkdir -p $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)
	@cp target/$(TARGET)/release/tsdb-cli$(SUFFIX) $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)/
	@cp target/$(TARGET)/release/tsdb-server$(SUFFIX) $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)/
	@cp -r $(DASHBOARD_DIR)/dist $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)/dashboard
	@cp -r configs $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)/
	@cp README.md $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)/
	@if echo "$(TARGET)" | grep -q "windows"; then \
		cd $(DIST_DIR) && zip -r tsdb2-$(VERSION)-$(TARGET).zip tsdb2-$(VERSION)-$(TARGET); \
	else \
		cd $(DIST_DIR) && tar czf tsdb2-$(VERSION)-$(TARGET).tar.gz tsdb2-$(VERSION)-$(TARGET); \
	fi
	@rm -rf $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)
	@echo "✅ Packaged: $(DIST_DIR)/tsdb2-$(VERSION)-$(TARGET)$(shell echo $(TARGET) | grep -q windows && echo .zip || echo .tar.gz)"

# ============================================================
# Distribution & Release
# ============================================================

dist: build-release
	@echo "==> Creating distribution package..."
	@mkdir -p $(DIST_DIR)/tsdb2-$(VERSION)
	@cp $(TARGET_DIR)/tsdb-cli $(DIST_DIR)/tsdb2-$(VERSION)/
	@cp $(TARGET_DIR)/tsdb-server $(DIST_DIR)/tsdb2-$(VERSION)/
	@cp -r $(TARGET_DIR)/dashboard $(DIST_DIR)/tsdb2-$(VERSION)/
	@cp -r configs $(DIST_DIR)/tsdb2-$(VERSION)/
	@cp README.md Makefile $(DIST_DIR)/tsdb2-$(VERSION)/
	@cd $(DIST_DIR) && tar czf tsdb2-$(VERSION)-$(shell rustc -vV | grep host | cut -d' ' -f2).tar.gz tsdb2-$(VERSION)
	@rm -rf $(DIST_DIR)/tsdb2-$(VERSION)
	@echo "✅ Distribution: $(DIST_DIR)/"

dist-clean:
	@rm -rf $(DIST_DIR)

release: build-frontend dist build-linux-amd64 build-linux-arm64 build-macos-amd64 build-macos-arm64 build-windows-amd64
	@echo "✅ Release packages built in $(DIST_DIR)/"
	@ls -lh $(DIST_DIR)/*.tar.gz $(DIST_DIR)/*.zip 2>/dev/null || true

tag:
	@if [ -z "$(VERSION)" ]; then echo "❌ VERSION not set"; exit 1; fi
	@git tag -a v$(VERSION) -m "TSDB2 v$(VERSION)"
	@git push origin v$(VERSION)
	@echo "✅ Tagged and pushed v$(VERSION)"

# ============================================================
# Server Start / Stop
# ============================================================

dev:
	@echo "==> Starting dev environment (RocksDB)..."
	@mkdir -p $(DATA_DIR) $(PARQUET_DIR) $(LOG_DIR)
	@$(MAKE) build-backend-debug
	@echo "==> Starting backend server..."
	@$(DEBUG_TARGET)/tsdb-server \
		--data-dir $(DATA_DIR) \
		--parquet-dir $(PARQUET_DIR) \
		--storage-engine rocksdb \
		--config default \
		--host $(HOST) \
		--flight-port $(FLIGHT_PORT) \
		--admin-port $(ADMIN_PORT) \
		--http-port $(HTTP_PORT) \
		--log-dir $(LOG_DIR) \
		--log-level debug &
	@sleep 2
	@echo "==> Starting frontend dev server (ExpoGo compatible)..."
	@echo "    Frontend: http://localhost:5173"
	@echo "    Backend:  http://localhost:$(HTTP_PORT)"
	@cd $(DASHBOARD_DIR) && npx vite --host 0.0.0.0

dev-arrow:
	@echo "==> Starting dev environment (Arrow)..."
	@mkdir -p $(DATA_DIR) $(PARQUET_DIR) $(LOG_DIR)
	@$(MAKE) build-backend-debug
	@echo "==> Starting backend server (Arrow engine)..."
	@$(DEBUG_TARGET)/tsdb-server \
		--data-dir $(DATA_DIR) \
		--parquet-dir $(PARQUET_DIR) \
		--storage-engine arrow \
		--config default \
		--host $(HOST) \
		--flight-port $(FLIGHT_PORT) \
		--admin-port $(ADMIN_PORT) \
		--http-port $(HTTP_PORT) \
		--log-dir $(LOG_DIR) \
		--log-level debug &
	@sleep 2
	@echo "==> Starting frontend dev server..."
	@echo "    Frontend: http://localhost:5173"
	@echo "    Backend:  http://localhost:$(HTTP_PORT)"
	@cd $(DASHBOARD_DIR) && npx vite --host 0.0.0.0

start:
	@mkdir -p $(DATA_DIR) $(PARQUET_DIR) $(LOG_DIR)
	@echo "╔══════════════════════════════════════════════╗"
	@echo "║       TSDB2 Server Starting (RocksDB)       ║"
	@echo "╠══════════════════════════════════════════════╣"
	@echo "║  Flight gRPC:  $(HOST):$(FLIGHT_PORT)"
	@echo "║  Dashboard:    http://$(HOST):$(HTTP_PORT)"
	@echo "║  Data Dir:     $(DATA_DIR)"
	@echo "║  Parquet Dir:  $(PARQUET_DIR)"
	@echo "║  Log Dir:      $(LOG_DIR)"
	@echo "╚══════════════════════════════════════════════╝"
	@exec $(TARGET_DIR)/tsdb-server \
		--data-dir $(DATA_DIR) \
		--parquet-dir $(PARQUET_DIR) \
		--storage-engine rocksdb \
		--config default \
		--host $(HOST) \
		--flight-port $(FLIGHT_PORT) \
		--admin-port $(ADMIN_PORT) \
		--http-port $(HTTP_PORT) \
		--log-dir $(LOG_DIR) \
		--log-level $(LOG_LEVEL)

start-arrow:
	@mkdir -p $(DATA_DIR) $(PARQUET_DIR) $(LOG_DIR)
	@echo "╔══════════════════════════════════════════════╗"
	@echo "║       TSDB2 Server Starting (Arrow)         ║"
	@echo "╠══════════════════════════════════════════════╣"
	@echo "║  Flight gRPC:  $(HOST):$(FLIGHT_PORT)"
	@echo "║  Dashboard:    http://$(HOST):$(HTTP_PORT)"
	@echo "║  Data Dir:     $(DATA_DIR)"
	@echo "║  Parquet Dir:  $(PARQUET_DIR)"
	@echo "║  Log Dir:      $(LOG_DIR)"
	@echo "╚══════════════════════════════════════════════╝"
	@exec $(TARGET_DIR)/tsdb-server \
		--data-dir $(DATA_DIR) \
		--parquet-dir $(PARQUET_DIR) \
		--storage-engine arrow \
		--config default \
		--host $(HOST) \
		--flight-port $(FLIGHT_PORT) \
		--admin-port $(ADMIN_PORT) \
		--http-port $(HTTP_PORT) \
		--log-dir $(LOG_DIR) \
		--log-level $(LOG_LEVEL)

stop:
	@if [ -f /tmp/tsdb-server.pid ]; then \
		PID=$$(cat /tmp/tsdb-server.pid); \
		kill $$PID 2>/dev/null; \
		rm -f /tmp/tsdb-server.pid; \
	fi
	@pkill -f "tsdb-server" 2>/dev/null; pkill -f "tsdb-cli serve" 2>/dev/null; echo "✅ Server stopped" || echo "No running server found"

start-server:
	@mkdir -p $(DATA_DIR) $(PARQUET_DIR) $(LOG_DIR)
	@echo "Starting tsdb-server in background..."
	@$(TARGET_DIR)/tsdb-server \
		--data-dir $(DATA_DIR) \
		--parquet-dir $(PARQUET_DIR) \
		--storage-engine rocksdb \
		--config default \
		--host $(HOST) \
		--flight-port $(FLIGHT_PORT) \
		--admin-port $(ADMIN_PORT) \
		--http-port $(HTTP_PORT) \
		--log-dir $(LOG_DIR) \
		--log-level $(LOG_LEVEL) \
		-b

start-server-arrow:
	@mkdir -p $(DATA_DIR) $(PARQUET_DIR) $(LOG_DIR)
	@echo "Starting tsdb-server (Arrow) in background..."
	@$(TARGET_DIR)/tsdb-server \
		--data-dir $(DATA_DIR) \
		--parquet-dir $(PARQUET_DIR) \
		--storage-engine arrow \
		--config default \
		--host $(HOST) \
		--flight-port $(FLIGHT_PORT) \
		--admin-port $(ADMIN_PORT) \
		--http-port $(HTTP_PORT) \
		--log-dir $(LOG_DIR) \
		--log-level $(LOG_LEVEL) \
		-b

stop-server:
	@if [ -f /tmp/tsdb-server.pid ]; then \
		PID=$$(cat /tmp/tsdb-server.pid); \
		kill $$PID 2>/dev/null && echo "✅ tsdb-server (PID: $$PID) stopped" || echo "Process $$PID not found"; \
		rm -f /tmp/tsdb-server.pid; \
	else \
		pkill -f "tsdb-server" 2>/dev/null && echo "✅ tsdb-server stopped" || echo "No running tsdb-server found"; \
	fi

server-status:
	@if [ -f /tmp/tsdb-server.pid ]; then \
		PID=$$(cat /tmp/tsdb-server.pid); \
		if kill -0 $$PID 2>/dev/null; then \
			echo "✅ tsdb-server is running (PID: $$PID)"; \
		else \
			echo "❌ tsdb-server PID file exists but process $$PID is not running"; \
			rm -f /tmp/tsdb-server.pid; \
		fi \
	else \
		echo "❌ tsdb-server is not running (no PID file)"; \
	fi

restart: stop
	@sleep 1
	@$(MAKE) start

status:
	@curl -sf http://localhost:$(HTTP_PORT)/api/services 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Server running: {len(d.get(\"data\",[]))} services')" 2>/dev/null || echo "Server not running on port $(HTTP_PORT)"

# ============================================================
# Test
# ============================================================

test: test-unit lint build-frontend

test-all: test-unit test-integration test-arrow lint build-frontend

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
	cd $(DASHBOARD_DIR) && npm ci --quiet 2>/dev/null && npm run build && npx tsc --noEmit 2>/dev/null || true

test-arrow:
	@echo "==> Running Arrow engine tests..."
	cargo test --lib -p tsdb-arrow -p tsdb-parquet -p tsdb-storage-arrow

test-arrow-unit:
	@echo "==> Running Arrow engine unit tests (nextest)..."
	cargo nextest run --lib -p tsdb-arrow -p tsdb-parquet -p tsdb-storage-arrow 2>/dev/null || cargo test --lib -p tsdb-arrow -p tsdb-parquet -p tsdb-storage-arrow

test-arrow-e2e:
	@echo "==> Running Arrow engine E2E tests..."
	@rm -rf /tmp/tsdb2_arrow_e2e && mkdir -p /tmp/tsdb2_arrow_e2e /tmp/tsdb2_arrow_e2e_parquet /tmp/tsdb2_arrow_e2e_logs
	@$(MAKE) build-backend-debug
	@$(DEBUG_TARGET)/tsdb-server \
		--data-dir /tmp/tsdb2_arrow_e2e \
		--parquet-dir /tmp/tsdb2_arrow_e2e_parquet \
		--storage-engine arrow \
		--host $(HOST) \
		--flight-port $(FLIGHT_PORT) \
		--admin-port $(ADMIN_PORT) \
		--http-port $(HTTP_PORT) \
		--log-dir /tmp/tsdb2_arrow_e2e_logs \
		--log-level debug &
	@sleep 3
	@curl -sf http://localhost:$(HTTP_PORT)/api/services 2>/dev/null && echo "Arrow server ready" || echo "Arrow server not ready"
	@$(MAKE) stop
	@rm -rf /tmp/tsdb2_arrow_e2e /tmp/tsdb2_arrow_e2e_parquet /tmp/tsdb2_arrow_e2e_logs

tdd:
	@echo "==> TDD mode: watching and testing (all)..."
	cargo watch -x 'test --workspace --exclude tsdb-stress-rocksdb --exclude tsdb-integration-tests --exclude tsdb-stress' 2>/dev/null || \
		cargo test --workspace --exclude tsdb-stress-rocksdb --exclude tsdb-integration-tests --exclude tsdb-stress

tdd-arrow:
	@echo "==> TDD mode: watching and testing (Arrow engine)..."
	cargo watch -x 'test --lib -p tsdb-arrow -p tsdb-parquet -p tsdb-storage-arrow' 2>/dev/null || \
		cargo test --lib -p tsdb-arrow -p tsdb-parquet -p tsdb-storage-arrow

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
	@rm -rf $(DATA_DIR) $(PARQUET_DIR) $(LOG_DIR)
	@echo "✅ Test data and logs cleaned"

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

bench-arrow:
	@echo "==> Arrow engine benchmark ($(BENCH_POINTS) points)..."
	@rm -rf /tmp/tsdb2_bench_arrow && mkdir -p /tmp/tsdb2_bench_arrow
	@$(TARGET_DIR)/tsdb-cli --data-dir /tmp/tsdb2_bench_arrow --storage-engine arrow \
		bench --mode write --points $(BENCH_POINTS) --workers $(BENCH_WORKERS)
	@$(TARGET_DIR)/tsdb-cli --data-dir /tmp/tsdb2_bench_arrow --storage-engine arrow \
		bench --mode read --points $(BENCH_POINTS) --workers $(BENCH_WORKERS)
	@rm -rf /tmp/tsdb2_bench_arrow

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
	@rm -rf $(DATA_DIR) $(PARQUET_DIR) $(LOG_DIR)
	@echo "✅ Data and logs cleaned"

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
		-v tsdb2-logs:/logs \
		tsdb2:latest tsdb-server \
		--data-dir /data \
		--parquet-dir /data/parquet \
		--host 0.0.0.0 \
		--log-dir /logs \
		--log-level $(LOG_LEVEL)

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

coverage-arrow:
	@echo "==> Running Arrow engine coverage..."
	cargo tarpaulin -p tsdb-arrow -p tsdb-parquet -p tsdb-storage-arrow --out xml --timeout 300 --skip-clean

security:
	@echo "==> Running security audit..."
	cargo deny check 2>/dev/null || cargo audit 2>/dev/null || echo "⚠️  Install cargo-deny: cargo install cargo-deny"

deny: security

# ============================================================
# Install
# ============================================================

install: build-release
	@echo "==> Installing tsdb-cli and tsdb-server..."
	@cp $(TARGET_DIR)/tsdb-cli /usr/local/bin/tsdb-cli
	@cp $(TARGET_DIR)/tsdb-server /usr/local/bin/tsdb-server
	@mkdir -p /usr/local/share/tsdb2/dashboard
	@cp -r $(TARGET_DIR)/dashboard/* /usr/local/share/tsdb2/dashboard/
	@cp -r configs /usr/local/share/tsdb2/
	@echo "✅ Installed to /usr/local/bin/"
	@echo "   tsdb-cli:    /usr/local/bin/tsdb-cli"
	@echo "   tsdb-server: /usr/local/bin/tsdb-server"
	@echo "   Dashboard:   /usr/local/share/tsdb2/dashboard/"

uninstall:
	@rm -f /usr/local/bin/tsdb-cli /usr/local/bin/tsdb-server
	@rm -rf /usr/local/share/tsdb2
	@echo "✅ Uninstalled"

# ============================================================
# Database Info (curl API)
# ============================================================

db-info: db-info-rocksdb db-info-lifecycle db-info-schema


db-info-rocksdb:
	@echo "╔════════════════════════════════════════════════════════════╗"
	@echo "║              RocksDB 数据结构与数据量                      ║"
	@echo "╚════════════════════════════════════════════════════════════╝"
	@python3 scripts/db-info.py http://localhost:$(HTTP_PORT) rocksdb

db-info-parquet:
	@echo "╔════════════════════════════════════════════════════════════╗"
	@echo "║              Parquet 数据文件概览                          ║"
	@echo "╚════════════════════════════════════════════════════════════╝"
	@python3 scripts/db-info.py http://localhost:$(HTTP_PORT) parquet

db-info-lifecycle:
	@echo "╔════════════════════════════════════════════════════════════╗"
	@echo "║              数据生命周期状态                              ║"
	@echo "╚════════════════════════════════════════════════════════════╝"
	@python3 scripts/db-info.py http://localhost:$(HTTP_PORT) lifecycle

db-info-schema:
	@echo "╔════════════════════════════════════════════════════════════╗"
	@echo "║              Schema 信息                                  ║"
	@echo "╚════════════════════════════════════════════════════════════╝"
	@python3 scripts/db-info.py http://localhost:$(HTTP_PORT) schema

# ============================================================
# Help
# ============================================================

help:
	@echo "TSDB2 Makefile - v$(VERSION) ($(GIT_SHA))"
	@echo ""
	@echo "构建 (Build):"
	@echo "  make build                完整构建 (release)"
	@echo "  make build-debug          完整构建 (debug)"
	@echo "  make build-backend        仅构建 Rust 后端 (tsdb-cli + tsdb-server)"
	@echo "  make build-frontend       仅构建前端"
	@echo ""
	@echo "交叉编译 (Cross-Platform Build):"
	@echo "  make build-linux-amd64    构建 Linux x86_64"
	@echo "  make build-linux-arm64    构建 Linux aarch64 (需要 cross)"
	@echo "  make build-macos-amd64    构建 macOS x86_64"
	@echo "  make build-macos-arm64    构建 macOS Apple Silicon"
	@echo "  make build-windows-amd64  构建 Windows x86_64"
	@echo "  make build-cross          交叉编译 (设置 CROSS_ARCH=...)"
	@echo ""
	@echo "发布 (Distribution):"
	@echo "  make dist                 创建当前架构分发包"
	@echo "  make release              构建所有平台分发包"
	@echo "  make install              安装到 /usr/local"
	@echo "  make uninstall            卸载"
	@echo "  make tag                  创建并推送 git 版本标签"
	@echo ""
	@echo "服务管理 (Server):"
	@echo "  make dev                  开发模式 (RocksDB + Vite 热重载)"
	@echo "  make dev-arrow            开发模式 (Arrow + Vite 热重载)"
	@echo "  make start                生产启动 - RocksDB (前台)"
	@echo "  make start-arrow          生产启动 - Arrow (前台)"
	@echo "  make stop                 停止服务"
	@echo "  make restart              重启服务"
	@echo "  make status               检查服务是否运行"
	@echo "  make start-server         后台守护进程 - RocksDB (-b)"
	@echo "  make start-server-arrow   后台守护进程 - Arrow (-b)"
	@echo "  make stop-server          停止后台守护进程"
	@echo "  make server-status        查看后台服务状态"
	@echo ""
	@echo "测试数据 (Test Data):"
	@echo "  make test-data            生成 IoT 测试数据"
	@echo "  make test-data-iot        生成 IoT 数据"
	@echo "  make test-data-devops     生成 DevOps 数据"
	@echo "  make test-data-ecommerce  生成电商数据"
	@echo "  make test-data-lifecycle  执行数据降级 (温/冷)"
	@echo "  make test-data-clean      清除所有测试数据"
	@echo ""
	@echo "客户端测试 (Client Tests):"
	@echo "  make client-test          运行所有客户端测试"
	@echo "  make client-status        检查数据库状态"
	@echo "  make client-doctor        健康检查"
	@echo "  make client-sql           测试 SQL 查询"
	@echo "  make client-iceberg-list  列出 Iceberg 表"
	@echo "  make client-iceberg-create 创建测试 Iceberg 表"
	@echo ""
	@echo "性能测试 (Performance):"
	@echo "  make bench                写入+读取基准测试 (RocksDB)"
	@echo "  make bench-write          仅写入基准"
	@echo "  make bench-read           仅读取基准"
	@echo "  make bench-full           完整基准套件 (10K/100K/1M)"
	@echo "  make bench-arrow          Arrow 引擎基准"
	@echo ""
	@echo "测试 (TDD):"
	@echo "  make test                 运行单元测试 + lint"
	@echo "  make test-all             运行所有测试 + lint"
	@echo "  make test-unit            仅单元测试"
	@echo "  make test-integration     集成测试"
	@echo "  make test-stress          压力测试"
	@echo "  make test-arrow           Arrow 引擎测试"
	@echo "  make test-arrow-unit      Arrow 单元测试 (nextest)"
	@echo "  make test-arrow-e2e       Arrow E2E 测试"
	@echo "  make tdd                  TDD watch 模式 (全部)"
	@echo "  make tdd-arrow            TDD watch 模式 (Arrow)"
	@echo ""
	@echo "代码质量 (Quality):"
	@echo "  make lint                 Clippy + 格式检查"
	@echo "  make clippy               运行 Clippy"
	@echo "  make fmt                  自动格式化代码"
	@echo "  make check                完整质量门禁"
	@echo "  make security             安全审计 (cargo-deny/audit)"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build         构建 Docker 镜像"
	@echo "  make docker-run           运行 Docker 容器"
	@echo "  make docker-push          推送 Docker 镜像"
	@echo ""
	@echo "其他 (Other):"
	@echo "  make coverage             代码覆盖率 (全部)"
	@echo "  make coverage-arrow       代码覆盖率 (Arrow)"
	@echo "  make clean                清理所有构建产物"
	@echo "  make clean-data           清理数据目录和日志"
	@echo "  make dist-clean           清理分发包"
	@echo ""
	@echo "数据库信息 (Database Info):"
	@echo "  make db-info              显示全部信息 (RocksDB + Parquet + 生命周期)"
	@echo "  make db-info-rocksdb      RocksDB 统计、CF 列表、CF 详情"
	@echo "  make db-info-parquet      Parquet 文件、列结构、层级分布"
	@echo "  make db-info-lifecycle    热/温/冷数据分布"
	@echo "  make db-info-schema       指标与维度 Schema"
	@echo ""
	@echo "变量 (Variables):"
	@echo "  DATA_DIR=$(DATA_DIR)          PARQUET_DIR=$(PARQUET_DIR)"
	@echo "  HOST=$(HOST)                  HTTP_PORT=$(HTTP_PORT)"
	@echo "  FLIGHT_PORT=$(FLIGHT_PORT)    ADMIN_PORT=$(ADMIN_PORT)"
	@echo "  LOG_DIR=$(LOG_DIR)            LOG_LEVEL=$(LOG_LEVEL)"
	@echo "  BENCH_POINTS=$(BENCH_POINTS)  BENCH_WORKERS=$(BENCH_WORKERS)"
	@echo "  CROSS_ARCH=$(CROSS_ARCH)      VERSION=$(VERSION)"
	@echo ""
	@echo "tsdb-server 日志参数:"
	@echo "  --log-dir <DIR>             日志文件目录 (默认: ./logs)"
	@echo "  --log-level <LEVEL>         日志级别 (默认: info)"
	@echo "  --log-max-files <N>         保留轮转日志数 (默认: 10)"
