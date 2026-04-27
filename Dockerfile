FROM rust:1.85.0 AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir -p crates/tsdb-cli/src && echo "fn main() {}" > crates/tsdb-cli/src/main.rs
RUN mkdir -p crates/tsdb-rocksdb/src && echo "" > crates/tsdb-rocksdb/src/lib.rs
RUN mkdir -p crates/tsdb-arrow/src && echo "" > crates/tsdb-arrow/src/lib.rs
RUN mkdir -p crates/tsdb-parquet/src && echo "" > crates/tsdb-parquet/src/lib.rs
RUN mkdir -p crates/tsdb-storage-arrow/src && echo "" > crates/tsdb-storage-arrow/src/lib.rs
RUN mkdir -p crates/tsdb-datafusion/src && echo "" > crates/tsdb-datafusion/src/lib.rs
RUN mkdir -p crates/tsdb-flight/src && echo "" > crates/tsdb-flight/src/lib.rs
RUN mkdir -p crates/tsdb-iceberg/src && echo "" > crates/tsdb-iceberg/src/lib.rs
RUN mkdir -p crates/tsdb-integration-tests/src && echo "" > crates/tsdb-integration-tests/src/lib.rs
RUN mkdir -p crates/tsdb-admin/src && echo "" > crates/tsdb-admin/src/lib.rs
RUN mkdir -p crates/tsdb-stress/src && echo "" > crates/tsdb-stress/src/lib.rs
RUN mkdir -p crates/tsdb-stress-rocksdb/src && echo "" > crates/tsdb-stress-rocksdb/src/lib.rs
RUN mkdir -p crates/tsdb-test-utils/src && echo "" > crates/tsdb-test-utils/src/lib.rs
RUN mkdir -p crates/tsdb-bench/src && echo "" > crates/tsdb-bench/src/lib.rs

COPY crates/tsdb-cli/Cargo.toml crates/tsdb-cli/
COPY crates/tsdb-rocksdb/Cargo.toml crates/tsdb-rocksdb/
COPY crates/tsdb-arrow/Cargo.toml crates/tsdb-arrow/
COPY crates/tsdb-parquet/Cargo.toml crates/tsdb-parquet/
COPY crates/tsdb-storage-arrow/Cargo.toml crates/tsdb-storage-arrow/
COPY crates/tsdb-datafusion/Cargo.toml crates/tsdb-datafusion/
COPY crates/tsdb-flight/Cargo.toml crates/tsdb-flight/
COPY crates/tsdb-iceberg/Cargo.toml crates/tsdb-iceberg/
COPY crates/tsdb-integration-tests/Cargo.toml crates/tsdb-integration-tests/
COPY crates/tsdb-admin/Cargo.toml crates/tsdb-admin/
COPY crates/tsdb-stress/Cargo.toml crates/tsdb-stress/
COPY crates/tsdb-stress-rocksdb/Cargo.toml crates/tsdb-stress-rocksdb/
COPY crates/tsdb-test-utils/Cargo.toml crates/tsdb-test-utils/
COPY crates/tsdb-bench/Cargo.toml crates/tsdb-bench/

RUN cargo build --release -p tsdb-cli 2>/dev/null || true

COPY . .
RUN cargo build --release -p tsdb-cli

FROM node:20-slim AS frontend
WORKDIR /app/tsdb-dashboard
COPY tsdb-dashboard/package.json tsdb-dashboard/package-lock.json ./
RUN npm ci
COPY tsdb-dashboard/ ./
RUN npm run build

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y ca-certificates libgcc-s1 tzdata && rm -rf /var/lib/apt/lists/*
RUN useradd -m -s /bin/bash tsdb
COPY --from=builder /app/target/release/tsdb-cli /usr/local/bin/tsdb-cli
COPY --from=frontend /app/tsdb-dashboard/dist /usr/local/bin/dashboard
USER tsdb
EXPOSE 50051
ENTRYPOINT ["tsdb-cli"]
