FROM rust:1.85.0 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p tsdb-cli

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y ca-certificates libgcc-s1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/tsdb-cli /usr/local/bin/tsdb-cli
ENTRYPOINT ["tsdb-cli"]
