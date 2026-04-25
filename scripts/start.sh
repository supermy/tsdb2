#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

DATA_DIR="${TSDB_DATA_DIR:-$PROJECT_DIR/data}"
HOST="${TSDB_HOST:-0.0.0.0}"
FLIGHT_PORT="${TSDB_FLIGHT_PORT:-50051}"
ADMIN_PORT="${TSDB_ADMIN_PORT:-8080}"
HTTP_PORT="${TSDB_HTTP_PORT:-3000}"
ENGINE="${TSDB_ENGINE:-rocksdb}"
CONFIG="${TSDB_CONFIG:-default}"

mkdir -p "$DATA_DIR"

echo "╔══════════════════════════════════════════════╗"
echo "║       TSDB2 Server Starting                 ║"
echo "╠══════════════════════════════════════════════╣"
echo "║  Data Dir:     $DATA_DIR"
echo "║  Engine:       $ENGINE"
echo "║  Config:       $CONFIG"
echo "║  Flight gRPC:  $HOST:$FLIGHT_PORT"
echo "║  Admin nng:    $HOST:$ADMIN_PORT"
echo "║  Dashboard:    http://$HOST:$HTTP_PORT"
echo "╚══════════════════════════════════════════════╝"

exec "$PROJECT_DIR/target/release/tsdb-cli" serve \
    --data-dir "$DATA_DIR" \
    --storage-engine "$ENGINE" \
    --config "$CONFIG" \
    --host "$HOST" \
    --flight-port "$FLIGHT_PORT" \
    --admin-port "$ADMIN_PORT" \
    --http-port "$HTTP_PORT"
