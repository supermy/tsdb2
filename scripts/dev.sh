#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "==> Starting dev environment..."
echo ""

# Build backend in debug mode
echo "[1/3] Building Rust backend (debug)..."
cd "$PROJECT_DIR"
cargo build -p tsdb-cli 2>&1 | tail -1

# Start backend in background
echo "[2/3] Starting TSDB2 server..."
DATA_DIR="${TSDB_DATA_DIR:-$PROJECT_DIR/data}"
mkdir -p "$DATA_DIR"

"$PROJECT_DIR/target/debug/tsdb-cli" serve \
    --data-dir "$DATA_DIR" \
    --storage-engine rocksdb \
    --config default \
    --host 0.0.0.0 \
    --flight-port 50051 \
    --admin-port 8080 \
    --http-port 3000 &
SERVER_PID=$!

cleanup() {
    echo ""
    echo "Shutting down..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Wait for server to start
echo "[3/3] Waiting for server..."
sleep 2

# Start frontend dev server
echo ""
echo "==> Starting frontend dev server..."
echo "    Frontend: http://localhost:5173"
echo "    Backend:  http://localhost:3000"
echo ""
cd "$PROJECT_DIR/tsdb-dashboard"
npx vite --host 0.0.0.0
