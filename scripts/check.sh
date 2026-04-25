#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "==> Running all tests..."
cd "$PROJECT_DIR"

echo "[1/3] cargo clippy..."
cargo clippy --workspace -- -D warnings 2>&1 | tail -5

echo ""
echo "[2/3] cargo test..."
cargo test --workspace --exclude tsdb-stress --exclude tsdb-integration-tests 2>&1 | tail -10

echo ""
echo "[3/3] Frontend build..."
cd "$PROJECT_DIR/tsdb-dashboard"
npm run build 2>&1 | tail -3

echo ""
echo "✅ All checks passed!"
