#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "==> Building Rust backend (release)..."
cd "$PROJECT_DIR"
cargo build --release -p tsdb-cli

echo "==> Building frontend dashboard..."
cd "$PROJECT_DIR/tsdb-dashboard"
npm ci --quiet 2>/dev/null || npm install --quiet
npm run build

echo "==> Copying frontend dist to static dir..."
rm -rf "$PROJECT_DIR/target/release/dashboard"
cp -r "$PROJECT_DIR/tsdb-dashboard/dist" "$PROJECT_DIR/target/release/dashboard"

echo ""
echo "✅ Build complete!"
echo "   Binary: $PROJECT_DIR/target/release/tsdb-cli"
echo "   Dashboard: $PROJECT_DIR/target/release/dashboard/"
echo ""
echo "Run: $SCRIPT_DIR/start.sh"
