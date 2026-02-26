#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:-/opt/worldmonitor}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$TARGET_DIR"
cp assets/service-template/intel_engine.py "$TARGET_DIR/"
cp assets/service-template/event_tracker.py "$TARGET_DIR/"
cp assets/service-template/threat_classifier.py "$TARGET_DIR/"
cp assets/service-template/db_schema.sql "$TARGET_DIR/"
cp assets/service-template/intel_pull_v1.py "$TARGET_DIR/"
cp assets/service-template/.env.example "$TARGET_DIR/.env"

cd "$TARGET_DIR"
if [ ! -d venv ]; then
  "$PYTHON_BIN" -m venv venv
fi
source venv/bin/activate
pip install --upgrade pip
pip install asyncpg

chmod 600 .env || true

echo "Bootstrapped worldmonitor at: $TARGET_DIR"
echo "Next: edit .env, source it, apply db_schema.sql, run smoke-test"
