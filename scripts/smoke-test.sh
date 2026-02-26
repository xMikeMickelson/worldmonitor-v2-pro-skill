#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:-.}"
cd "$TARGET_DIR"

python3 intel_engine.py --section >/tmp/worldmonitor-section.txt
python3 intel_engine.py --cii >/tmp/worldmonitor-cii.txt

echo "--- SECTION PREVIEW ---"
head -n 20 /tmp/worldmonitor-section.txt || true
echo "--- CII PREVIEW ---"
head -n 20 /tmp/worldmonitor-cii.txt || true

echo "Smoke test complete"
