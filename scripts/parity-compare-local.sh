#!/usr/bin/env bash
set -euo pipefail

LOCAL_DIR="${1:-./local-worldmonitor}"
TPL_DIR="${2:-assets/service-template}"

for f in intel_engine.py event_tracker.py threat_classifier.py db_schema.sql intel_pull_v1.py; do
  if [ ! -f "$LOCAL_DIR/$f" ] || [ ! -f "$TPL_DIR/$f" ]; then
    echo "Missing file: $f"
    continue
  fi
  echo "== $f =="
  if cmp -s "$LOCAL_DIR/$f" "$TPL_DIR/$f"; then
    echo "MATCH"
  else
    echo "DIFF"
    # show first useful diff lines
    diff -u "$LOCAL_DIR/$f" "$TPL_DIR/$f" | sed -n '1,80p' || true
  fi
  echo
 done
