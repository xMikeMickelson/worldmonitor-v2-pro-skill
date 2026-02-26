# Sanitized Diffs vs Local Production Files

This skill intentionally applies a minimal sanitization layer to avoid publishing personal paths/location defaults.

## `intel_engine.py`

1. Added env-configurable weather point:
   - `NWS_ALERT_POINT = os.environ.get("WORLDMONITOR_NWS_POINT", "39.8283,-98.5795")`
2. Replaced fixed NWS URL point with configurable point.
3. Replaced location-specific label text with generic configured-point label.
4. Replaced hardcoded DB env fallback path with:
   - `WORLDMONITOR_DB_ENV_FILE` (default `~/.config/worldmonitor/neon.env`)

## `event_tracker.py`

1. Replaced hardcoded DB env fallback path with:
   - `WORLDMONITOR_DB_ENV_FILE` (default `~/.config/worldmonitor/neon.env`)

## Files unchanged from local copy

- `threat_classifier.py`
- `db_schema.sql`
- `intel_pull_v1.py`
