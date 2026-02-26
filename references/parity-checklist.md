# Parity Checklist (Local vs Skill Template)

Use this to verify the public skill reproduces the advanced local setup.

## Code Parity

- [ ] `intel_engine.py` copied from local production version
- [ ] `event_tracker.py` copied from local production version
- [ ] `threat_classifier.py` copied from local production version
- [ ] `db_schema.sql` copied from local production version
- [ ] `intel_pull_v1.py` included as legacy fallback

## Expected Sanitized Diffs

Only these sanitization diffs are expected vs local copy:

1. DB env fallback path:
- local hardcoded fallback path -> env-configurable `WORLDMONITOR_DB_ENV_FILE`

2. Weather alert location:
- local fixed NWS point/comment labels -> env-configurable `WORLDMONITOR_NWS_POINT`
- display label changed from location-specific to generic configured-point label

If additional diffs exist, review before publishing.

## Functional Parity

- [ ] intel engine `--store` writes snapshots + CII history
- [ ] event ingest creates/updates lifecycle records
- [ ] escalation detection logs state deltas
- [ ] follow-up queue generation works
- [ ] auto-close transitions stale events
- [ ] briefing-context returns continuity buckets

## Cron Parity

- [ ] daily snapshot pipeline order preserved
- [ ] morning briefing pipeline order preserved
- [ ] report logging occurs after delivery

## Data Source Parity

- [ ] all 17 configured sources represented
- [ ] unavailable source handling is graceful
- [ ] anomaly/convergence logic remains active
