---
name: worldmonitor-v2-pro
description: Deploy and operate a production-grade global intelligence + event-lifecycle system (WorldMonitor v2) with 18-source ingestion (incl. GDELT TimelineVolRaw/TimelineTone), Country Instability Index scoring, escalation tracking, follow-up generation, Neon/Postgres history, and cron-driven daily briefing workflows. Use when building or running a world-monitoring map/briefing stack, hardening signal quality, or reproducing an advanced local WorldMonitor setup.
---

# worldmonitor-v2-pro

Run a full production workflow for geopolitical and global-risk monitoring:
- multi-source intel pull + normalization
- Country Instability Index (CII) scoring for monitored nations
- event lifecycle tracking (new -> active -> escalating -> resolved -> closed)
- escalation detection + follow-up queue generation
- persistent historical storage (Postgres/Neon)
- cron-scheduled snapshot + briefing pipelines

## Read References On Demand

- System architecture: `references/architecture.md`
- Data source matrix (18 feeds): `references/data-sources.md`
- Deployment/setup workflow: `references/deployment.md`
- Cron playbooks: `references/cron-playbooks.md`
- Troubleshooting guide: `references/troubleshooting.md`
- Exact parity checklist: `references/parity-checklist.md`
- Sanitized diff report vs local files: `references/sanitized-diffs.md`

## Exact Replica Path (Recommended)

Use the shipped templates in `assets/service-template/`:
- `intel_engine.py`
- `event_tracker.py`
- `threat_classifier.py`
- `db_schema.sql`
- `intel_pull_v1.py` (legacy fallback)
- `.env.example`

These templates mirror a production WorldMonitor stack, with sanitized path/location knobs:
- `WORLDMONITOR_DB_ENV_FILE`
- `WORLDMONITOR_NWS_POINT`

New in this build: `intel_engine.py` includes GDELT TimelineVolRaw/TimelineTone regime detection and persists timeline spikes into `intel_stories` (`sources={GDELT_TIMELINE}`) for downstream model consumers.

## Setup Steps

1. Create runtime directory and venv.
2. Copy template files from `assets/service-template/`.
3. Configure `.env` (DB URL + optional weather point override).
4. Install dependencies.
5. Apply DB schema.
6. Run smoke tests.
7. Wire cron jobs for snapshot and briefing cycles.

See exact commands: `references/deployment.md`.

## Core CLI Commands

### Intel Engine

```bash
python3 intel_engine.py
python3 intel_engine.py --section
python3 intel_engine.py --brief
python3 intel_engine.py --store
python3 intel_engine.py --cii
python3 intel_engine.py --alerts
python3 intel_engine.py --polymarket
```

### Event Tracker

```bash
python3 event_tracker.py ingest
python3 event_tracker.py status
python3 event_tracker.py briefing-context
python3 event_tracker.py follow-ups
python3 event_tracker.py complete-followup <ID> "findings"
python3 event_tracker.py close <EVENT_ID>
python3 event_tracker.py report <morning_briefing|on_demand>
python3 event_tracker.py auto-close
python3 event_tracker.py stats
```

## DB Requirements

- PostgreSQL-compatible database (Neon works)
- env var support:
  - `DATABASE_URL` or `NEON_DATABASE_URL`
  - fallback file path via `WORLDMONITOR_DB_ENV_FILE`

Apply schema:

```bash
psql "$DATABASE_URL" -f db_schema.sql
```

## Production Behavior You Should Preserve

- request timeout + bounded total pull budget
- retries/fallbacks for unstable upstreams
- event dedupe and state transitions
- escalation snapshots with before/after context
- report continuity (`new`, `escalated`, `continuing`, `resolved`)
- auto-close policy by event type

Use `references/parity-checklist.md` to verify full behavior parity.

## OpenClaw Integration Pattern

Preferred daily cycle:
1. `intel_engine.py --store`
2. `event_tracker.py ingest`
3. `event_tracker.py auto-close`
4. morning briefing build using `intel_engine.py --section` + `event_tracker.py briefing-context`
5. log delivery via `event_tracker.py report morning_briefing`

## Security & Privacy Baseline

- never commit real DB URLs or API secrets
- keep `.env` local and permissioned (`chmod 600`)
- keep DB + cron credentials outside git
- avoid hardcoding personal directories, host IPs, chat IDs, or identities

## Quality Gates Before Production

- schema apply succeeds
- smoke test outputs non-empty section + context
- cron runs complete without timeout
- follow-up queue populates for critical events
- report continuity tables update daily
