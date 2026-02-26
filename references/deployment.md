# Deployment

## 1) Bootstrap runtime

```bash
mkdir -p /opt/worldmonitor
cd /opt/worldmonitor
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install asyncpg
```

Copy from skill assets:

```bash
cp assets/service-template/intel_engine.py .
cp assets/service-template/event_tracker.py .
cp assets/service-template/threat_classifier.py .
cp assets/service-template/db_schema.sql .
cp assets/service-template/intel_pull_v1.py .
cp assets/service-template/.env.example .env
```

## 2) Configure env

Edit `.env` and set:
- `DATABASE_URL` (or `NEON_DATABASE_URL`)
- `WORLDMONITOR_DB_ENV_FILE` (optional fallback path)
- `WORLDMONITOR_NWS_POINT` (optional weather point)

Protect file:

```bash
chmod 600 .env
```

## 3) Load env + apply schema

```bash
set -a
source .env
set +a
psql "$DATABASE_URL" -f db_schema.sql
```

## 4) Smoke test

```bash
python3 intel_engine.py --section
python3 intel_engine.py --store
python3 event_tracker.py ingest
python3 event_tracker.py briefing-context
python3 event_tracker.py stats
```

## 5) Promote to scheduled operations

Create cron jobs or OpenClaw cron entries for:
- daily snapshot pipeline
- morning briefing pipeline

See `references/cron-playbooks.md`.
