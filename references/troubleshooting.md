# Troubleshooting

## 1) `DATABASE_URL not found`

Cause:
- env not loaded
- fallback env file path invalid

Fix:
- export `DATABASE_URL`
- or set `WORLDMONITOR_DB_ENV_FILE` to a valid env file path

---

## 2) Slow/timeout pulls

Cause:
- unstable upstream (notably conflict/news feeds)

Fix:
- increase per-source timeout budget carefully
- keep graceful fallback behavior (do not crash full run)
- reduce source page size/record volume where acceptable

---

## 3) No escalations detected

Cause:
- thresholds too strict or no delta from prior state

Fix:
- verify ingest is running after snapshots
- inspect event signature matching logic and severity mapping
- confirm previous state records exist

---

## 4) Follow-up queue empty

Cause:
- no critical events or follow-up generation disabled by thresholds

Fix:
- verify critical/high events exist in `event_tracker.py status`
- test with known high-severity sample events

---

## 5) Briefing has no continuity context

Cause:
- report logging not being called after delivery

Fix:
- run `event_tracker.py report morning_briefing` (or `on_demand`) post-send

---

## 6) Schema errors on startup

Cause:
- mismatched DB schema version

Fix:
- re-apply `db_schema.sql` in staging first
- verify indexes/views expected by code are present
