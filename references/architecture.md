# Architecture

## Pipeline

```text
intel_engine (multi-source pull)
  -> normalize + score (CII, anomalies, convergence)
  -> persist snapshot/history
  -> event_tracker ingest
      -> create/update events
      -> detect escalations
      -> generate follow-ups
      -> auto-close stale events
  -> briefing context
  -> daily/on-demand report
```

## Primary Components

1. **intel_engine.py**
   - pulls global data from 18 feeds (including GDELT TimelineVolRaw/Tone)
   - computes Country Instability Index for monitored countries
   - detects volume/tone spike regimes and persists them as `GDELT_TIMELINE` intel stories
   - emits brief/section output and optional DB snapshot writes

2. **event_tracker.py**
   - ingests current intel signals
   - tracks event lifecycle state
   - captures escalations and follow-up tasks
   - logs report continuity

3. **threat_classifier.py**
   - enriches event severity/type classification logic

4. **db_schema.sql**
   - creates snapshots/history/events/escalations/follow-ups/report tables

## Event Lifecycle Model

`NEW -> ACTIVE -> ESCALATING -> DE-ESCALATING -> RESOLVED -> CLOSED`

Key behavior:
- dedupe by external identity/signature
- severity/status transitions tracked with timestamps
- follow-up priorities raised for escalations
- stale-event auto-close by event category timeout

## Reporting Context Model

`briefing-context` returns at minimum:
- new events since last report
- escalations since last report
- active critical events
- recently resolved events
- continuing stories already reported
- pending follow-ups
