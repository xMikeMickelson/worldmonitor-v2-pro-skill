# Cron Playbooks

## A) Daily Snapshot Pipeline

Run in order:

1. `python3 intel_engine.py --store`
2. `python3 event_tracker.py ingest`
3. `python3 event_tracker.py auto-close`

Purpose:
- ingest new global signals
- update event state
- prune stale event lifecycle tails

## B) Morning Briefing Pipeline

Run in order:

1. `python3 intel_engine.py --section`
2. `python3 event_tracker.py briefing-context`
3. `python3 event_tracker.py follow-ups`
4. external research pass on follow-ups (optional)
5. deliver briefing
6. `python3 event_tracker.py report morning_briefing`

Purpose:
- produce continuity-aware briefing
- separate new/escalating/continuing/resolved stories

## C) On-Demand Briefing Pipeline

1. `python3 intel_engine.py --brief`
2. `python3 event_tracker.py briefing-context`
3. `python3 event_tracker.py follow-ups`
4. deliver report
5. `python3 event_tracker.py report on_demand`

## D) Timeout/Failure Guidance

- keep execution timeout high enough for slow upstreams
- if one source fails, keep run alive and annotate degraded status
- only fail hard on DB schema/connection blocking operations
