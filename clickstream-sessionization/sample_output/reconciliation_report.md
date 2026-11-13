# Reconciliation report — `run_20261111T105659Z_79b9f5`

- **Status:** SUCCESS (exit code 0)
- **Started / ended (UTC):** 2026-11-11T10:56:59.620Z / 2026-11-11T10:56:59.624Z
- **Duration:** 0.0041 s, throughput 0.0 rows/s

## Source-to-target counts (this run)

| file | status | read | inserted | duplicate | already loaded | rejected | balanced |
|---|---|---:|---:|---:|---:|---:|:---:|
| data/sample/input/events/events_batch_001.csv | SKIPPED | 0 | 0 | 0 | 0 | 0 | yes |
| data/sample/input/events/events_batch_002.csv | SKIPPED | 0 | 0 | 0 | 0 | 0 | yes |
| sample_output/demo_late_batch/events_batch_late.csv | SKIPPED | 0 | 0 | 0 | 0 | 0 | yes |
| **total** | | 0 | 0 | 0 | 0 | 0 | |

## Quarantine by reason (this run)

| class | reason | rows |
|---|---|---:|

## Data-quality checks

| check | status | violations | rule |
|---|---|---:|---|
| pk_unique_events | PASS | 0 | events.event_id is unique |
| pk_unique_sessions | PASS | 0 | sessions.session_id is unique |
| required_fields_not_null | PASS | 0 | no NULL required fields in events |
| ri_events_users | PASS | 0 | every event.user_id exists in dim_users |
| ri_sessions_users | PASS | 0 | every session.user_id exists in dim_users |
| every_event_sessionized | PASS | 0 | every valid event is assigned to exactly one session |
| no_orphan_session_events | PASS | 0 | every session_events row points to an existing session and event |
| session_event_count_matches | PASS | 0 | sessions.event_count equals the number of mapped events |
| total_events_reconcile | PASS | 0 | SUM(sessions.event_count) equals COUNT(events) |
| session_bounds_match_events | PASS | 0 | session start/end equal min/max event time of its events |
| duration_consistent | PASS | 0 | duration_seconds = (end - start) and is non-negative |
| intra_session_gap_within_limit | PASS | 0 | no gap inside a session exceeds the inactivity gap |
| inter_session_gap_exceeds_limit | PASS | 0 | consecutive sessions of a user are separated by more than the gap |
| sessions_use_current_gap | PASS | 0 | all sessions were computed with the configured gap (else run --rebuild-sessions) |
| no_pending_sessionization | PASS | 0 | no user is waiting for session recomputation |
| user_session_seq_contiguous | PASS | 0 | user_session_seq runs 1..n without gaps per user |
| rejects_traceable | PASS | 0 | every rejected record has source file, hash, line and reason |
| file_counts_reconcile | PASS | 0 | per file in this run: rows_read = inserted + duplicate + already_loaded + rejected |
| file_rejects_persisted | PASS | 0 | per successful file: rows_rejected + rows_duplicate equals quarantined rows for that file |
| file_inserts_persisted | PASS | 0 | per successful file: rows_inserted + rows_already_loaded equals events stored from that file |

## Warehouse totals (all runs)

- users: 25, events: 401, sessions: 87, quarantined rows: 27
- avg events/session: 4.609, avg duration: 2778.075 s, bounce rate: 0.1954
