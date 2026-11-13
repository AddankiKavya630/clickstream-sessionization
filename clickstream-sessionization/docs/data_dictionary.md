# Data dictionary

Conventions: `snake_case` names; timestamps are ISO-8601 UTC strings with millisecond precision
(`2026-11-01T10:15:30.123Z`) plus an integer `*_epoch_ms` copy wherever arithmetic is needed;
IDs are deterministic (see [ID conventions](#id-conventions)). DDL: [`sql/schema.sql`](../sql/schema.sql).

## Source entities

### Clickstream event file (`events_*.csv` with header, or `*.jsonl`)

Grain: one row per event as emitted by the producer. Rows may arrive late, out of order, duplicated, or broken.

| column | type | required | rules |
|---|---|---|---|
| `event_id` | string | yes | producer's event key; matches `^[A-Za-z0-9_\-:.]{1,64}$`; **natural key** |
| `user_id` | string | yes | must exist in the users file (**foreign key** → `dim_users.user_id`) |
| `event_type` | string | yes | trimmed + lower-cased; one of `page_view, click, search, add_to_cart, remove_from_cart, purchase, login, logout` (configurable) |
| `event_ts` | string | yes | ISO-8601 date **and** time; `Z`, `+05:30` etc. or no offset (= UTC by default); `T` or space separator; must be ≥ `2000-01-01` and ≤ reference time + 10 min |
| `page_url` | string | no | empty → `NULL` |
| `device` | string | no | lower-cased; empty → `NULL` |

Header handling: names are trimmed/lower-cased, column order is free, a UTF-8 BOM is ignored. **Extra columns** are ignored and
reported as `SCHEMA_DRIFT`. A **missing required column** rejects the whole file (`FILE_SCHEMA_MISMATCH`).

### Users reference file (`users.csv`)

| column | type | required | rules |
|---|---|---|---|
| `user_id` | string | yes | **primary key**; first occurrence wins if repeated |
| `signup_ts` | string | no | ISO-8601; unparseable → `NULL` (counted in `bad_signup_ts`) |
| `country` | string | no | upper-cased |

## Target tables

### `dim_users` — reference data (grain: user)

| column | type | null | notes |
|---|---|---|---|
| `user_id` | TEXT | no | **PK** |
| `signup_ts`, `country` | TEXT | yes | latest value from the users file (SCD type 1 — overwritten) |
| `source_file`, `loaded_run_id`, `updated_at` | TEXT | no | lineage |

### `events` — silver, validated and de-duplicated (grain: one row per unique `event_id`)

Append-only: rows are never updated or deleted by the pipeline, so the event history is preserved.

| column | type | null | notes |
|---|---|---|---|
| `event_id` | TEXT | no | **PK** |
| `user_id` | TEXT | no | **FK** → `dim_users` (enforced by SQLite `FOREIGN KEY` and a DQ check) |
| `event_type` | TEXT | no | normalized |
| `event_ts` | TEXT | no | canonical UTC string |
| `event_ts_epoch_ms` | INTEGER | no | derived; used for ordering and gap arithmetic |
| `page_url`, `device` | TEXT | yes | |
| `source_file`, `source_file_hash`, `source_line` | TEXT/TEXT/INT | no | exact lineage back to the input row |
| `loaded_run_id`, `ingested_at` | TEXT | no | which run inserted it, when |

Index `ix_events_user_ts (user_id, event_ts_epoch_ms, event_id)` serves the sessionization window.

### `rejected_events` — quarantine (grain: one row per quarantined source line)

| column | type | null | notes |
|---|---|---|---|
| `reject_id` | TEXT | no | **PK** = `rej_` + sha256(source_file_hash, source_line)[:20] |
| `record_class` | TEXT | no | `invalid`, `duplicate` or `file` |
| `reason_code` | TEXT | no | primary reason (see table below) |
| `reason_detail` | TEXT | yes | all failed rules, or what the duplicate matched |
| `event_id` | TEXT | yes | as supplied (may be missing/garbage) |
| `source_file`, `source_file_hash`, `source_line` | | no | `source_line = 0` for a whole-file rejection. **UNIQUE(source_file_hash, source_line)** makes reruns safe |
| `raw_record` | TEXT | yes | original cells as a JSON array (CSV) or the original JSON line |
| `run_id`, `rejected_at` | TEXT | no | |

| reason_code | class | meaning |
|---|---|---|
| `MALFORMED_ROW` | invalid | wrong number of CSV fields, invalid JSON, JSON not an object |
| `MISSING_REQUIRED_FIELD` | invalid | `event_id`, `user_id` or `event_type` missing/blank |
| `MISSING_EVENT_TS` | invalid | `event_ts` missing/blank |
| `INVALID_EVENT_TS` | invalid | not an ISO-8601 date-time (incl. date-only, impossible dates, epoch numbers) |
| `EVENT_TS_OUT_OF_RANGE` | invalid | earlier than `min_event_ts` |
| `FUTURE_EVENT_TS` | invalid | later than reference time + `max_future_skew_minutes` |
| `INVALID_EVENT_ID` | invalid | violates `event_id_pattern` |
| `UNKNOWN_EVENT_TYPE` | invalid | not in `allowed_event_types` |
| `INVALID_USER_REF` | invalid | `user_id` not in `dim_users` (referential integrity) |
| `DUPLICATE_EXACT` | duplicate | same `event_id` and same payload as the reference version |
| `DUPLICATE_CONFLICT` | duplicate | same `event_id`, different payload; reference version kept |
| `FILE_SCHEMA_MISMATCH` | file | required column missing / duplicated column in header |

When a row breaks several rules, the reason earliest in the table above becomes `reason_code` and all of them are listed in `reason_detail`.
The **reference version** of an `event_id` is the stored one if it exists, otherwise its first occurrence in the file.

### `sessions` — gold (grain: one row per user session)

| column | type | null | notes |
|---|---|---|---|
| `session_id` | TEXT | no | **PK** = `ses_` + sha256(user_id, first_event_id)[:20] |
| `user_id` | TEXT | no | **FK** → `dim_users` |
| `user_session_seq` | INTEGER | no | 1..n per user by start time; **UNIQUE(user_id, user_session_seq)** |
| `session_start_ts` / `session_end_ts` | TEXT | no | first / last event time |
| `session_start_epoch_ms` / `session_end_epoch_ms` | INTEGER | no | |
| `duration_seconds` | REAL | no | end − start (0 for single-event sessions) |
| `event_count` | INTEGER | no | |
| `distinct_event_types` | INTEGER | no | |
| `first_event_id` / `last_event_id` | TEXT | no | ordering is (event_ts, event_id) |
| `landing_page` / `exit_page` | TEXT | yes | `page_url` of first / last event |
| `device` | TEXT | yes | device of the first event |
| `is_bounce` | INTEGER | no | 1 when `event_count = 1` |
| `gap_seconds_used` | INTEGER | no | inactivity gap that produced the row (detects stale sessions after a config change) |
| `computed_run_id`, `computed_at` | TEXT | no | lineage |

### `session_events` — gold bridge (grain: event)

| column | type | null | notes |
|---|---|---|---|
| `event_id` | TEXT | no | **PK**, **FK** → `events` — every event is in exactly one session |
| `session_id` | TEXT | no | **FK** → `sessions` |
| `user_id` | TEXT | no | denormalized for fast per-user recompute |
| `event_seq_in_session` | INTEGER | no | 1..n |
| `gap_from_prev_seconds` | REAL | yes | `NULL` for the first event of a session |

### Operational tables

| table | grain | purpose |
|---|---|---|
| `pipeline_runs` | run | `run_id`, status (`RUNNING / SUCCESS / PARTIAL_SUCCESS / FAILED / DQ_FAILED`), start/end, duration, file and row counts, `failure_reason`, config and stage timings as JSON |
| `file_ingestion_log` | distinct file content (`source_file_hash` **PK**) | idempotency key; status `SUCCESS / FAILED / REJECTED`, `attempts`, per-file counts, schema warnings, error message |
| `pending_session_users` | user | crash-safe queue of users whose sessions must be rebuilt |
| `dq_check_results` | (run, check) | every reconciliation check with observed / expected value |

## Derived-field rules

* **Session boundary:** per user, order events by `(event_ts_epoch_ms, event_id)`. A new session starts at the first event or
  when `event_ts_epoch_ms − previous > gap` (strictly greater; default gap 30 min). Equal to the gap = same session.
* Sessions are **not** cut at midnight or any calendar boundary, and there is no maximum session length.
* Ties (identical timestamps) are ordered by `event_id`, which makes `first_event_id`, `landing_page`, `exit_page` deterministic.

## ID conventions

| id | format | deterministic? |
|---|---|---|
| `event_id` | from source | yes (natural key) |
| `session_id` | `ses_` + 20 hex of sha256(`user_id`, `first_event_id`) | yes: same events ⇒ same id, regardless of file order or rerun |
| `reject_id` | `rej_` + 20 hex of sha256(`source_file_hash`, `source_line`) | yes |
| `source_file_hash` | sha256 of file bytes | yes |
| `run_id` | `run_<UTC yyyymmddThhmmssZ>_<6 hex random>` | unique per execution by design (a run is an event, not a function of data) |

Hash inputs are joined with the ASCII unit separator (`\x1f`) so that `("a|b","c")` and `("a","b|c")` cannot collide.
