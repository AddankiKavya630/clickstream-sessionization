# Design notes and trade-offs

## Why Python standard library + SQLite

* **Runs from a clean environment with zero runtime dependencies** (only `pytest` for tests). No JVM, no Docker, no credentials.
* SQLite gives real SQL (window functions, transactions, foreign keys, unique constraints) so the sessionization and the
  reconciliation are written as plain SQL in [`sql/`](../sql) and can be read and reviewed on their own.
* The same SQL would move to Postgres / Snowflake / DuckDB with small dialect changes (`make_session_id` would become a
  native `sha2()` call).
* Trade-off: single writer, single machine. Fine for the target volume (1M rows in well under a minute); see "next steps".

## Layers

`input files → validation (Python) → events (silver, append-only) → sessions + session_events (gold, recomputable) → CSV snapshot`

Python owns what is awkward in SQL (parsing CSV/JSON, timestamp formats, per-row rule evaluation with good error messages).
SQL owns set-based work (dedupe against stored events, sessionization, reconciliation).

## Key decisions

| decision | alternative considered | why |
|---|---|---|
| **Recompute all sessions of an affected user** from the full event history | incremental "extend the open session" logic | late and out-of-order events can merge, extend or re-order sessions anywhere in the past; full per-user recompute is simple and obviously correct. Only touched users are recomputed (2.4× faster than a full rebuild in the benchmark). |
| **Reject** rows without a timestamp | use ingestion time / file time | event-time sessionization with a made-up time would silently produce wrong sessions; the row is quarantined with its raw content so it can be fixed and replayed. |
| Gap rule is **strictly greater than** | `>=` | most analytics tools (GA-style "30 minutes of inactivity") treat exactly 30 min as still active; documented and tested at the ms boundary. |
| Ordering key `(event_ts, event_id)` | timestamp only | identical timestamps are common (client batching); the tie-breaker makes every derived field deterministic. |
| `session_id = hash(user_id, first_event_id)` | sequential ids, `hash(user, start_ts)` | stable across reruns and file order. A late event *before* a session's first event changes the id — correct, because it really is a different session now; the old row is deleted in the same transaction. |
| **First-seen wins** for conflicting duplicates; stored version is the reference | last-write-wins | event streams should be immutable; a conflicting resend is a producer bug, so it is quarantined for review rather than silently overwriting history. Trade-off: if files are loaded in a different order, which copy "wins" can differ (sessions are unaffected unless the conflict is in `user_id`/`event_ts`). |
| File-level idempotency by **content hash**, record-level by `event_id` PK and `UNIQUE(file_hash, line)` in quarantine | file name | renamed or re-delivered copies are detected; `--force` reprocessing is still a no-op. |
| **One transaction per file** | one per run / per row | a failure loses at most the current file, which is retried automatically on the next run; earlier files stay committed. |
| Missing required column ⇒ **reject the file**; extra columns ⇒ **accept + warn** | fail the run / accept everything | a renamed `event_ts` would make every row invalid — better to stop that file loudly. Additive changes are normal schema evolution. |
| Naive timestamps assumed UTC (configurable to reject) | always reject | many producers emit UTC without an offset; the policy is explicit in config. |
| Publish CSV snapshot only when DQ checks pass | always publish | consumers keep the last good snapshot if a run produces inconsistent data. |
| Dimension `dim_users` is SCD-1 (overwrite) | SCD-2 | user attributes are not used for sessionization; history of events is what matters and is preserved. |

## Failure modes and recovery

| situation | what happens | operator action |
|---|---|---|
| bad rows | quarantined with reason + raw content; run `SUCCESS` | fix upstream, resend (new file) |
| missing required column | file `REJECTED`, other files load; run `PARTIAL_SUCCESS` (exit 2) | fix the file and deliver it again (new content = new hash) |
| crash / exception while loading a file | that file's transaction rolls back, logged as `FAILED`; run `PARTIAL_SUCCESS` (exit 2) | just re-run; failed files are retried, loaded files skipped |
| crash after events committed but before queued sessionization (`per_run` mode) | users stay in `pending_session_users`; run `FAILED` | just re-run; the queue is drained first |
| reconciliation check fails | run `DQ_FAILED` (exit 3); outputs not republished | investigate `dq_check_results` |
| session gap changed in config | `sessions_use_current_gap` fails | `run --rebuild-sessions` (also the backfill tool) |
| users file broken | run `FAILED` (exit 1), nothing loaded | fix the users file |

**Backfill:** drop historical files into the input folder (or pass them with `--input`) and run normally — affected users are
recomputed. To recompute everything (e.g. after a rule change) use `--rebuild-sessions`.

**Replay rejects:** `rejected_events.raw_record` keeps the original content. A corrected file can be re-delivered; rows already
loaded are recognised as duplicates, so replaying a whole file is safe.

## Limitations

* Single-process, single-writer (SQLite). Concurrent runs against the same database are not supported (a second writer
  waits / fails on the lock).
* Late events arriving after the published snapshot change past sessions (by design). Consumers that need frozen history
  would need a "closed session" watermark.
* No maximum session duration (a bot clicking every 29 minutes produces one very long session).
* Events whose user arrives in the users file *later* are rejected as `INVALID_USER_REF`; they must be replayed.
* Sub-millisecond precision is truncated.
* `FUTURE_EVENT_TS` depends on the reference time; pass `--reference-time` for reproducible reruns of old data.

## Next steps

1. Watermark-based "session closed" flag (e.g. no events for gap + allowed lateness) for consumers needing stable history.
2. Optional max session duration / midnight split as configurable rules.
3. A `replay-rejects` command that re-validates quarantined rows after a users-file fix.
4. Move the SQL to DuckDB/Postgres for parallel scans; partition sessionization by user hash for multi-worker runs.
5. Push run metrics (`pipeline_runs`) to a dashboard / alert on `exit_code != 0`.
