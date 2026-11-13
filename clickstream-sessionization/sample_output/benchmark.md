# Performance measurement

Machine: Linux-6.18.44-fc-v378-x86_64-with-glibc2.39, Python 3.13.18, SQLite 3.53.1

## A. Initial load: 106,500 input rows (100,000 valid events, 5,000 users, 4 files)

| stage | seconds |
|---|---:|
| generate dataset (not part of pipeline) | 1.10 |
| load_users | 0.03 |
| ingest_validate_load_sessionize | 5.52 |
| sessionize_queued_or_rebuild | 0.00 |
| reconciliation_checks | 0.68 |
| publish | 0.16 |
| **total pipeline run** | **6.44** |

Throughput: **16,547 rows/s**; session rows written: 46,718 (users recomputed 16,154 times for 5,000 users), DQ checks failed: 0.

### Optimization: rebuild each user's sessions once per run (`sessionize_mode = per_run`)

Profiling showed sessionization is the largest cost: with the default `per_file` mode a user present in every batch file is rebuilt once per file. `per_run` queues affected users (crash-safe, in the file's transaction) and rebuilds them once at the end.

| mode | user recomputations | sessionization s | total s | rows/s |
|---|---:|---:|---:|---:|
| per_file (default) | 16,154 | n/a (inside ingest) | 6.44 | 16,547 |
| per_run | 5,000 | 1.49 | 4.66 | 22,835 |

Identical session tables in both modes: **True**. Speed-up: **1.38x**.

## B. Late batch of 500 events touching up to 500 users

| strategy | users recomputed | seconds |
|---|---:|---:|
| incremental (affected users only, default) | 479 | 1.10 |
| full recompute (`--rebuild-sessions`) | all 5,000 | 2.63 |

Identical session tables after both strategies: **True**. Speed-up of incremental: **2.4x**.
