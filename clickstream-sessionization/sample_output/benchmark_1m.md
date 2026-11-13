# Performance measurement

Machine: Linux-6.18.44-fc-v378-x86_64-with-glibc2.39, Python 3.13.18, SQLite 3.53.1

## A. Initial load: 1,065,000 input rows (1,000,000 valid events, 50,000 users, 40 files)

| stage | seconds |
|---|---:|
| generate dataset (not part of pipeline) | 17.61 |
| load_users | 0.36 |
| ingest_validate_load_sessionize | 136.56 |
| sessionize_queued_or_rebuild | 0.00 |
| reconciliation_checks | 10.25 |
| publish | 1.85 |
| **total pipeline run** | **149.31** |

Throughput: **7,133 rows/s**; session rows written: 929,200 (users recomputed 327,753 times for 50,000 users), DQ checks failed: 0.

### Optimization: rebuild each user's sessions once per run (`sessionize_mode = per_run`)

Profiling showed sessionization is the largest cost: with the default `per_file` mode a user present in every batch file is rebuilt once per file. `per_run` queues affected users (crash-safe, in the file's transaction) and rebuilds them once at the end.

| mode | user recomputations | sessionization s | total s | rows/s |
|---|---:|---:|---:|---:|
| per_file (default) | 327,753 | n/a (inside ingest) | 149.31 | 7,133 |
| per_run | 50,000 | 15.60 | 66.28 | 16,069 |

Identical session tables in both modes: **True**. Speed-up: **2.25x**.

## B. Late batch of 500 events touching up to 500 users

| strategy | users recomputed | seconds |
|---|---:|---:|
| incremental (affected users only, default) | 495 | 10.28 |
| full recompute (`--rebuild-sessions`) | all 50,000 | 30.78 |

Identical session tables after both strategies: **True**. Speed-up of incremental: **3.0x**.
