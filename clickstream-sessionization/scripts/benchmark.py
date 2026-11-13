"""Simple performance measurement.

  python scripts/benchmark.py                    # 100k clean events
  python scripts/benchmark.py --events 1000000   # 1M

Scenario A: full initial load of N events (all batches), with stage timings.
Scenario B: a small late batch arrives afterwards. Compares
            (1) incremental recompute of only the affected users (what the pipeline does) vs
            (2) recomputing sessions for every user (--rebuild-sessions).
Writes a markdown summary to sample_output/benchmark.md (or --report).
"""
from __future__ import annotations

import argparse
import csv
import logging
import platform
import random
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sessionize.config import PipelineConfig  # noqa: E402
from sessionize.generator import GeneratorConfig, generate  # noqa: E402
from sessionize.pipeline import run_pipeline  # noqa: E402
from sessionize.schema import SOURCE_COLUMNS  # noqa: E402
from sessionize.timeutil import format_utc, from_epoch_ms, parse_iso_utc  # noqa: E402

REF = parse_iso_utc("2026-12-31T00:00:00Z")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=100_000)
    ap.add_argument("--users", type=int)
    ap.add_argument("--late-events", type=int, default=500)
    ap.add_argument("--report", default=str(ROOT / "sample_output" / "benchmark.md"))
    a = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("sessionize").propagate = False   # per-run log files are still written
    n_users = a.users or max(10, a.events // 20)
    cfg = PipelineConfig(reference_time=REF)

    work = Path(tempfile.mkdtemp(prefix="sessionize_bench_"))
    try:
        t = time.perf_counter()
        summary = generate(work / "gen", GeneratorConfig(n_users=n_users, n_events=a.events,
                                                         n_batches=max(2, a.events // 25_000), seed=42))
        t_gen = time.perf_counter() - t

        t = time.perf_counter()
        res = run_pipeline([work / "gen" / "input" / "events"], work / "wh.db", work / "out", cfg,
                           users_path=work / "gen" / "input" / "users.csv")
        t_run = time.perf_counter() - t
        assert res.status == "SUCCESS", res.failure_reason

        # Same load with sessionize_mode=per_run (rebuild each user once per run instead of once per file)
        t = time.perf_counter()
        res_pr = run_pipeline([work / "gen" / "input" / "events"], work / "wh_pr.db", work / "out_pr",
                              cfg.with_overrides(sessionize_mode="per_run"),
                              users_path=work / "gen" / "input" / "users.csv")
        t_run_pr = time.perf_counter() - t
        assert res_pr.status == "SUCCESS", res_pr.failure_reason
        c1, c2 = sqlite3.connect(work / "wh.db"), sqlite3.connect(work / "wh_pr.db")
        qq = "SELECT session_id, event_count, session_start_ts, session_end_ts FROM sessions ORDER BY session_id"
        same_pr = c1.execute(qq).fetchall() == c2.execute(qq).fetchall()
        c1.close(), c2.close()

        # ---- Scenario B: small late batch for random users
        rng = random.Random(1)
        conn = sqlite3.connect(work / "wh.db")
        sample = conn.execute("SELECT user_id, event_ts_epoch_ms FROM events ORDER BY random() LIMIT ?",
                              (a.late_events,)).fetchall()
        conn.close()
        late = work / "late" / "late_batch.csv"
        late.parent.mkdir()
        with late.open("w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(SOURCE_COLUMNS)
            for i, (uid, ms) in enumerate(sample):
                w.writerow([f"late_{i:06d}", uid, "click", format_utc(from_epoch_ms(ms + rng.randint(1, 600_000))),
                            "/late", "mobile"])
        shutil.copy(work / "wh.db", work / "wh_full.db")

        t = time.perf_counter()
        inc = run_pipeline([late], work / "wh.db", work / "out_inc", cfg, publish_outputs=False)
        t_inc = time.perf_counter() - t
        t = time.perf_counter()
        full = run_pipeline([late], work / "wh_full.db", work / "out_full", cfg, rebuild_sessions=True,
                            publish_outputs=False)
        t_full = time.perf_counter() - t
        assert inc.status == full.status == "SUCCESS"
        c1, c2 = sqlite3.connect(work / "wh.db"), sqlite3.connect(work / "wh_full.db")
        q = "SELECT session_id, event_count, session_start_ts, session_end_ts FROM sessions ORDER BY session_id"
        same = c1.execute(q).fetchall() == c2.execute(q).fetchall()
        c1.close(), c2.close()

        m = res.metrics
        st = m["stage_timings_seconds"]
        lines = [
            "# Performance measurement",
            "",
            f"Machine: {platform.platform()}, Python {platform.python_version()}, SQLite {sqlite3.sqlite_version}",
            "",
            f"## A. Initial load: {summary['total_input_rows']:,} input rows "
            f"({a.events:,} valid events, {n_users:,} users, {len(summary['files'])} files)",
            "",
            "| stage | seconds |",
            "|---|---:|",
            f"| generate dataset (not part of pipeline) | {t_gen:.2f} |",
            *[f"| {k} | {v:.2f} |" for k, v in st.items()],
            f"| **total pipeline run** | **{t_run:.2f}** |",
            "",
            f"Throughput: **{m['input_rows'] / t_run:,.0f} rows/s**; session rows written: "
            f"{m['sessions_written']:,} (users recomputed {m['users_recomputed']:,} times for "
            f"{n_users:,} users), DQ checks failed: {m['dq_checks_failed']}.",
            "",
            "### Optimization: rebuild each user's sessions once per run (`sessionize_mode = per_run`)",
            "",
            "Profiling showed sessionization is the largest cost: with the default `per_file` mode a user "
            "present in every batch file is rebuilt once per file. `per_run` queues affected users "
            "(crash-safe, in the file's transaction) and rebuilds them once at the end.",
            "",
            "| mode | user recomputations | sessionization s | total s | rows/s |",
            "|---|---:|---:|---:|---:|",
            f"| per_file (default) | {m['users_recomputed']:,} | n/a (inside ingest) | {t_run:.2f} | {m['input_rows'] / t_run:,.0f} |",
            f"| per_run | {res_pr.metrics['users_recomputed']:,} | "
            f"{res_pr.metrics['stage_timings_seconds']['sessionize_queued_or_rebuild']:.2f} | {t_run_pr:.2f} | "
            f"{res_pr.metrics['input_rows'] / t_run_pr:,.0f} |",
            "",
            f"Identical session tables in both modes: **{same_pr}**. Speed-up: **{t_run / t_run_pr:.2f}x**.",
            "",
            f"## B. Late batch of {a.late_events} events touching up to {a.late_events} users",
            "",
            "| strategy | users recomputed | seconds |",
            "|---|---:|---:|",
            f"| incremental (affected users only, default) | {inc.metrics['users_recomputed']:,} | {t_inc:.2f} |",
            f"| full recompute (`--rebuild-sessions`) | all {n_users:,} | {t_full:.2f} |",
            "",
            f"Identical session tables after both strategies: **{same}**. "
            f"Speed-up of incremental: **{t_full / t_inc:.1f}x**.",
            "",
        ]
        Path(a.report).parent.mkdir(parents=True, exist_ok=True)
        Path(a.report).write_text("\n".join(lines), encoding="utf-8")
        print("\n".join(lines))
        return 0
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
