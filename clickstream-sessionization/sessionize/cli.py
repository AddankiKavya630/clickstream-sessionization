"""Command line interface: python -m sessionize <command> ..."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from .config import load_config
from .db import connect, load_named_sql
from .expected import render_verification_markdown, verify_against_expected
from .generator import GeneratorConfig, generate
from .pipeline import run_pipeline
from .publish import _atomic_write_text
from .timeutil import parse_iso_utc

PROFILES = {
    "small": dict(n_users=25, n_events=400, n_batches=2),
    "large": dict(n_users=5000, n_events=100_000, n_batches=4),
    "xlarge": dict(n_users=20000, n_events=1_000_000, n_batches=10),
}


def _setup_console_logging(verbose: bool) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")


def cmd_generate(a) -> int:
    params = dict(PROFILES[a.profile])
    for k in ("n_users", "n_events", "n_batches", "seed"):
        if getattr(a, k) is not None:
            params[k] = getattr(a, k)
    summary = generate(a.out, GeneratorConfig(**params))
    print(json.dumps({k: v for k, v in summary.items() if k != "generator_config"}, indent=2))
    return 0


def cmd_run(a) -> int:
    cfg = load_config(a.config).with_overrides(
        session_gap_seconds=int(a.gap_minutes * 60) if a.gap_minutes is not None else None,
        reference_time=parse_iso_utc(a.reference_time) if a.reference_time else None,
        sessionize_mode=a.sessionize_mode,
    )
    cfg.validate()
    res = run_pipeline(a.input, a.db, a.out, cfg, users_path=a.users, force=a.force,
                       rebuild_sessions=a.rebuild_sessions)
    m = res.metrics
    print(f"\nrun_id={res.run_id} status={res.status} exit_code={res.exit_code}")
    print(f"input_rows={m['input_rows']} inserted={m['events_inserted']} duplicates={m['duplicate_rows']} "
          f"already_loaded={m['already_loaded_rows']} rejected={m['rejected_rows']} "
          f"files skipped={m['files_skipped']} failed={m['files_failed']} rejected={m['files_rejected']}")
    print(f"dq_checks_failed={m['dq_checks_failed']}  report={res.report_path}")
    if res.failure_reason:
        print(f"failure_reason: {res.failure_reason}")
    return res.exit_code


def cmd_verify(a) -> int:
    conn = connect(a.db)
    try:
        res = verify_against_expected(conn, a.expected)
    finally:
        conn.close()
    md = render_verification_markdown(res)
    if a.report:
        _atomic_write_text(Path(a.report), md)
    print(md)
    if not res["passed"]:
        print(json.dumps({"session_mismatch_examples": res["session_mismatch_examples"],
                          "reject_examples": res["reject_examples"]}, indent=2, default=str))
    return 0 if res["passed"] else 1


def cmd_report(a) -> int:
    conn = connect(a.db)
    try:
        for name, desc, stmt in load_named_sql("sample_queries.sql"):
            cur = conn.execute(stmt)
            cols = [d[0] for d in cur.description]
            rows = [[("" if v is None else str(v)) for v in r] for r in cur.fetchall()]
            widths = [max(len(c), *(len(r[i]) for r in rows)) if rows else len(c) for i, c in enumerate(cols)]
            print(f"\n== {name}: {desc}")
            print("  ".join(c.ljust(w) for c, w in zip(cols, widths)))
            print("  ".join("-" * w for w in widths))
            for r in rows:
                print("  ".join(v.ljust(w) for v, w in zip(r, widths)))
    finally:
        conn.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m sessionize",
                                description="Clickstream sessionization pipeline (event-time gaps).")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="generate a reproducible synthetic dataset + expected answers")
    g.add_argument("--out", required=True, help="output folder")
    g.add_argument("--profile", choices=sorted(PROFILES), default="small")
    g.add_argument("--users", dest="n_users", type=int)
    g.add_argument("--events", dest="n_events", type=int, help="number of clean events")
    g.add_argument("--batches", dest="n_batches", type=int)
    g.add_argument("--seed", type=int)
    g.set_defaults(func=cmd_generate)

    r = sub.add_parser("run", help="ingest, validate, sessionize, check and publish")
    r.add_argument("--input", nargs="+", required=True, help="event files and/or folders")
    r.add_argument("--users", help="users reference CSV (upserted into dim_users)")
    r.add_argument("--db", default="output/warehouse.db")
    r.add_argument("--out", default="output")
    r.add_argument("--config", help="TOML config (default: config/default.toml)")
    r.add_argument("--gap-minutes", type=float, help="override inactivity gap")
    r.add_argument("--reference-time", help="ISO-8601 'now' for the future-timestamp rule (reproducible runs)")
    r.add_argument("--sessionize-mode", choices=["per_file", "per_run"], help="override config")
    r.add_argument("--force", action="store_true", help="re-process files already loaded (still no duplicates)")
    r.add_argument("--rebuild-sessions", action="store_true", help="recompute sessions for ALL users (backfill / gap change)")
    r.set_defaults(func=cmd_run)

    v = sub.add_parser("verify", help="compare the warehouse with generator expected answers")
    v.add_argument("--db", default="output/warehouse.db")
    v.add_argument("--expected", required=True)
    v.add_argument("--report", help="write the markdown result here")
    v.set_defaults(func=cmd_verify)

    q = sub.add_parser("report", help="print sample analytical and operational queries")
    q.add_argument("--db", default="output/warehouse.db")
    q.set_defaults(func=cmd_report)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _setup_console_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
