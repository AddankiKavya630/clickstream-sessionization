"""Seeded synthetic clickstream generator.

Produces input files (users + N event batches) and, in a SEPARATE folder,
the expected answers derived from the generator's own ground truth:

  <out>/input/users.csv
  <out>/input/events/events_batch_001.csv ...
  <out>/expected/expected_sessions.csv         one row per true session
  <out>/expected/expected_event_sessions.csv   event_id -> true session key
  <out>/expected/expected_rejects.csv          labelled invalid / duplicate rows (file, line, reason)
  <out>/expected/expected_summary.json         headline counts

Ground-truth sessions are *constructed* (gaps inside a session are <= gap,
gaps between sessions are > gap), not computed by the pipeline's SQL, so
comparing the two is an independent correctness check.
"""
from __future__ import annotations

import csv
import json
import random
from dataclasses import asdict, dataclass
from datetime import timedelta, timezone
from pathlib import Path

from .schema import SOURCE_COLUMNS, Reason
from .timeutil import format_utc, from_epoch_ms, parse_iso_utc, to_epoch_ms

IST = timezone(timedelta(hours=5, minutes=30))
EVENT_TYPES = ["page_view"] * 10 + ["click"] * 6 + ["search"] * 3 + ["add_to_cart"] * 2 + \
              ["remove_from_cart", "purchase", "login", "logout"]
DEVICES = ["desktop", "mobile", "tablet"]
COUNTRIES = ["IN", "US", "GB", "AU", "DE", "SG"]

# kind -> (expected reason, how to corrupt a row). Each invalid row breaks exactly ONE rule.
INVALID_KINDS = [
    ("missing_event_ts", Reason.MISSING_EVENT_TS),
    ("unparseable_event_ts", Reason.INVALID_EVENT_TS),
    ("future_event_ts", Reason.FUTURE_EVENT_TS),
    ("ancient_event_ts", Reason.EVENT_TS_OUT_OF_RANGE),
    ("unknown_user", Reason.INVALID_USER_REF),
    ("unknown_event_type", Reason.UNKNOWN_EVENT_TYPE),
    ("missing_user_id", Reason.MISSING_REQUIRED_FIELD),
    ("missing_event_id", Reason.MISSING_REQUIRED_FIELD),
    ("bad_event_id", Reason.INVALID_EVENT_ID),
    ("malformed_row", Reason.MALFORMED_ROW),
]


@dataclass
class GeneratorConfig:
    n_users: int = 25
    n_events: int = 400                 # clean (valid, unique) events
    seed: int = 42
    n_batches: int = 2
    start: str = "2026-11-01T00:00:00Z"
    session_gap_seconds: int = 1800
    duplicate_rate: float = 0.03        # exact re-sends
    conflict_rate: float = 0.005        # same event_id, different page_url
    late_rate: float = 0.04             # arrive 1-3 days late (usually in a later batch)
    invalid_rate: float = 0.03          # labelled invalid rows (spread over INVALID_KINDS)
    local_shuffle_rate: float = 0.05    # extra out-of-order swaps inside a batch


def _render_ts(rng: random.Random, ms: int) -> str:
    dt = from_epoch_ms(ms)
    r = rng.random()
    if r < 0.10:
        return dt.astimezone(IST).isoformat(timespec="milliseconds")           # +05:30 offset
    if r < 0.14:
        return dt.isoformat(sep=" ", timespec="milliseconds")                  # space separator, +00:00
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def generate(out_dir: str | Path, gc: GeneratorConfig) -> dict:
    rng = random.Random(gc.seed)
    out = Path(out_dir)
    in_dir, ev_dir, exp_dir = out / "input", out / "input" / "events", out / "expected"
    for d in (ev_dir, exp_dir):
        d.mkdir(parents=True, exist_ok=True)
    for old in list(ev_dir.glob("events_batch_*.csv")):
        old.unlink()

    start_ms = to_epoch_ms(parse_iso_utc(gc.start))
    gap_ms = gc.session_gap_seconds * 1000
    users = [f"u_{i:06d}" for i in range(1, gc.n_users + 1)]

    # ---- users
    with (in_dir / "users.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["user_id", "signup_ts", "country"])
        for u in users:
            signup = from_epoch_ms(start_ms - rng.randint(1, 400) * 86_400_000)
            w.writerow([u, format_utc(signup), rng.choice(COUNTRIES)])

    # ---- clean events with constructed sessions
    quota = {u: gc.n_events // gc.n_users for u in users}
    for u in rng.sample(users, gc.n_events % gc.n_users):
        quota[u] += 1

    clean: list[dict] = []
    expected_sessions: list[dict] = []
    counter = 0
    for u in users:
        cursor = start_ms + rng.randint(0, 12 * 3_600_000)
        seq = 0
        remaining = quota[u]
        while remaining > 0:
            seq += 1
            if seq > 1:   # gap BETWEEN sessions: always > gap; sometimes exactly gap + 1s (boundary)
                cursor += gap_ms + 1000 if rng.random() < 0.05 else rng.randint(gap_ms + 60_000, 36 * 3_600_000)
            size = min(remaining, 1 + min(int(rng.expovariate(1 / 5)), 40))
            device = rng.choice(DEVICES) if rng.random() > 0.05 else None
            key = f"{u}#{seq}"
            first_ms = cursor
            for i in range(size):
                if i > 0:  # gap INSIDE a session: always <= gap; sometimes exactly gap or 0
                    r = rng.random()
                    cursor += gap_ms if r < 0.04 else 0 if r < 0.07 else rng.randint(1000, 25 * 60_000)
                counter += 1
                clean.append({
                    "event_id": f"evt_{counter:09d}", "user_id": u,
                    "event_type": "page_view" if i == 0 else rng.choice(EVENT_TYPES),
                    "ts_ms": cursor, "page_url": f"/p/{rng.randint(1, 300)}" if rng.random() > 0.03 else None,
                    "device": device, "session_key": key,
                })
            expected_sessions.append({"expected_session_key": key, "user_id": u,
                                      "session_start_ts": format_utc(from_epoch_ms(first_ms)),
                                      "session_end_ts": format_utc(from_epoch_ms(cursor)),
                                      "event_count": size})
            remaining -= size

    # ---- arrival model: rows = clean + duplicates + conflicts + invalid
    rows: list[dict] = []
    for e in clean:
        late = rng.random() < gc.late_rate
        delay = rng.randint(86_400_000, 3 * 86_400_000) if late else rng.randint(0, 120_000)
        rows.append({**e, "kind": "clean", "arrival": e["ts_ms"] + delay})
    for e in rng.sample(clean, int(len(clean) * gc.duplicate_rate)):
        rows.append({**e, "kind": "dup_exact", "arrival": e["ts_ms"] + rng.randint(1000, 2 * 86_400_000)})
    for e in rng.sample(clean, int(len(clean) * gc.conflict_rate)):
        rows.append({**e, "kind": "dup_conflict", "page_url": f"/conflict/{rng.randint(1, 99)}",
                     "arrival": e["ts_ms"] + rng.randint(1000, 2 * 86_400_000)})
    n_invalid = max(len(INVALID_KINDS), int(len(clean) * gc.invalid_rate)) if gc.invalid_rate > 0 else 0
    span = max(clean[-1]["ts_ms"] - start_ms, 1) if clean else 1
    for k in range(n_invalid):
        kind, reason = INVALID_KINDS[k % len(INVALID_KINDS)]
        ts = start_ms + rng.randint(0, span)
        rows.append({"event_id": f"evt_bad_{k + 1:06d}", "user_id": rng.choice(users), "event_type": "click",
                     "ts_ms": ts, "page_url": "/p/1", "device": "desktop", "session_key": None,
                     "kind": kind, "expected_reason": reason, "arrival": ts + rng.randint(0, 120_000)})

    rows.sort(key=lambda r: (r["arrival"], r["event_id"]))
    n_batches = max(1, gc.n_batches)
    size = -(-len(rows) // n_batches) if rows else 0
    batches = [rows[i * size:(i + 1) * size] for i in range(n_batches)]
    for b in batches:  # extra local out-of-order swaps
        for _ in range(int(len(b) * gc.local_shuffle_rate)):
            i = rng.randrange(len(b))
            j = min(len(b) - 1, max(0, i + rng.randint(-5, 5)))
            b[i], b[j] = b[j], b[i]

    # ---- write batches, labelling every non-clean line by simulating first-seen order
    expected_rejects: list[dict] = []
    first_seen: dict[str, tuple] = {}
    files = []
    for bi, batch in enumerate(batches, start=1):
        name = f"events_batch_{bi:03d}.csv"
        files.append(name)
        with (ev_dir / name).open("w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(SOURCE_COLUMNS)
            for line_no, r in enumerate(batch, start=2):
                ts_text = _render_ts(rng, r["ts_ms"])
                rec = [r["event_id"], r["user_id"], r["event_type"], ts_text, r["page_url"] or "", r["device"] or ""]
                kind = r["kind"]
                if kind in ("clean", "dup_exact", "dup_conflict"):
                    payload = (r["user_id"], r["event_type"], r["ts_ms"], r["page_url"], r["device"])
                    prior = first_seen.get(r["event_id"])
                    if prior is None:
                        first_seen[r["event_id"]] = payload
                    else:
                        reason = Reason.DUPLICATE_EXACT if prior == payload else Reason.DUPLICATE_CONFLICT
                        expected_rejects.append({"source_file": name, "source_line": line_no,
                                                 "event_id": r["event_id"], "expected_record_class": "duplicate",
                                                 "expected_reason_code": reason})
                    w.writerow(rec)
                    continue
                # invalid kinds
                if kind == "missing_event_ts":
                    rec[3] = ""
                elif kind == "unparseable_event_ts":
                    rec[3] = rng.choice(["not-a-timestamp", "2026-13-45T25:61:00Z", "1730000000", "2026-11-03"])
                elif kind == "future_event_ts":
                    rec[3] = "2099-01-01T00:00:00Z"
                elif kind == "ancient_event_ts":
                    rec[3] = "1999-12-31T23:59:59Z"
                elif kind == "unknown_user":
                    rec[1] = "u_999999"
                elif kind == "unknown_event_type":
                    rec[2] = "hover"
                elif kind == "missing_user_id":
                    rec[1] = ""
                elif kind == "missing_event_id":
                    rec[0] = ""
                elif kind == "bad_event_id":
                    rec[0] = f"bad id #{r['event_id'][-6:]}"
                expected_rejects.append({"source_file": name, "source_line": line_no,
                                         "event_id": rec[0] or None, "expected_record_class": "invalid",
                                         "expected_reason_code": r["expected_reason"]})
                if kind == "malformed_row":
                    fh.flush()
                    w.writerow(rec + ["UNEXPECTED_EXTRA_FIELD"])
                else:
                    w.writerow(rec)

    # ---- expected answers (kept apart from the input)
    def write_csv(path: Path, cols: list[str], items: list[dict]) -> None:
        with path.open("w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n", extrasaction="ignore")
            w.writeheader()
            w.writerows(items)

    write_csv(exp_dir / "expected_sessions.csv",
              ["expected_session_key", "user_id", "session_start_ts", "session_end_ts", "event_count"],
              expected_sessions)
    write_csv(exp_dir / "expected_event_sessions.csv", ["event_id", "expected_session_key"],
              [{"event_id": e["event_id"], "expected_session_key": e["session_key"]} for e in clean])
    write_csv(exp_dir / "expected_rejects.csv",
              ["source_file", "source_line", "event_id", "expected_record_class", "expected_reason_code"],
              expected_rejects)

    by_reason: dict[str, int] = {}
    for r in expected_rejects:
        by_reason[r["expected_reason_code"]] = by_reason.get(r["expected_reason_code"], 0) + 1
    summary = {
        "generator_config": asdict(gc),
        "files": files,
        "users": len(users),
        "total_input_rows": len(rows),
        "expected_valid_unique_events": len(clean),
        "expected_sessions": len(expected_sessions),
        "expected_quarantined_rows": len(expected_rejects),
        "expected_quarantine_by_reason": dict(sorted(by_reason.items())),
        "late_events_injected": sum(1 for r in rows if r["kind"] == "clean" and r["arrival"] - r["ts_ms"] > 120_000),
    }
    (exp_dir / "expected_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
