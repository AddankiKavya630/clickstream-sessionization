"""Compare warehouse contents with the generator's separately stored expected answers."""
from __future__ import annotations

import csv
import json
import sqlite3
from collections import defaultdict
from pathlib import Path, PurePath


def _read_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def verify_against_expected(conn: sqlite3.Connection, expected_dir: str | Path) -> dict:
    exp = Path(expected_dir)
    summary = json.loads((exp / "expected_summary.json").read_text(encoding="utf-8"))
    exp_sessions = {r["expected_session_key"]: r for r in _read_csv(exp / "expected_sessions.csv")}
    exp_members: dict[str, set[str]] = defaultdict(set)
    for r in _read_csv(exp / "expected_event_sessions.csv"):
        exp_members[r["expected_session_key"]].add(r["event_id"])

    actual_members: dict[str, set[str]] = defaultdict(set)
    event_to_session: dict[str, str] = {}
    for r in conn.execute("SELECT event_id, session_id FROM session_events"):
        actual_members[r[1]].add(r[0])
        event_to_session[r[0]] = r[1]
    act_sessions = {r["session_id"]: dict(r) for r in conn.execute(
        "SELECT session_id, user_id, session_start_ts, session_end_ts, event_count FROM sessions")}

    session_mismatches = []
    for key, members in exp_members.items():
        sids = {event_to_session.get(e) for e in members}
        if len(sids) != 1 or None in sids:
            session_mismatches.append({"expected_session_key": key, "problem": "events split or missing",
                                       "actual_session_ids": sorted(s or "<none>" for s in sids)})
            continue
        sid = sids.pop()
        e, a = exp_sessions[key], act_sessions[sid]
        if actual_members[sid] != members or (e["user_id"], e["session_start_ts"], e["session_end_ts"],
                                              int(e["event_count"])) != (a["user_id"], a["session_start_ts"],
                                                                         a["session_end_ts"], a["event_count"]):
            session_mismatches.append({"expected_session_key": key, "problem": "boundaries differ",
                                       "expected": e, "actual": a})

    exp_rejects = {(r["source_file"], int(r["source_line"])): r["expected_reason_code"]
                   for r in _read_csv(exp / "expected_rejects.csv")}
    act_rejects = {(PurePath(r[0]).name, r[1]): r[2] for r in conn.execute(
        "SELECT source_file, source_line, reason_code FROM rejected_events WHERE record_class <> 'file'")}
    missing = sorted(set(exp_rejects) - set(act_rejects))
    unexpected = sorted(set(act_rejects) - set(exp_rejects))
    wrong_reason = sorted(k for k in set(exp_rejects) & set(act_rejects) if exp_rejects[k] != act_rejects[k])

    n_events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    result = {
        "expected_valid_unique_events": summary["expected_valid_unique_events"],
        "actual_events": n_events,
        "expected_sessions": summary["expected_sessions"],
        "actual_sessions": len(act_sessions),
        "session_mismatches": len(session_mismatches),
        "session_mismatch_examples": session_mismatches[:5],
        "expected_quarantined_rows": len(exp_rejects),
        "actual_quarantined_rows": len(act_rejects),
        "rejects_missing": len(missing),
        "rejects_unexpected": len(unexpected),
        "rejects_wrong_reason": len(wrong_reason),
        "reject_examples": {"missing": missing[:5], "unexpected": unexpected[:5], "wrong_reason": wrong_reason[:5]},
    }
    result["passed"] = (n_events == summary["expected_valid_unique_events"]
                        and len(act_sessions) == summary["expected_sessions"]
                        and not session_mismatches and not missing and not unexpected and not wrong_reason)
    return result


def render_verification_markdown(res: dict) -> str:
    ok = lambda a, b: "yes" if a == b else "**NO**"
    return "\n".join([
        "# Validation against expected answers",
        "",
        f"**Overall: {'PASS' if res['passed'] else 'FAIL'}**",
        "",
        "| measure | expected | actual | match |",
        "|---|---:|---:|:---:|",
        f"| valid unique events | {res['expected_valid_unique_events']} | {res['actual_events']} | "
        f"{ok(res['expected_valid_unique_events'], res['actual_events'])} |",
        f"| sessions | {res['expected_sessions']} | {res['actual_sessions']} | "
        f"{ok(res['expected_sessions'], res['actual_sessions'])} |",
        f"| sessions with wrong membership/boundaries | 0 | {res['session_mismatches']} | {ok(0, res['session_mismatches'])} |",
        f"| quarantined rows (file, line) | {res['expected_quarantined_rows']} | {res['actual_quarantined_rows']} | "
        f"{ok(res['expected_quarantined_rows'], res['actual_quarantined_rows'])} |",
        f"| labelled rows not quarantined | 0 | {res['rejects_missing']} | {ok(0, res['rejects_missing'])} |",
        f"| quarantined rows not labelled | 0 | {res['rejects_unexpected']} | {ok(0, res['rejects_unexpected'])} |",
        f"| quarantined with a different reason | 0 | {res['rejects_wrong_reason']} | {ok(0, res['rejects_wrong_reason'])} |",
        "",
    ])
