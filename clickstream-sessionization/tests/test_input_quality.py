"""Empty input, malformed records, missing fields / nulls, invalid values, schema changes."""
import json

from tests.conftest import ev


# ---------------------------------------------------------------- empty batches

def test_no_input_files_is_an_empty_successful_batch(env):
    res = env.run()
    assert res.status == "SUCCESS"
    assert res.metrics["input_rows"] == 0
    assert env.scalar("SELECT COUNT(*) FROM sessions") == 0


def test_header_only_and_zero_byte_files(env):
    env.file("batch_001.csv", [])
    (env.inbox / "batch_002.csv").write_bytes(b"")
    (env.inbox / "batch_003.jsonl").write_text("\n\n", encoding="utf-8")
    res = env.run()
    assert res.status == "SUCCESS"
    assert [f.status for f in res.files] == ["SUCCESS", "SUCCESS", "SUCCESS"]
    assert res.metrics["input_rows"] == 0
    assert all(c.passed for c in res.checks)


# ---------------------------------------------------------------- malformed

def test_malformed_csv_rows_are_quarantined_with_source_line(env):
    env.file("batch_001.csv", [
        ev("e1", "2026-11-01T10:00:00Z"),
        "e2,u1,click,2026-11-01T10:01:00Z,/a,desktop,EXTRA_FIELD",   # line 3: 7 fields
        "e3,u1,click",                                                # line 4: 3 fields
        ev("e4", "2026-11-01T10:02:00Z"),
    ])
    res = env.run()
    assert env.reasons() == {3: "MALFORMED_ROW", 4: "MALFORMED_ROW"}
    raw = env.scalar("SELECT raw_record FROM rejected_events WHERE source_line = 3")
    assert json.loads(raw)[-1] == "EXTRA_FIELD"           # original content preserved for replay
    assert res.metrics["events_inserted"] == 2 and res.status == "SUCCESS"


def test_malformed_jsonl(env):
    (env.inbox / "batch_001.jsonl").write_text("\n".join([
        json.dumps(ev("e1", "2026-11-01T10:00:00Z")),
        '{"event_id": "e2", "user_id": "u1"',                  # truncated JSON
        '["not", "an", "object"]',
        json.dumps({**ev("e3", "2026-11-01T10:05:00Z"), "page_url": None}),
    ]) + "\n", encoding="utf-8")
    env.run()
    assert env.reasons() == {2: "MALFORMED_ROW", 3: "MALFORMED_ROW"}
    assert env.q("SELECT event_id, page_url FROM events ORDER BY event_id") == [("e1", "/home"), ("e3", None)]


# ---------------------------------------------------------------- missing / null fields

def test_missing_required_fields_and_missing_timestamp(env):
    env.file("batch_001.csv", [
        ev("", "2026-11-01T10:00:00Z"),                  # 2: no event_id
        ev("e2", "2026-11-01T10:00:00Z", user=""),       # 3: no user_id
        ev("e3", "2026-11-01T10:00:00Z", etype="  "),    # 4: blank event_type
        ev("e4", ""),                                    # 5: no timestamp
        ev("e5", "   "),                                 # 6: whitespace timestamp
        ev("e6", "2026-11-01T10:00:00Z", page="", device=""),   # 7: optional fields empty -> NULL, accepted
    ])
    env.run()
    assert env.reasons() == {2: "MISSING_REQUIRED_FIELD", 3: "MISSING_REQUIRED_FIELD",
                             4: "MISSING_REQUIRED_FIELD", 5: "MISSING_EVENT_TS", 6: "MISSING_EVENT_TS"}
    assert env.q("SELECT event_id, page_url, device FROM events") == [("e6", None, None)]
    assert env.scalar("SELECT landing_page FROM sessions") is None


def test_multiple_errors_keep_all_details_with_priority_reason(env):
    env.file("batch_001.csv", [ev("e1", "", user="nobody", etype="hover")])
    env.run()
    code, detail = env.q("SELECT reason_code, reason_detail FROM rejected_events")[0]
    assert code == "MISSING_EVENT_TS"
    assert "hover" in detail and "nobody" in detail


# ---------------------------------------------------------------- invalid values

def test_invalid_values(env):
    env.file("batch_001.csv", [
        ev("e1", "not-a-date"),                          # 2
        ev("e2", "2026-11-03"),                          # 3 date without time
        ev("e3", "2026-13-01T10:00:00Z"),                # 4 month 13
        ev("e4", "2099-01-01T00:00:00Z"),                # 5 future
        ev("e5", "1999-12-31T23:59:59Z"),                # 6 too old
        ev("e6", "2026-11-01T10:00:00Z", etype="hover"), # 7 unknown type
        ev("bad id!", "2026-11-01T10:00:00Z"),           # 8 bad id pattern
        ev("e8", "2026-11-01T10:00:00Z", user="u_404"),  # 9 invalid reference key
        ev("e9", "2026-11-01T10:00:00Z", etype="CLICK"), # 10 valid: type normalized to lower case
    ])
    env.run()
    assert env.reasons() == {2: "INVALID_EVENT_TS", 3: "INVALID_EVENT_TS", 4: "INVALID_EVENT_TS",
                             5: "FUTURE_EVENT_TS", 6: "EVENT_TS_OUT_OF_RANGE", 7: "UNKNOWN_EVENT_TYPE",
                             8: "INVALID_EVENT_ID", 9: "INVALID_USER_REF"}
    assert env.q("SELECT event_id, event_type FROM events") == [("e9", "click")]


def test_future_rule_uses_reference_time_and_skew(env):
    env.cfg = env.cfg.with_overrides(max_future_skew_seconds=600)
    env.file("batch_001.csv", [ev("e1", "2026-12-31T00:10:00Z"),     # exactly at the limit: accepted
                               ev("e2", "2026-12-31T00:10:00.001Z")])  # 1 ms over: rejected
    env.run()
    assert env.reasons() == {3: "FUTURE_EVENT_TS"}


def test_reference_key_checked_against_users_file(env):
    env.file("batch_001.csv", [ev("e1", "2026-11-01T10:00:00Z", user="u3"),
                               ev("e2", "2026-11-01T10:00:00Z", user="u4")])
    res = env.run()
    assert env.reasons() == {3: "INVALID_USER_REF"}
    assert next(c for c in res.checks if c.name == "ri_events_users").passed


# ---------------------------------------------------------------- schema change

def test_missing_required_column_rejects_whole_file_others_still_load(env):
    env.file("batch_001.csv", [["e1", "u1", "page_view", "2026-11-01T10:00:00Z"]],
             columns=("event_id", "user_id", "event_type", "timestamp"))   # event_ts renamed upstream
    env.file("batch_002.csv", [ev("e2", "2026-11-01T10:00:00Z")])
    res = env.run()
    assert res.status == "PARTIAL_SUCCESS" and res.exit_code == 2
    assert [f.status for f in res.files] == ["REJECTED", "SUCCESS"]
    assert "event_ts" in res.files[0].error
    assert env.q("SELECT reason_code, source_line, record_class FROM rejected_events") == \
        [("FILE_SCHEMA_MISMATCH", 0, "file")]
    assert env.q("SELECT event_id FROM events") == [("e2",)]
    assert env.scalar("SELECT status FROM file_ingestion_log WHERE source_file LIKE '%batch_001.csv'") == "REJECTED"


def test_extra_and_reordered_columns_are_accepted_with_warning(env):
    cols = ("device", "event_ts", "user_id", "event_id", "event_type", "page_url", "campaign_id")
    env.file("batch_001.csv", [["mobile", "2026-11-01T10:00:00Z", "u1", "e1", "click", "/x", "cmp_9"]], columns=cols)
    res = env.run()
    assert res.status == "SUCCESS"
    assert res.files[0].schema_warnings == ["SCHEMA_DRIFT: unexpected column(s) ignored: campaign_id"]
    assert env.q("SELECT event_id, user_id, device FROM events") == [("e1", "u1", "mobile")]
    assert "campaign_id" in env.scalar("SELECT schema_warnings FROM file_ingestion_log")


def test_header_with_bom_whitespace_and_upper_case(env):
    path = env.inbox / "batch_001.csv"
    path.write_bytes("﻿ Event_ID ,USER_ID,event_type,Event_TS,page_url,device\ne1,u1,click,2026-11-01T10:00:00Z,,\n"
                     .encode("utf-8"))
    env.run()
    assert env.q("SELECT event_id FROM events") == [("e1",)]


def test_users_file_without_user_id_column_fails_the_run(env):
    env.users.write_text("id,country\nu1,IN\n", encoding="utf-8")
    env.file("batch_001.csv", [ev("e1", "2026-11-01T10:00:00Z")])
    res = env.run()
    assert res.status == "FAILED" and res.exit_code == 1
    assert "user_id" in res.failure_reason
