"""Stage 2: expected checks to DB (matcher, expected resolve, db_poller with a fake DB)."""
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from harness.db_poller import build_where, check_db, identifier
from harness.expected import CheckContext, load_expected, resolve, steps_for
from harness.matcher import match, same
from harness.runner import Checker, stop_after_failed_check, verify_step

NOW = datetime(2026, 11, 18, 3, 0, 0, tzinfo=timezone.utc)
TOL = 0.00001
FAST = dict(timeout=timedelta(seconds=0.3), poll=timedelta(seconds=0.05), approx_tolerance=TOL,
            clock_skew=timedelta(seconds=5))


class FakeDb:
    """Return rows from a list per call (last one repeats). Record queries."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.queries = []

    async def get_one(self, sql, params):
        self.queries.append((sql, list(params)))
        answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if sql.startswith("SELECT count"):
            return {"count": answer["count"]}
        return answer.get("row")


# --- matcher ---

def test_same_approx_number_datetime_text():
    assert same({"approx": -6.2001}, Decimal("-6.200105"), TOL)
    assert not same({"approx": -6.2001}, -6.2003, TOL)
    assert same({"approx": 7920, "tolerance": 150}, 7800, TOL)
    assert same(30, Decimal("30.0"), TOL)
    assert same(NOW, NOW.replace(tzinfo=None) + timedelta(milliseconds=400), TOL)     # naive = UTC
    assert not same(NOW, NOW + timedelta(seconds=3), TOL)
    assert same("R", "R", TOL) and not same("R", "S", TOL)
    assert same(None, None, TOL) and not same(1, None, TOL)


def test_match_reports_missing_column_and_mismatch():
    problems = match({"type": "M", "nope": 1}, {"type": "P"}, TOL)
    assert len(problems) == 2 and "nope" in problems[1]


# --- expected ---

def test_steps_for_family_block_overrides_default_step():
    expected = {"uc": {"default": {"steps": {"a": {"db": [1]}, "b": {"db": [2]}}},
                       "concox": {"steps": {"b": {"db": [3]}}}}}
    assert steps_for(expected, "uc", "concox") == {"a": {"db": [1]}, "b": {"db": [3]}}
    assert steps_for(expected, "uc", "teltonika") == {"a": {"db": [1]}, "b": {"db": [2]}}


def test_resolve_variables():
    context = CheckContext(
        device={"imei": "358000000000001", "vehicle_id": 7},
        step_times={"start": NOW, "end": NOW + timedelta(minutes=1)},
        step_values={"end": {"port_gate": 41234}},
    )
    check = {"where": {"vehicle_id": "$vehicle_id", "sent_on": {"between": ["$at.start", "$step_at"]}},
             "match": {"port": "$port_gate", "type": "M"}}
    resolved = resolve(check, context, "end")
    assert resolved["where"] == {"vehicle_id": 7, "sent_on": {"between": [NOW, NOW + timedelta(minutes=1)]}}
    assert resolved["match"] == {"port": 41234, "type": "M"}
    with pytest.raises(KeyError):
        resolve("$unknown", context, "end")
    with pytest.raises(KeyError):
        resolve("$at.missing", context, "end")


def test_device_normal_expected_resolves_for_all_steps():
    expected = load_expected("v2", "device_normal")
    steps = steps_for(expected, "device_normal", "concox")
    starts = ["trip_start", "trip_end", "assisted_start", "tow_end", "buffer_trip_start", "buffer_start"]
    context = CheckContext(
        device={"imei": "1", "vehicle_id": 1, "company_id": 1},
        step_times={name: NOW for name in [*starts, "buffer_end", "drive_end", *steps]},
        step_values={name: {"ip_gate": "x", "port_gate": 1, "reply_text": "ok"} for name in steps},
        timeline_start=NOW,
    )
    for name, checks in steps.items():
        for check in checks["db"]:
            assert "$" not in repr(resolve(check, context, name))


# --- db_poller ---

def test_identifier_rejects_injection():
    assert identifier("vehicle_status") == "vehicle_status"
    with pytest.raises(ValueError):
        identifier("vehicle_status; drop table x")


def test_build_where_with_between_and_freshness():
    sql, params = build_where({"vehicle_id": 7, "sent_on": {"between": [1, 2]}}, ("last_receive", NOW))
    assert sql == " WHERE vehicle_id = %s AND sent_on BETWEEN %s AND %s AND last_receive >= %s"
    assert params == [7, 1, 2, NOW]


def test_check_db_polls_until_match():
    db = FakeDb({"row": {"speed": 0}}, {"row": None}, {"row": {"speed": 30}})
    check = {"table": "vehicle_status", "where": {"vehicle_id": 7}, "match": {"speed": 30}}
    result = asyncio.run(check_db(db, check, {"time": "last_receive", "since": "sent"}, NOW, NOW, **FAST))
    assert result.ok and len(db.queries) == 3
    sql, params = db.queries[0]
    assert "ORDER BY last_receive DESC" in sql and params == [7, NOW - timedelta(seconds=5)]


def test_check_db_fails_after_timeout_with_reason():
    db = FakeDb({"count": 19})
    check = {"table": "raw_location", "where": {"vehicle_id": 7}, "count": 20}
    result = asyncio.run(check_db(db, check, {"time": "sent_on", "since": "start"}, NOW, NOW - timedelta(hours=1),
                                  **FAST))
    assert not result.ok and "expected 20, actual 19" in result.message
    assert db.queries[0][1] == [7, NOW - timedelta(hours=1)]          # since start -> timeline start


@pytest.mark.parametrize("stop, expected_done", [(False, 2), (True, 1)])
def test_verify_step_on_check_fail_continue_or_stop(stop, expected_done):
    db = FakeDb({"row": {"battery": 70, "signal_strength": 3}})
    checker = Checker(db=db, tables={}, expected={}, timeout=FAST["timeout"], poll=FAST["poll"],
                      clock_skew=FAST["clock_skew"], approx_tolerance=TOL, stop_on_check_fail=stop)
    checks = {"db": [{"table": "vehicle_status", "match": {"battery": 85}},
                     {"table": "vehicle_status", "match": {"signal_strength": 4}}]}
    context = CheckContext(device={}, step_times={"heartbeat": NOW})
    step = asyncio.run(verify_step(checker, "heartbeat", checks, context, NOW, NOW))
    assert not step.ok and step.done == expected_done and step.passed == 0 and not step.known_bug_only


def test_verify_step_counts_passed_checks_and_known_bug_failures():
    db = FakeDb({"row": {"battery": 70, "signal_strength": 3}})
    checker = Checker(db=db, tables={}, expected={}, timeout=FAST["timeout"], poll=FAST["poll"],
                      clock_skew=FAST["clock_skew"], approx_tolerance=TOL, stop_on_check_fail=True)
    checks = {"db": [{"table": "vehicle_status", "match": {"battery": 85}, "known_bug": "x"},
                     {"table": "vehicle_status", "match": {"signal_strength": 3}}]}
    context = CheckContext(device={}, step_times={"heartbeat": NOW})
    step = asyncio.run(verify_step(checker, "heartbeat", checks, context, NOW, NOW))
    assert not step.ok and step.done == 2 and step.passed == 1 and step.known_bug_only


@pytest.mark.parametrize("keep_going, stop", [(False, True), (True, False)])
def test_failed_check_stops_the_scenario_unless_keep_going(keep_going, stop):
    checker = Checker(db=None, tables={}, expected={}, timeout=FAST["timeout"], poll=FAST["poll"],
                      clock_skew=FAST["clock_skew"], approx_tolerance=TOL, stop_on_check_fail=False,
                      keep_going=keep_going)
    failed = []
    assert stop_after_failed_check(checker, "cek_003500", failed) is stop
    assert failed == ["cek_003500"]                      # always listed: the scenario still ends FAIL


def test_check_db_table_without_time_column_has_no_freshness():
    db = FakeDb({"row": {"ip": "1.2.3.4", "port": "41234"}})
    check = {"table": "device", "where": {"imei": "1"}, "match": {"ip": "1.2.3.4", "port": 41234}}
    result = asyncio.run(check_db(db, check, {}, NOW, NOW, **FAST))
    assert result.ok and "ORDER BY" not in db.queries[0][0] and db.queries[0][1] == ["1"]
