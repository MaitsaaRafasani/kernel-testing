"""Timeline (repeated rows, row order, HH:MM:SS) + excel <-> yaml (1 excel = 1 usecase = 1 timeline + expected)."""
from datetime import datetime, time, timedelta, timezone

import pytest
from openpyxl import Workbook, load_workbook

from harness.config import ROOT, load_yaml
from harness.excel import case_to_excel, excel_to_case, excel_to_expected, from_cell
from harness.loader import as_scenarios
from harness.timeline import expand_steps, format_duration, parse_duration

NOW = datetime(2026, 11, 18, 3, 0, 0, tzinfo=timezone.utc)
CASE = ROOT / "cases" / "device_normal.yaml"
EXPECTED = ROOT / "versions" / "v2" / "expected" / "device_normal.yaml"


def expanded(case: dict, family: str) -> list[tuple]:
    scenario = as_scenarios(case)[0]
    return [(s.packet, s.at, s.id, s.batch, s.fields) for s in expand_steps(scenario, {}, NOW, family=family)]


def sent_seconds(scenario: dict, family: str | None = None) -> list[tuple]:
    return [(int((s.at - NOW).total_seconds()), s.packet) for s in expand_steps(scenario, {}, NOW, family=family)]


def test_repeated_rows_interpolation_and_carry_over():
    scenario = {"start": "now-01:00:00", "timeline": [
        {"from": "00:00:00", "to": "00:01:00", "every": "00:00:10", "packet": "location", "first_step_name": "a",
         "step_name": "b", "lng": [106.0, 106.5], "acc": True},
        {"from": "00:01:00", "to": "00:11:00", "every": "00:05:00", "packet": "location", "acc": False},
        {"from": "00:00:30", "to": "00:02:00", "every": "00:01:00", "packet": "heartbeat", "families": ["concox"]},
    ]}
    steps = expand_steps(scenario, {}, NOW, family="concox")
    start = NOW - timedelta(hours=1)
    locations = [s for s in steps if s.packet == "location"]
    assert len(locations) == 6 + 2
    assert [s.fields["longitude"] for s in locations[:6]] == [106.0, 106.1, 106.2, 106.3, 106.4, 106.5]
    assert (locations[0].id, locations[5].id) == ("a", "b")
    assert locations[6].fields == {"longitude": 106.5, "engine_on": False}           # carry-over
    assert [s.at - start for s in steps if s.packet == "heartbeat"] == [timedelta(seconds=30), timedelta(seconds=90)]
    assert [s.at for s in steps] == sorted(s.at for s in steps)
    assert not [s for s in expand_steps(scenario, {}, NOW, family="teltonika") if s.packet == "heartbeat"]


def test_family_skip_does_not_shift_time():
    scenario = {"start": "now", "interval": "00:00:30", "timeline": [
        {"packet": "login"}, {"packet": "heartbeat", "families": ["concox"]}, {"packet": "location"},
    ]}
    assert sent_seconds(scenario, "teltonika") == [(0, "login"), (60, "location")]


def test_same_time_follows_row_order():
    scenario = {"start": "now", "timeline": [
        {"from": "00:00:10", "to": "00:01:00", "every": "00:00:10", "packet": "location"},
        {"from": "00:00:30", "packet": "heartbeat"},
    ]}
    assert sent_seconds(scenario)[2:4] == [(30, "location"), (30, "heartbeat")]       # location row is above
    scenario["timeline"].reverse()
    assert sent_seconds(scenario)[2:4] == [(30, "heartbeat"), (30, "location")]


def test_clock_format_hh_mm_ss():
    assert parse_duration("00:10:00") == timedelta(minutes=10)
    assert parse_duration("01:02:03") == timedelta(hours=1, minutes=2, seconds=3)
    assert parse_duration("30s") == timedelta(seconds=30)                 # old format still accepted
    assert format_duration(timedelta(minutes=90, seconds=5)) == "01:30:05"
    assert from_cell(time(0, 10, 0)) == "00:10:00"                        # Excel time cell
    assert from_cell("a -> b") == "a -> b"                                # arrow only for numbers
    assert from_cell("1 -> 2") == [1, 2]
    assert from_cell("x -> y", parse_text=False) == "x -> y"


@pytest.mark.parametrize("family", ["concox", "teltonika", "topflytech", "ruptela", "queclink"])
def test_yaml_excel_yaml_round_trip(tmp_path, family):
    case, expected = load_yaml(CASE), load_yaml(EXPECTED)
    path = tmp_path / "device_normal.xlsx"
    case_to_excel(case, {"v2": expected}, path)

    converted = excel_to_case(path)
    assert converted["description"] == case["description"]
    assert converted["db_requirements"] == case["db_requirements"]
    assert expanded(converted, family) == expanded(case, family)
    assert excel_to_expected(path) == {"v2": expected}


def test_excel_layout(tmp_path):
    path = tmp_path / "device_normal.xlsx"
    case_to_excel(load_yaml(CASE), {"v2": load_yaml(EXPECTED)}, path)
    workbook = load_workbook(path)
    assert workbook.sheetnames == ["notes", "db_requirements", "settings", "timeline", "expected_v2"]
    assert workbook["db_requirements"]["A1"].value.startswith("IMEI tiap family")
    header = [cell.value for cell in workbook["timeline"][1]]
    assert header[:7] == ["from", "to", "every", "step_name", "first_step_name", "packet", "families"]
    rows = list(workbook["timeline"].iter_rows(min_row=2, values_only=True))
    location = next(row for row in rows if row[4] == "location")              # repeated row: first_step_name
    assert location[header.index("to")] == "00:01:00"
    drive = next(row for row in rows if row[3] == "trip_start")                # 1 row = 1 packet on the road
    assert isinstance(drive[header.index("lat")], float) and isinstance(drive[header.index("lng")], float)
    expected_header = [cell.value for cell in workbook["expected_v2"][1]]
    assert expected_header == ["step_name", "family", "table", "where", "column", "expected", "note"]


def test_user_example_location_range_and_single_heartbeat(tmp_path):
    workbook = Workbook()
    workbook.active.title = "settings"
    for row in (["key", "value"], ["start", "now"], ["interval", "00:00:10"], ["families", "concox"]):
        workbook["settings"].append(row)
    timeline = workbook.create_sheet("timeline")
    timeline.append(["from", "to", "every", "packet"])
    timeline.append(["00:00:10", "00:01:00", None, "location"])          # every kosong = interval
    timeline.append(["00:00:30", None, None, "heartbeat"])
    path = tmp_path / "example.xlsx"
    workbook.save(path)

    scenario = as_scenarios(excel_to_case(path))[0]
    assert sent_seconds(scenario, "concox") == [
        (10, "location"), (20, "location"), (30, "location"), (30, "heartbeat"), (40, "location"), (50, "location"),
    ]
