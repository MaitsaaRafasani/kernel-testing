"""Kernel scenario import (.txt -> YAML timeline), carry_over false, today-Nd, H-time, datetime where, pins."""
import importlib
from datetime import datetime, timedelta, timezone

import pytest

from creators.teltonika.creator import TeltonikaCreator
from harness.db_poller import build_where
from harness.excel import expected_rows, parse_where, read_expected
from harness.expected import CheckContext, resolve
from harness.matcher import DATETIME_TOLERANCE
from harness.scenario_import import COORDINATE_TOLERANCE, END_STEP, read_scenario, to_timeline
from harness.timeline import expand_steps, normalize, parse_time

setup = importlib.import_module("versions.v2.setup")

NOW = datetime(2026, 11, 19, 3, 0, 0, tzinfo=timezone.utc)         # 10:00 WIB
WIB = timezone(timedelta(hours=7))
HEADER = "second,Longitude,Latitude,Satellites,Speed,acc,dallasTemperature1,ultrasonicFuel,event,comment\n"
SCENARIO = HEADER + (
    "0,106.857,-6.175,12,25,1,-19.3,45.5,,H1 00:00:00\n"
    "10,106.858,-6.176,12,25,1,-19.0,,,\n"
    "20,106.859,-6.177,12,25,1,-18.7,,,\n"
    "30,106.860,-6.178,12,25,1,-18.4,,,\n"            # 0-30: linear -> 1 repeated row
    "40,106.8605,-6.178,12,25,1,-18.4,,,\n"
    "50,,,,,,,,disconnected,\n"
    "45,106.8607,-6.179,3,0,,-0.4,-0.4,,buffer\n"       # older than previous row: buffered
    "60,106.8607,-6.179,12,0,0,,,,\n"
    "360,106.8607,-6.179,12,0,0,,,,\n"
    "660,106.8607,-6.179,12,0,0,,,,\n"                 # parked every 5 min -> 1 repeated row
)


@pytest.fixture
def scenario(tmp_path):
    path = tmp_path / "device_health_X.txt"
    path.write_text(SCENARIO, encoding="utf-8")
    return path


def test_read_uses_kernel_scales_empty_cell_not_sent_and_buffer_sent_at(scenario):
    rows = read_scenario(scenario)
    first = rows[0][1]
    assert first == {"packet": "location", "lat": -6.175, "lng": 106.857, "altitude": 10, "satellites": 12,
                     "speed": 25, "acc": True, "dallas_temperature_1": -193, "ultrasonic_fuel": 4550}
    assert "ultrasonic_fuel" not in rows[1][1]
    assert rows[5][1] == {"packet": "disconnect"}
    assert rows[6][0] == 45 and rows[6][1]["sent_at"] == 50 and "acc" not in rows[6][1]
    assert rows[6][1]["ultrasonic_fuel"] == -40


def test_unknown_column_is_an_error(tmp_path):
    path = tmp_path / "bad.txt"
    path.write_text("second,Longitude,foo\n0,106.8,1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="foo"):
        read_scenario(path)


def test_timeline_merges_linear_and_constant_runs_and_names_last_step_end(scenario):
    timeline = to_timeline(read_scenario(scenario))
    assert timeline[0] == {"from": "00:00:00", "packet": "location", "lat": -6.175, "lng": 106.857, "altitude": 10,
                           "satellites": 12, "speed": 25, "acc": True, "dallas_temperature_1": -193,
                           "ultrasonic_fuel": 4550}                  # different fields: alone
    assert timeline[1]["to"] == "00:00:40" and timeline[1]["lng"] == [106.858, 106.86]
    assert timeline[1]["dallas_temperature_1"] == [-190, -184]
    parked = timeline[-1]
    assert parked["from"] == "00:01:00" and parked["to"] == "00:16:00" and parked["every"] == "00:05:00"
    assert parked["step_name"] == END_STEP and sum(row.get("step_name") == END_STEP for row in timeline) == 1


def test_imported_timeline_sends_the_same_packets_as_the_file(scenario):
    rows = read_scenario(scenario)
    case = {"start": "today-1d", "interval": "00:00:10", "carry_over": False, "timeline": to_timeline(rows)}
    steps = expand_steps(case, {"altitude": 99, "engine_on": False}, NOW, {}, None, WIB)
    start = datetime(2026, 11, 18, tzinfo=WIB)
    assert len(steps) == len(rows)
    for step, (second, raw) in zip(steps, rows):
        assert step.at == start + timedelta(seconds=second) and step.packet == raw["packet"]
        expected = normalize({key: value for key, value in raw.items() if key != "sent_at"})
        assert step.fields.keys() == expected.keys()                # no defaults, no carry-over
        for key, value in expected.items():
            assert step.fields[key] == pytest.approx(value, abs=COORDINATE_TOLERANCE) \
                if isinstance(value, float) else step.fields[key] == value
    buffered = steps[6]
    assert buffered.send_at == start + timedelta(seconds=50) and buffered.at == start + timedelta(seconds=45)


def test_teltonika_io_name_as_field():
    creator = TeltonikaCreator()
    items = creator.collect_io({"digital_input_2": 1, "ultrasonic_fuel": -40, "engine_on": "on"})
    assert [(hex(io_id), value) for io_id, _, value, _ in items] == [
        ("0xef", "on"), ("0x2", 1), ("0x147", -40), ("0x15", 5)]


def test_today_is_local_midnight():
    assert parse_time("today", NOW, WIB) == datetime(2026, 11, 19, tzinfo=WIB)
    assert parse_time("today-2d", NOW, WIB) == datetime(2026, 11, 17, tzinfo=WIB)
    assert parse_time("today+1d", NOW, WIB) == datetime(2026, 11, 20, tzinfo=WIB)


def test_day_time_variable():
    start = datetime(2026, 11, 17, tzinfo=WIB)
    context = CheckContext(device={}, step_times={}, timeline_start=start)
    assert resolve("H1 00:59:50", context, "end") == start + timedelta(minutes=59, seconds=50)
    assert resolve({"between": ["H2 00:30:00", "H3 00:00:00"]}, context, "end")["between"] == [
        start + timedelta(days=1, minutes=30), start + timedelta(days=2)]
    with pytest.raises(KeyError):
        resolve("H1 00:00:00", CheckContext(device={}, step_times={}), "end")


def test_datetime_where_matches_within_tolerance():
    sql, params = build_where({"error_type": "TEMP-01", "start_time": NOW}, None)
    assert sql == " WHERE error_type = %s AND start_time BETWEEN %s AND %s"
    assert params == ["TEMP-01", NOW - DATETIME_TOLERANCE, NOW + DATETIME_TOLERANCE]


def test_setup_pin_overrides_and_new_sensor_codes():
    sensors = {"sensors": [{"code": "refrige_active", "pin": "digital_2"}, {"code": "pto_active", "pin": "ANALOG_2"},
                           {"code": "temperature_1"}]}
    assert setup.required_sensors(sensors) == {"speed": 6, "refrige_active": 15, "pto_active": 24, "temperature_1": 12}
    assert setup.setup_pins(sensors) == {"refrige_active": 2, "pto_active": 6}
    with pytest.raises(ValueError, match="pin"):
        setup.setup_pins({"sensors": [{"code": "fuel", "pin": "usb"}]})


class Sheet:
    title = "expected_v2"

    def __init__(self, rows):
        self.rows = rows

    def iter_rows(self, values_only=True):
        columns = ["step_name", "family", "table", "where", "column", "expected", "note"]
        yield columns
        for row in self.rows:
            yield [row.get(column) for column in columns]


def test_excel_timeout_column_and_between_with_spaces_round_trip():
    expected = {"default": {"steps": {"end": {"db": [
        {"table": "vehicle_status", "where": {"vehicle_id": "$vehicle_id"}, "match": {"last_packet": "$step_at"},
         "timeout": "00:10:00"},
        {"table": "summary_device_error",
         "where": {"vehicle_id": "$vehicle_id", "start_time": {"between": ["H1 00:20:00", "H5 00:00:00"]}},
         "count": 29},
    ]}}}}
    assert parse_where("start_time between H1 00:20:00 and H5 00:00:00") == {
        "start_time": {"between": ["H1 00:20:00", "H5 00:00:00"]}}
    assert read_expected(Sheet(expected_rows(expected)), "uc") == {"uc": expected}
