"""Temperature simulator (generators/temperature_common.py) vs kernel v2 TemperatureSensor / Device / DeviceHealth
rules, the family decoders and the generated cases. No network."""
from datetime import datetime, timezone

import pytest

from creators.registry import get_creator
from generators import temperature_common as tc
from generators import temperature_queclink, temperature_ruptela, temperature_teltonika
from generators.health_common import m
from harness.config import load_yaml
from harness.loader import CASES_DIR, as_scenarios
from harness.timeline import expand_steps

FAMILIES = (temperature_teltonika, temperature_ruptela, temperature_queclink)
NOW = datetime(2026, 11, 23, 3, 0, 0, tzinfo=timezone.utc)


def record(sec: int, speed: int = 0, acc: bool = False, lng: float = 106.9) -> dict:
    return {"sec": sec, "lat": -6.2, "lng": lng, "speed": speed, "acc": acc}


def parked(kernel: tc.Kernel, readings_per_sec: list[tuple[int, dict]]) -> tc.Kernel:
    for sec, readings in readings_per_sec:
        kernel.message(record(sec), readings)
    return kernel


def wire1(value: float) -> dict:
    return {1: tc.Reading(value), 2: tc.Reading(4.0), 3: tc.Reading(29.0)}


def test_first_packet_starts_the_timers_and_the_next_alert_needs_more_than_300_s():
    kernel = parked(tc.Kernel(bug=False), [(0, wire1(-10)), (300, wire1(-11)), (310, wire1(-11))])
    alerts = [(alert.sec, alert.kind, alert.flag, alert.trigger) for alert in kernel.alerts]
    assert alerts == [(310, "idle_hi", False, -12)]


def test_zero_threshold_is_ignored_by_the_kernel():
    """Driving (motion M from the 2nd record): chiller below alert_lo 0."""
    sims = [tc.Kernel(bug) for bug in (False, True)]
    for kernel in sims:
        for step, (sec, chiller) in enumerate(((0, 4.0), (10, 4.1), (400, -0.5), (800, -0.6))):
            readings = {1: tc.Reading(-18.0), 2: tc.Reading(chiller), 3: tc.Reading(29.0)}
            kernel.message(record(sec, speed=40, acc=True, lng=106.9 + step * 0.001), readings)
    correct, bug = sims
    assert [(alert.sec, alert.kind, alert.trigger) for alert in correct.alerts] == [(400, "lo", 0), (800, "lo", 0)]
    assert bug.alerts == []


def test_one_day_gap_counts_as_zero_seconds_in_the_kernel():
    day = m(0, day=1)
    readings = [(0, wire1(-18)), (400, wire1(-11)), (day + 400, wire1(-11)), (day + 800, wire1(-11))]
    correct, bug = (parked(tc.Kernel(flag), readings) for flag in (False, True))
    assert [alert.sec for alert in correct.alerts] == [400, day + 400, day + 800]
    assert [alert.sec for alert in bug.alerts] == [400, day + 800]                # 1 day + 400 s -> 400 > 300


def test_wire_without_data_more_than_10_min_is_reset():
    lost = {1: tc.Reading(-18.0), 2: tc.Reading(4.0)}
    kernel = parked(tc.Kernel(False), [(0, wire1(-18.1)), (300, lost), (600, lost), (660, {1: tc.Reading(-18.2)})])
    assert kernel.vehicle_status[600] == [-18.0, 4.0, 29.0]                       # exactly 600 s: not yet
    assert kernel.vehicle_status[660] == [-18.2, 4.0, None]
    assert kernel.raw_rows[-1] == (660, (-18.2, 4.0, None))


def test_raw_sensor_info_insert_rules():
    kernel = parked(tc.Kernel(False), [(0, wire1(-18.0)), (30, wire1(-18.1)), (60, wire1(-18.2)), (90, wire1(-18.2)),
                                       (360, wire1(-18.2))])
    assert [created for created, _ in kernel.raw_rows] == [0, 60, 360]             # first, changed >= 60, >= 300


@pytest.mark.parametrize("status, label", [((True, "3000"), "TEMP-02"), (None, "TEMP-01")])
def test_error_value_is_temp02_missing_data_is_temp01(status, label):
    error = {1: tc.Reading(-18.0), 2: tc.Reading(4.0), 3: tc.Reading(None, status)}
    steps = [(0, wire1(-18.1))] + [(sec, error) for sec in range(60, 721, 60)] + [(780, wire1(-18.3))]
    kernel = parked(tc.Kernel(False), steps)
    assert kernel.health_status[720][2] == label
    wire3 = [(item.label, item.start, item.end) for item in kernel.summaries if item.description == "temperature-3"]
    assert wire3 == [(label, 0, 780)]


def test_teltonika_decode():
    decode = temperature_teltonika.decode
    assert decode(-183, False) == tc.Reading(-18.3, (False, "-18.3"))
    assert decode(3000, False) == tc.Reading(None, (True, "3000")) and decode(850, False).status == (True, "850")
    assert decode(1250, False).value == 125.0 and decode(1251, False) == tc.Reading(None, (False, "125.1"))
    assert decode(-551, False).value is None and decode(0, True) == tc.Reading(None, (True, "-127"))


def test_ruptela_decode_has_text_errors_and_no_range():
    decode = temperature_ruptela.decode
    assert decode(2000) == tc.Reading(None, (True, "No device"))
    assert decode(850).status == (True, "Power supply too low")
    assert decode(1251).value == 125.1 and decode(-551).value == -55.1


def test_queclink_ids_and_unknown_sensor():
    row = {"sec": tc.FAULTS[0], "celsius": [-18.0, 4.0, 85.0]}
    io, readings = temperature_queclink.read(row)
    assert list(io["one_wire"]) == [*temperature_queclink.IDS.values(), temperature_queclink.UNKNOWN_ID]
    assert readings == {1: tc.Reading(-18.0), 2: tc.Reading(4.0), 3: tc.Reading(85.0)}
    lost = temperature_queclink.read({"sec": tc.SENSOR_LOST[0], "celsius": [-18.0, 4.0, 29.0]})
    assert temperature_queclink.IDS[3] not in lost[0]["one_wire"] and 3 not in lost[1]


@pytest.mark.parametrize("family", FAMILIES)
def test_build_known_bugs_and_steps(family):
    case, expected = family.build()
    steps = expected[family.USECASE]["default"]["steps"]
    named = {row["step_name"] for row in case["timeline"] if "step_name" in row}
    assert named == set(steps) and case["families"] == [family.FAMILY]
    bugs = [(name, check["table"]) for name, step in steps.items() for check in step["db"] if "known_bug" in check]
    assert bugs == [("cek_h1_014500", "raw_alert"), ("cek_h2_060000", "raw_alert"), ("end", "raw_alert"),
                    ("end", "raw_alert")]
    assert steps["end"]["db"][0]["match"] == {"last_packet": "$step_at"}
    reset = steps["cek_h1_024000"]["db"]
    assert reset[0]["match"]["temperature"][2] is None and reset[1]["match"]["temperature3"] is None
    status_alerts = [check for step in steps.values() for check in step["db"] if check["where"].get("alert_code") == 76]
    assert bool(status_alerts) is family.HAS_STATUS


def test_status_alert_toggling_every_record_is_one_count_check():
    kernel = tc.Kernel(False)
    kernel.status_alerts = [(0, 1, False, "-18.0")] + [(100 + i * 10, 3, i % 2 == 0, "x") for i in range(6)]
    checks = tc.status_alert_checks(kernel)
    assert checks[0][0]["count"] == 1 and checks[0][0]["match"]["alert_flag"] is False
    assert list(checks) == [0, 150] and checks[150][0]["count"] == 6
    assert checks[150][0]["where"]["created_on"] == {"between": [tc.fmt(100), tc.fmt(150)]}


@pytest.mark.parametrize("family", FAMILIES)
def test_device_health_temp_cases_moved_from_the_kernel_scenario(family):
    """Every family has the TEMP cases of device_health_TEMP.txt that do not depend on the family."""
    _, correct, _ = tc.simulated(family)
    found = {(item.label, item.description, item.start, item.end) for item in correct.summaries}
    assert {item[1:5] for item in tc.COMMON_HEALTH} <= found
    assert ("TEMP-03", "temperature-1", tc.h2(14, 29, 50), tc.h2(15)) not in found     # Y: wire reset, not TEMP-03


@pytest.mark.parametrize("family", FAMILIES)
def test_generated_case_is_sendable_by_the_creator(family):
    """Every timeline row of cases/<usecase>.yaml builds a packet (fields supported, values fit)."""
    case = load_yaml(CASES_DIR / f"{family.USECASE}.yaml")
    creator = get_creator(family.FAMILY)
    steps = expand_steps(as_scenarios(case)[0], {}, NOW, family=family.FAMILY)
    assert len(steps) == len(tc.records())
    for step in steps:
        assert creator.build(step.packet, step.fields, step.at, family.IMEI)
