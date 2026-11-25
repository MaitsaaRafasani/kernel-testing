"""Preflight v2: device registered + sensors (+ creator pins) + reset (versions/v2/setup.py) and check_ready."""
import asyncio
import importlib

import pytest

from creators.concox.creator import ConcoxCreator
from creators.teltonika.creator import TeltonikaCreator
from harness.runner import Preflight, check_ready

setup = importlib.import_module("versions.v2.setup")

IMEI = "358000000000001"
CONCOX_PINS = ConcoxCreator.sensor_pins                  # door1 -> DIGITAL_1 (1)
READY_ROW = {"vehicle_id": 12, "company_id": 3, "has_vehicle": True, "has_status": True, "is_reset": True,
             "sensors": [[6, None], [4, 1]]}             # speed (no pin), door1 on pin 1
SPEED_DOOR1 = {"speed": 6, "door1": 4}


class FakeDb:
    def __init__(self, row):
        self.row = row
        self.params = None

    async def get_one(self, sql, params):
        self.params = params
        return self.row


def state_of(row, sensors=None, pins=None):
    return asyncio.run(setup.check_device(FakeDb(row), IMEI, sensors or {"speed": 6}, pins or {}))


def test_required_sensors_base_speed_plus_case_setup():
    assert setup.required_sensors(None) == {"speed": 6}
    assert setup.required_sensors({"sensors": [{"code": "door1"}]}) == SPEED_DOOR1
    with pytest.raises(ValueError):
        setup.required_sensors({"sensors": [{"code": "pintu"}]})


def test_creator_pins_are_kernel_pin_codes():
    assert CONCOX_PINS["door1"] == 1 and TeltonikaCreator.sensor_pins["door1"] == 2
    assert set(CONCOX_PINS) | set(TeltonikaCreator.sensor_pins) <= set(setup.SENSOR_CODES)


def test_ready_device_gives_variables_and_sends_sentinel():
    db = FakeDb(READY_ROW)
    state = asyncio.run(setup.check_device(db, IMEI, SPEED_DOOR1, CONCOX_PINS))
    assert state.problems == [] and state.is_reset
    assert state.variables == {"imei": IMEI, "vehicle_id": 12, "company_id": 3}
    assert db.params == {"imei": IMEI, "sentinel": setup.RESET_SENTINEL_TIME}


@pytest.mark.parametrize("row, expected", [
    (None, "tidak ada di tabel device"),
    ({**READY_ROW, "vehicle_id": None}, "vehicle_id kosong"),
    ({**READY_ROW, "has_vehicle": False}, "tidak ada di tabel vehicle"),
    ({**READY_ROW, "has_status": False, "is_reset": False}, "tidak ada di vehicle_status"),
    ({**READY_ROW, "sensors": [[4, 1]]}, "speed (6)"),
])
def test_not_registered(row, expected):
    state = state_of(row)
    assert len(state.problems) == 1 and expected in state.problems[0]


@pytest.mark.parametrize("rows, expected", [
    ([[6, None], [4, 1]], []),
    ([[6, None], [4, 2]], ["pin_code 2, creator kirim di pin 1"]),                 # teltonika pin on concox
    ([[6, None], [4, 1], [5, 1]], ["pin 1 sensor door1 juga dipakai sensor_code [5]"]),
    ([[6, None]], ["door1 (4)"]),
])
def test_sensor_pin_must_match_creator(rows, expected):
    problems = setup.sensor_problems(12, rows, SPEED_DOOR1, CONCOX_PINS)
    assert len(problems) == len(expected) and all(text in problem for text, problem in zip(expected, problems))


def test_sensor_not_sent_on_pin_is_not_pin_checked():
    assert setup.sensor_problems(12, [[6, 9], [4, 1]], SPEED_DOOR1, CONCOX_PINS) == []


@pytest.mark.parametrize("row, force, ready", [
    (READY_ROW, False, True),
    ({**READY_ROW, "is_reset": False}, False, False),      # not reset -> skip
    ({**READY_ROW, "is_reset": False}, True, True),        # force -> warn, run
    ({**READY_ROW, "sensors": []}, True, False),           # force never skips a missing sensor
])
def test_check_ready_reset_and_force(row, force, ready):
    preflight = Preflight(FakeDb(row), setup, force)
    result, device = asyncio.run(check_ready(preflight, IMEI, {"sensors": [{"code": "door1"}]}, CONCOX_PINS))
    assert result is ready and device["vehicle_id"] == 12
