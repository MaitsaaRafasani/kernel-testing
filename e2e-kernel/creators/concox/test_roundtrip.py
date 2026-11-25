"""Round-trip: kernel v2 ConcoxParser(creator(fields)) == fields. Skipped without KERNEL_V2_PATH."""
import os
import sys
from datetime import datetime, timezone

import pytest

from creators.concox.creator import ConcoxCreator

KERNEL_V2_PATH = os.getenv("KERNEL_V2_PATH")
pytestmark = pytest.mark.skipif(not KERNEL_V2_PATH, reason="KERNEL_V2_PATH not set")

AT = datetime(2026, 11, 18, 3, 0, 0, tzinfo=timezone.utc)
IMEI = "351510093236753"
GPS = {"latitude": -6.2001, "longitude": 106.8167, "speed": 40, "direction": 90, "gps_satellites": 10}


def parse(packet: str) -> dict:
    if KERNEL_V2_PATH not in sys.path:
        sys.path.insert(0, KERNEL_V2_PATH)
    from family.concox.concox import ConcoxParser
    result, _ = ConcoxParser().device_parser(packet, 7, "GT06N")
    return result


@pytest.mark.parametrize("protocol", ["22", "12", "a0"])
def test_location_roundtrip(protocol):
    packet = ConcoxCreator().build("location", {**GPS, "protocol": protocol, "engine_on": True}, AT, IMEI)[0]
    result = parse(packet)
    assert result["latitude"] == pytest.approx(-6.2001, abs=1e-6)
    assert result["longitude"] == pytest.approx(106.8167, abs=1e-6)
    assert result["speed"] == 40
    assert result["direction"] == 90
    assert result["satellites"] == 10
    assert result["position_status"] is True
    assert result["acc"] is True


@pytest.mark.parametrize("protocol", ["26", "16", "a4"])
def test_alarm_roundtrip(protocol):
    fields = {**GPS, "protocol": protocol, "alarm": "sos", "engine_on": True, "battery": 5, "signal_strength": 3}
    result = parse(ConcoxCreator().build("alarm", fields, AT, IMEI)[0])
    assert result["code"].name == "SOS"
    assert result["acc"] is True
    assert result["battery"] == 5
    assert result["signal"] == 3


def test_heartbeat_roundtrip():
    result = parse(ConcoxCreator().build("heartbeat", {"battery": 4, "signal_strength": 3, "engine_on": True},
                                         AT, IMEI)[0])
    assert result == {**result, "acc": True, "battery_level": 4, "signal": 3}


def test_door_roundtrip():
    result = parse(ConcoxCreator().build("sensor", {"door1": "open"}, AT, IMEI)[0])
    assert result["digital_input_1"] == 1


def test_analog_input_1_roundtrip():
    result = parse(ConcoxCreator().build("sensor", {"analog_input_1": 12310}, AT, IMEI)[0])
    assert result["analog_input_1"] == pytest.approx(12.31)


def test_login_roundtrip():
    assert parse(ConcoxCreator().build("login", {}, AT, IMEI)[0])["imei"] == IMEI
