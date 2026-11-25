"""Round-trip: kernel v2 TeltonikaParser(creator(fields)) == fields. Skipped without KERNEL_V2_PATH."""
import os
import sys
from datetime import datetime, timezone

import pytest

from creators.teltonika.creator import TeltonikaCreator

KERNEL_V2_PATH = os.getenv("KERNEL_V2_PATH")
pytestmark = pytest.mark.skipif(not KERNEL_V2_PATH, reason="KERNEL_V2_PATH not set")

AT = datetime(2026, 11, 18, 3, 0, 0, tzinfo=timezone.utc)
IMEI = "350000000000001"


def parse(packet: str) -> dict:
    if KERNEL_V2_PATH not in sys.path:
        sys.path.insert(0, KERNEL_V2_PATH)
    from family.teltonika.teltonika import TeltonikaParser
    result, _ = TeltonikaParser().device_parser(packet, 0, "FMB130")
    assert len(result) == 1
    return result[0]


def test_location_roundtrip():
    fields = {
        "latitude": -6.2001, "longitude": 106.8167, "altitude": 15, "direction": 90,
        "speed": 40, "gps_satellites": 12, "engine_on": True, "door1": "open",
    }
    result = parse(TeltonikaCreator().build("location", fields, AT, IMEI)[0])
    assert result["latitude"] == pytest.approx(-6.2001)
    assert result["longitude"] == pytest.approx(106.8167)
    assert result["altitude"] == 15
    assert result["direction"] == 90
    assert result["speed"] == 40
    assert result["satellites"] == 12
    assert result["acc"] == "1"
    assert result["digital_input_2"] == 1


def test_x_byte_roundtrip():
    fields = {
        "latitude": -6.2, "longitude": 106.8, "gps_satellites": 10,
        "io": {"ble1_custom_1": "0a0b0c", "vin_number": {"ascii": "WVWZZZ1JZXW000001"}},
    }
    result = parse(TeltonikaCreator().build("location", fields, AT, IMEI)[0])
    assert result["ble1_custom_1"] == "0a0b0c"
    assert result["vin_number"] == b"WVWZZZ1JZXW000001".hex()


def test_eye_beacon_roundtrip():
    fields = {"latitude": -6.2, "longitude": 106.8, "gps_satellites": 10,
              "io": {"eye_beacon_visible": ["7cd9f40a1b2c"]}}
    result = parse(TeltonikaCreator().build("location", fields, AT, IMEI)[0])
    beacon = result["eye_beacon_visible"]
    assert [content.mac_addr for content in beacon.content] == ["7cd9f40a1b2c"]
