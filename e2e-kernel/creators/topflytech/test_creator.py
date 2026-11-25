"""Standalone tests for TopflytechCreator (offsets = kernel v2 TopflytechParser slices)."""
import struct
from datetime import datetime, timezone

import pytest

from creators.base import UnsupportedFieldError
from creators.topflytech.creator import TopflytechCreator

AT = datetime(2024, 5, 22, 10, 6, 10, tzinfo=timezone.utc)
IMEI = "867284062611670"
GPS = {"latitude": -6.2001, "longitude": 106.8167, "altitude": 10, "speed": 120, "direction": 270,
       "gps_satellites": 10, "engine_on": True}


def test_login_matches_kernel_sample():
    assert TopflytechCreator().build("login", {}, AT, IMEI) == ["2727010017000108672840626116700106240522100610"]


def test_location_kernel_offsets():
    packet = TopflytechCreator().build("location", {**GPS, "battery": 85, "battery_voltage": 4.1}, AT, IMEI)[0]
    assert packet[:6] == "272702" and int(packet[6:10], 16) * 2 == len(packet) == 146
    assert packet[30:32] == format(0x40 | 10, "02x")                  # gnss data, 10 satellites
    assert packet[34:46] == "240522100610"                             # UTC as BCD digits
    assert struct.unpack("<f", bytes.fromhex(packet[54:62]))[0] == pytest.approx(106.8167, abs=1e-5)
    assert struct.unpack("<f", bytes.fromhex(packet[62:70]))[0] == pytest.approx(-6.2001, abs=1e-5)
    assert packet[70:74] == "1200" and int(packet[74:78], 16) == 270    # speed BCD x10, direction
    assert packet[88:90] == "85" and packet[94:96] == "41"             # battery %, battery voltage
    status = int(packet[106:110], 16)
    assert status >> 2 & 1 == 1 and status >> 4 & 0x7F == 100         # acc on, signal (level 4 + 1) x 20


def test_buffered_batch_sets_status_bit7_and_serial_increments():
    creator = TopflytechCreator()
    message = creator.build_batch([("location", GPS, AT), ("location", GPS, AT)], IMEI)
    first, second = message[:146], message[146:]
    assert int(first[30:32], 16) & 0x80 and int(second[30:32], 16) & 0x80
    assert first[10:14] == "0001" and second[10:14] == "0002"


def test_alarm_ble_and_command_reply():
    creator = TopflytechCreator()
    assert creator.build("alarm", {**GPS, "alarm": "sos"}, AT, IMEI)[0][32:34] == "03"
    ble = creator.build("sensor", {**GPS, "ble": {"sensor": "door", "mac_address": "f1:7a:12:57:84:60",
                                                  "door": "open", "temperature": 31.5, "battery": 100}}, AT, IMEI)[0]
    assert ble[4:6] == "12" and ble[78:82] == "0005" and ble[82:106] == "f17a12578460ff640c4e0150"
    reply = creator.build("command_reply", {"reply_text": "OK"}, AT, IMEI)[0]
    assert reply[4:6] == "81" and bytes.fromhex(reply[32:]).decode("utf-16-le") == "OK"


def test_rejects_unknown_field_and_bad_battery():
    with pytest.raises(UnsupportedFieldError):
        TopflytechCreator().build("location", {**GPS, "door1": 1}, AT, IMEI)
    with pytest.raises(ValueError):
        TopflytechCreator().build("location", {**GPS, "battery": 0}, AT, IMEI)
