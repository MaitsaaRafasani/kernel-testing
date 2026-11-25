"""Standalone tests for QueclinkCreator (kernel v2 fixture + QueclinkParser layout)."""
from datetime import datetime, timezone

import pytest

from creators.base import UnsupportedFieldError
from creators.queclink.creator import QueclinkCreator, crc16_ccitt, unique_id

AT = datetime(2026, 11, 20, 3, 4, 5, tzinfo=timezone.utc)
IMEI = "353000000000001"
GPS = {"latitude": -6.2001, "longitude": 106.8167, "altitude": 10, "speed": 60, "direction": 90,
       "gps_satellites": 10, "engine_on": True}
# kernel fixture family/queclink/fixtures/queclink_hex_gv58cg.json, HBD (crc_valid, unique_id 867488060468518)
HBD_SAMPLE = "2b484244ef2280200309020914564a58060444330807e80b0d0f2a1e3af25e720d0a"


def test_crc_and_imei_packing_match_kernel_fixture():
    assert crc16_ccitt(HBD_SAMPLE[8:-8]) == int(HBD_SAMPLE[-8:-4], 16)
    assert unique_id("867488060468518") == HBD_SAMPLE[26:42]     # after header, mask, length, type, versions


def test_location_frame_layout():
    frame = QueclinkCreator().build("location", GPS, AT, IMEI)[0]
    assert frame.startswith(b"+RSP".hex() + "07") and frame.endswith("0d0a")
    assert int(frame[18:22], 16) * 2 == len(frame)                    # length = whole frame
    assert crc16_ccitt(frame[8:-8]) == int(frame[-8:-4], 16)
    assert frame[22:38] == unique_id(IMEI)
    assert frame[38:40] == "22" and frame[40:42] == "0a"              # ignition on + moving, 10 satellites
    assert frame.count(format(2026, "04x") + "0b14030405") == 2       # GNSS time + send time


def test_eri_frame_with_one_wire_temperatures():
    """Layout of kernel QueclinkParser._parse_eri (GV58CG body): mask, ERI mask, length, unique id, status,
    report id, reserved, 1-wire block (id + type 01 + length 02 + signed 1/16 C), position."""
    sensors = {"28FF4A1B6416034E": -289, "28ff4a1b6416034f": 64}             # -18.0625 C, 4.0 C
    frame = QueclinkCreator().build("location", {**GPS, "one_wire": sensors}, AT, IMEI)[0]
    assert frame.startswith(b"+RSP".hex() + "12") and frame.endswith("0d0a")
    assert frame[18:26] == "00000002"                                  # ERI mask: 1-wire data
    assert int(frame[26:30], 16) * 2 == len(frame) and crc16_ccitt(frame[8:-8]) == int(frame[-8:-4], 16)
    assert frame[30:46] == unique_id(IMEI)
    assert frame[46:50] == "220a" and frame[50:54] == "0000"           # motion + satellites, report id + reserved
    block = frame[54:104]                                              # number + 2 x 12 bytes
    assert block == ("02" + "28ff4a1b6416034e" + "0102" + "fedf" + "28ff4a1b6416034f" + "0102" + "0040")
    assert frame[104:106] == "01"                                      # position tail starts like FRI


def test_one_wire_only_in_location():
    with pytest.raises(UnsupportedFieldError):
        QueclinkCreator().build("alarm", {**GPS, "alarm": "sos", "one_wire": {"28FF4A1B6416034E": 1}}, AT, IMEI)


def test_motion_status_and_buffered_header():
    towed = QueclinkCreator().build("location", {**GPS, "engine_on": False, "buffered": True}, AT, IMEI)[0]
    assert towed[38:40] == "12" and towed.startswith(b"+BSP".hex())


def test_login_sends_nothing_and_batch_is_one_frame_per_message():
    creator = QueclinkCreator()
    assert creator.build("login", {}, AT, IMEI) == []
    messages = creator.build_batch_messages([("location", GPS, AT), ("location", GPS, AT)], IMEI)
    assert len(messages) == 2 and all(message.startswith(b"+BSP".hex()) for message in messages)
    with pytest.raises(UnsupportedFieldError):
        creator.build_batch([("location", GPS, AT)], IMEI)


def test_heartbeat_and_command_reply():
    creator = QueclinkCreator()
    frame = creator.build("heartbeat", {}, AT, IMEI)[0]
    assert int(frame[10:12], 16) * 2 == len(frame) and crc16_ccitt(frame[8:-8]) == int(frame[-8:-4], 16)
    reply = creator.build("command_reply", {"reply_text": "+RESP:GTOUT,0"}, AT, IMEI)[0]
    assert bytes.fromhex(reply).decode() == "+RESP:GTOUT,0,20261120030405,0002$"
    with pytest.raises(ValueError):
        creator.build("command_reply", {"reply_text": "OK"}, AT, IMEI)
