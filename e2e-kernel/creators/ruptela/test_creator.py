"""Standalone tests for RuptelaCreator (kernel v2 samples and RuptelaParser offsets)."""
from datetime import datetime, timezone

import pytest

from creators.base import UnsupportedFieldError
from creators.ruptela.creator import RuptelaCreator, crc16_kermit

AT = datetime(2026, 7, 28, 9, 32, 59, tzinfo=timezone.utc)
IMEI = "861327089808625"
GPS = {"latitude": -6.18967, "longitude": 106.838263, "altitude": 32.1, "direction": 185.1, "speed": 40,
       "gps_satellites": 17, "hdop": 0.9}


def test_command_reply_matches_kernel_sample():
    packet = RuptelaCreator().build("command_reply", {"reply_text": "Resetting device"}, AT, IMEI)[0]
    assert packet == "001900030f5f573934f107526573657474696e6720646576696365164b"


def test_crc_matches_kernel_extended_record_sample():
    sample = (
        "006800030F5F573934F14400016A718DFA00000080000000800000008000FFFFFFFFFFFF00070C000501019901001B1"
        "D000200000300001C0100AD0002FA0702FB0101A20103DC0001950104001D39D3001E00000016000002300000020041"
        "000000C800960000C74200F3D0"
    ).lower()
    assert int(sample[:4], 16) * 2 == len(sample) - 8
    assert crc16_kermit(sample[4:-4]) == int(sample[-4:], 16)


def test_extended_record_kernel_offsets():
    packet = RuptelaCreator().build("location", {**GPS, "engine_on": True, "door1": "open"}, AT, IMEI)[0]
    raw = bytes.fromhex(packet)
    assert raw[10] == 68 and raw[11:13] == b"\x00\x01"                 # extended records, 1 record
    record = raw[13:]
    assert int.from_bytes(record[0:4], "big") == int(AT.timestamp())
    assert int.from_bytes(record[7:11], "big", signed=True) == 1068382630
    assert int.from_bytes(record[11:15], "big", signed=True) == -61896700
    assert int.from_bytes(record[15:17], "big") == 321 and int.from_bytes(record[17:19], "big") == 18510
    assert record[19] == 17 and int.from_bytes(record[20:22], "big") == 40 and record[22] == 9
    io = record[25:]
    assert io[0] == 3                                                  # 1-byte io: acc, signal, digital_input_2
    assert io[1:10] == bytes.fromhex("019901" "001b19" "000301")


def test_batch_one_packet_many_records():
    items = [("location", GPS, AT), ("location", GPS, AT)]
    assert bytes.fromhex(RuptelaCreator().build_batch(items, IMEI))[12] == 2


def test_rejects_unknown_io():
    with pytest.raises(UnsupportedFieldError):
        RuptelaCreator().build("location", {**GPS, "io": {"nope": 1}}, AT, IMEI)
