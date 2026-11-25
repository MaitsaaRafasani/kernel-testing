"""Standalone tests for TeltonikaCreator (no kernel needed)."""
from datetime import datetime, timezone

import pytest

from creators.base import UnsupportedFieldError
from creators.teltonika.creator import TeltonikaCreator, crc16_arc

AT = datetime(2026, 11, 18, 3, 0, 0, tzinfo=timezone.utc)
GPS = {"latitude": -6.2001, "longitude": 106.8167, "speed": 20, "gps_satellites": 10}


def test_crc16_arc_check_value():
    assert crc16_arc(b"123456789".hex()) == 0xBB3D


def test_codec8_matches_teltonika_doc_sample():
    # Codec 8 example from Teltonika data sending protocols wiki
    expected = (
        "000000000000003608010000016b40d8ea30010000000000000000000000000000000105021503010101425e0f01f10000601a"
        "014e0000000000000000010000c7cf"
    )
    at = datetime.fromtimestamp(0x16B40D8EA30 / 1000, tz=timezone.utc)
    fields = {
        "codec": "8", "priority": 1, "event_io": 1,
        "io": {
            "gsm_signal_strength": 3, "digital_input_1": 1, "external_voltage": 0x5E0F,
            "gsm_operator_data": 0x601A, "i_button": "0000000000000000",
        },
    }
    assert TeltonikaCreator().avl_packet([(fields, at)], "8") == expected


def test_location_length_and_crc_consistent():
    packet = TeltonikaCreator().build("location", {**GPS, "engine_on": True, "door1": "open"}, AT, "350000000000001")[0]
    data = packet[16:-8]
    assert int(packet[8:16], 16) * 2 == len(data)
    assert int(packet[-8:], 16) == crc16_arc(data)
    assert data[:2] == "8e"


def test_x_byte_raw_ascii_and_beacon():
    fields = {**GPS, "io": {
        "ble1_custom_1": "0a0b0c",
        "vin_number": {"ascii": "WVWZZZ1JZXW000001"},
        "eye_beacon_visible": ["7cd9f40a1b2c"],
    }}
    packet = TeltonikaCreator().build("location", fields, AT, "350000000000001")[0]
    assert "014b00030a0b0c" in packet
    assert "0100" + format(17, "04x") + b"WVWZZZ1JZXW000001".hex() in packet
    assert "2a4c" in packet and "7cd9f40a1b2c" in packet


def test_x_byte_rejected_on_codec8():
    with pytest.raises(UnsupportedFieldError):
        TeltonikaCreator().build("location", {**GPS, "codec": "8", "io": {"ble1_custom_1": "00"}}, AT, "1")


def test_unknown_field_rejected():
    with pytest.raises(UnsupportedFieldError):
        TeltonikaCreator().build("location", {**GPS, "pto": 1}, AT, "1")


def test_negative_value_twos_complement():
    packet = TeltonikaCreator().build("location", {**GPS, "io": {"x_acc": -1}}, AT, "1")[0]
    assert "0011ffff" in packet


def test_rfid_written_byte_reversed():
    packet = TeltonikaCreator().build("location", {**GPS, "rfid": "0102030405060708"}, AT, "1")[0]
    assert "004e0807060504030201" in packet


def test_manual_can_ids():
    from creators.teltonika.io_table import IO
    assert IO["manual_can_0"] == (0x0384, 8)
    assert IO["manual_can_29"] == (0x03A1, 8)
    assert IO["manual_can_ft_19"] == (0x27FB, 8)
    assert IO["manual_can_ft_20"] == (0x283A, 8)
    assert IO["manual_can_ft_69"] == (0x286B, 8)
    packet = TeltonikaCreator().build("location", {**GPS, "io": {"manual_can_1": 5}}, AT, "1")[0]
    assert "03850000000000000005" in packet


def test_command_reply_codec12_matches_kernel_sample():
    packets = TeltonikaCreator().build("command_reply", {"reply_text": "New value 11703:0;"}, AT, "1")
    assert packets == ["000000000000001a0c0106000000124e65772076616c75652031313730333a303b010000d653"]


def test_login_packet():
    assert TeltonikaCreator().build("login", {}, AT, "356307042441013")[0] == "000f" + b"356307042441013".hex()
