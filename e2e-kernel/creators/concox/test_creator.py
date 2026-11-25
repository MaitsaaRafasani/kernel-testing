"""Standalone tests for ConcoxCreator. Expected packets are real samples from kernel v2 family/concox/test_concox.py."""
from datetime import datetime, timezone

import pytest

from creators.base import UnsupportedFieldError
from creators.concox.creator import ConcoxCreator, crc_itu

IMEI = "351510093236753"
LBS_1CC = {"mcc": 0x01CC, "mnc": 0x00, "lac": 0x287D, "cell_id": 0x1F71}

SAMPLES = [
    "78780a134004040001000fdcee0d0a",
    "78780b2300019f040002002bbd310d0a",
    "78781101035151009323675320082bc10001d1700d0a",
    "7878121901cc00287d001f71200404010100946c890d0a",
    "787819a501fe0a0000184a00000000038da0150a050403000093980e0d0a",
    "78781f12170c040b023acf00d0e1550c03c5b034d11101fe0a3fc0002c642ba4c5b10d0a",
    "787822220f0c1d023305c9027ac8180c46586000140001cc00287d001f71000001000820860d0a",
    "7878252612030c063816c3026c10540c38c97001440309"
    "01cc002866000eee0c06040302000da2db0d0a",
    "78782da4170a1f0a010eca00c7a5960c18c7400310f11001fe0a0000184a00000000038b58204205041302ff0063164f0d0a",
    "797900079405000001fc7a0d0a",
    "7979000e941e00000133000001c8026bde070d0a",
    "79790014f166fbb733100408b4a8283a221f000002520067" "0d0a",
]


def utc(*parts) -> datetime:
    return datetime(*parts, tzinfo=timezone.utc)


def coordinate(raw_hex: str) -> float:
    return int(raw_hex, 16) / 1_800_000


@pytest.mark.parametrize("sample", SAMPLES)
def test_crc_matches_kernel_samples(sample):
    assert format(crc_itu(sample[4:-8]), "04x") == sample[-8:-4]


def test_location_0x22():
    fields = {
        "protocol": "22", "serial": 0x08, "lbs": LBS_1CC, "reupload": 1,
        "latitude": coordinate("027ac818"), "longitude": coordinate("0c465860"),
        "gps_satellites": 9, "speed": 0, "direction": 0, "engine_on": False,
    }
    packets = ConcoxCreator().build("location", fields, utc(2015, 12, 29, 2, 51, 5), IMEI)
    assert packets == ["787822220f0c1d023305c9027ac8180c46586000140001cc00287d001f71000001000820860d0a"]


def test_location_0x12_local_time_and_acc_bits():
    fields = {
        "protocol": "12", "serial": 0x2BA4,
        "lbs": {"mcc": 0x01FE, "mnc": 0x0A, "lac": 0x3FC0, "cell_id": 0x2C64},
        "latitude": -coordinate("00d0e155"), "longitude": coordinate("0c03c5b0"),
        "gps_satellites": 15, "speed": 52, "direction": 273, "engine_on": True,
    }
    # device time 2023-12-04 11:02:58 (+7)
    packets = ConcoxCreator(device_tz=7).build("location", fields, utc(2023, 12, 4, 4, 2, 58), IMEI)
    assert packets == ["78781f12170c040b023acf00d0e1550c03c5b034d11101fe0a3fc0002c642ba4c5b10d0a"]


def test_alarm_0x26():
    fields = {
        "protocol": "26", "serial": 0x0D, "alarm": "vibration", "language": "02", "terminal_info": 0x0C,
        "lbs": {"mcc": 0x01CC, "mnc": 0x00, "lac": 0x2866, "cell_id": 0x0EEE},
        "latitude": coordinate("026c1054"), "longitude": coordinate("0c38c970"),
        "gps_satellites": 3, "speed": 1, "direction": 3, "position_status": False,
        "acc_in_course": True, "engine_on": False, "battery": 6, "signal_strength": 4,
    }
    packets = ConcoxCreator().build("alarm", fields, utc(2018, 3, 12, 6, 56, 22), IMEI)
    assert packets == [SAMPLES[7]]


def test_alarm_0xa4_4g():
    fields = {
        "protocol": "a4", "serial": 0x63, "alarm": "tamper", "language": "02", "engine_on": True,
        "lbs": {"mcc": 0x01FE, "mnc": 0x0A, "lac": 0x184A, "cell_id": 0x038B5820},
        "latitude": -coordinate("00c7a596"), "longitude": coordinate("0c18c740"),
        "gps_satellites": 10, "speed": 3, "direction": 241, "battery": 5, "signal_strength": 4,
    }
    packets = ConcoxCreator().build("alarm", fields, utc(2023, 10, 31, 10, 1, 14), IMEI)
    assert packets == [SAMPLES[8]]


def test_alarm_lbs_0x19_and_0xa5():
    creator = ConcoxCreator()
    alarm_19 = {"protocol": "19", "serial": 0x94, "alarm": "sos", "language": "01", "terminal_info": 0x20,
                "lbs": LBS_1CC, "battery": 4, "signal_strength": 4}
    assert creator.build("alarm", alarm_19, utc(2026, 1, 1), IMEI) == [SAMPLES[3]]

    alarm_a5 = {"protocol": "a5", "serial": 0x93, "alarm": "vibration", "terminal_info": 0x0A,
                "lbs": {"mcc": 0x01FE, "mnc": 0x0A, "lac": 0x184A, "cell_id": 0x038DA015},
                "battery": 5, "signal_strength": 4}
    assert creator.build("alarm", alarm_a5, utc(2026, 1, 1), IMEI) == [SAMPLES[4]]


def test_heartbeat_0x13_and_0x23():
    creator = ConcoxCreator()
    hb_13 = {"serial": 0x0F, "battery": 4, "signal_strength": 4, "language": "0001", "engine_on": False}
    assert creator.build("heartbeat", hb_13, utc(2026, 1, 1), IMEI) == [SAMPLES[0]]

    hb_23 = {"protocol": "23", "serial": 0x2B, "battery_voltage": 0x019F, "signal_strength": 4,
             "position_status": False, "engine_on": False}
    assert creator.build("heartbeat", hb_23, utc(2026, 1, 1), IMEI) == [SAMPLES[1]]


def test_door_information_0x94():
    packets = ConcoxCreator().build("sensor", {"serial": 1, "door1": "open"}, utc(2026, 1, 1), IMEI)
    assert packets == [SAMPLES[9]]


def test_analog_input_1_is_external_voltage_0x94_in_centivolt():
    creator = ConcoxCreator()
    packet = creator.build("sensor", {"analog_input_1": 12310}, utc(2026, 1, 1), IMEI)[0]
    assert packet[8:16] == "940004cf"         # type 00, 1231 = 12.31 V (kernel: int / 100 on ANALOG_1)
    assert format(crc_itu(packet[4:-8]), "04x") == packet[-8:-4]
    again = creator.build("sensor", {"analog_input_1": 12310}, utc(2026, 1, 1, 0, 0, 10), IMEI)
    assert len(again) == 1                      # sensor row = periodic sample: sent even when unchanged
    gps = {"latitude": -6.2, "longitude": 106.8, "analog_input_1": 12310}
    assert len(creator.build("location", gps, utc(2026, 1, 1, 0, 0, 20), IMEI)) == 1     # location: only on change


def test_temperature_information_0x94():
    fields = {"serial": 0x026B, "temperature_1": 307, "humidity_1": 0x01C8}
    assert ConcoxCreator().build("sensor", fields, utc(2026, 1, 1), IMEI) == [SAMPLES[10]]


def test_ble_door_0xf1():
    fields = {"serial": 0x0252, "ble": {"mac_address": "b4:a8:28:3a:22:1f", "door": 0}}
    at = datetime.fromtimestamp(0x66FBB733, tz=timezone.utc)
    assert ConcoxCreator().build("sensor", fields, at, IMEI) == [SAMPLES[11]]


def test_command_reply_0x21_matches_kernel_sample():
    fields = {"protocol": "21", "serial": 0x9B, "reply_text": "ICCID:8962100030155483703F"}
    packets = ConcoxCreator().build("command_reply", fields, utc(2026, 1, 1), IMEI)
    assert packets == ["7979002421000000000149434349443a3839363231303030333031353534383337303346009b18d70d0a"]


def test_command_reply_0x15_kernel_offsets():
    text = "Battery:4.18V,NORMAL; GPRS:Link Up; GSM Signal Level:Strong"
    packet = ConcoxCreator().build("command_reply", {"reply_text": text}, utc(2026, 1, 1), IMEI)[0]
    # kernel ConcoxParser REPLY: content = message[18:18 + (int(message[8:10], 16) - 4) * 2]
    length = (int(packet[8:10], 16) - 4) * 2
    assert bytes.fromhex(packet[18:18 + length]).decode() == text
    assert packet[6:8] == "15"
    assert format(crc_itu(packet[4:-8]), "04x") == packet[-8:-4]


def test_login_imei_position():
    packet = ConcoxCreator().build("login", {}, utc(2026, 1, 1), IMEI)[0]
    assert packet[6:8] == "01"
    assert packet[9:24] == IMEI                      # kernel reads message[9:24]
    assert format(crc_itu(packet[4:-8]), "04x") == packet[-8:-4]


def test_location_sends_sensor_only_on_change():
    creator = ConcoxCreator()
    gps = {"latitude": -6.2, "longitude": 106.8, "gps_satellites": 10}
    first = creator.build("location", {**gps, "door1": "closed"}, utc(2026, 1, 1, 0, 0, 0), IMEI)
    same = creator.build("location", {**gps, "door1": "closed"}, utc(2026, 1, 1, 0, 0, 30), IMEI)
    changed = creator.build("location", {**gps, "door1": "open"}, utc(2026, 1, 1, 0, 1, 0), IMEI)
    assert [len(first), len(same), len(changed)] == [2, 1, 2]
    assert changed[1][8:12] == "9405"


def test_unknown_field_and_bad_protocol_rejected():
    with pytest.raises(UnsupportedFieldError):
        ConcoxCreator().build("location", {"altitude": 10}, utc(2026, 1, 1), IMEI)
    with pytest.raises(UnsupportedFieldError):
        ConcoxCreator().build("location", {"protocol": "26"}, utc(2026, 1, 1), IMEI)
