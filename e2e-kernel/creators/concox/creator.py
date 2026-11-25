"""Concox creator: fields -> raw hex packets, readable by kernel v2 parser (family/concox/concox.py).

send -> packets:
- login:     0x01
- location:  0x22 (default, UTC) | 0x12 (local time, acc in course bits) | 0xa0 (4G)
             + sensor packets for sensor fields whose value changed (like a real device)
- sensor:    sensor packets only, every field of the step (ex: periodic analog sample)
- heartbeat: 0x13 (default) | 0x23 | 0x36
- alarm:     0x26 (default, UTC) | 0x16 (local time) | 0x27 | 0xa4 (4G) | 0x19 (2G LBS) | 0xa5 (4G LBS)
- command_reply: 0x15 (default) | 0x21, field `reply_text` = reply text

Sensor packets:
- door1          -> 0x94 type 05 (IO detection, kernel pin DIGITAL_1)
- vcc_voltage    -> 0x94 type 00 (raw, 0.01 V)
- analog_input_1 -> 0x94 type 00 (mV, sent as 0.01 V, kernel pin ANALOG_1, ex: analog door sensor)
- fuel          -> 0x94 type 0d (ASCII, kernel pin RS232)
- temperature_1  -> 0x94 type 1e (raw, 0.1 C, with humidity_1)
- ble option     -> 0xf1 (BLE door / temperature / battery)

Values are device-level (raw), no scaling.
"""
from datetime import datetime, timedelta, timezone

from creators.base import FamilyCreator, IdentifierFormat, UnsupportedFieldError

START_SHORT = "7878"      # 1-byte length
START_LONG = "7979"       # 2-byte length
STOP = "0d0a"

LOCATION_PROTOCOLS = ("22", "12", "a0")
HEARTBEAT_PROTOCOLS = ("13", "23", "36")
REPLY_PROTOCOLS = ("15", "21")
ALARM_PROTOCOLS = ("26", "16", "27", "a4", "19", "a5")
LOCAL_TIME_PROTOCOLS = ("12", "16")       # kernel parser: not UTC
FOUR_G_PROTOCOLS = ("a0", "a4", "a5")     # LAC 4 bytes, cell id 8 bytes

# kernel PinCodes (enums/sensor_enum.py)
PIN_DIGITAL_1 = 1
PIN_RS232 = 9
PIN_INTERNAL_TEMPERATURE = 24

# alarm name (kernel AlertCodes, lowercase) -> concox alarm code (enums/concox_enum.py ConcoxAlarmCode)
ALARM_CODES = {
    "sos": 0x01, "disarm": 0x02, "vibration": 0x03, "overspeeds": 0x06, "lowbattery": 0x0E,
    "harsh_accels": 0x29, "harsh_brakes": 0x30, "sharp_turns": 0x2A, "cover_removed": 0x18,
    "tamper": 0x13, "power_on": 0x0C, "power_off": 0x11, "power_removal": 0x88, "crash": 0x93,
    "driving_duration_too_long": 0x47, "eye_closed": 0x8C, "distraction": 0x8F, "yawn": 0xA0,
    "smoking": 0x9A, "phone_calling": 0x97, "missing_face": 0x94, "file_uploaded": 0x69,
    "sd_card_removed": 0x86, "sd_card_instaled": 0x70, "sd_card_not_enough": 0xA4, "sd_card_fault": 0x6F,
    "forward_collision_warning": 0xCC, "lane_departure_warning": 0xCD, "headway_monitoring_warning": 0xCE,
}

DEFAULT_LBS = {"mcc": 0x01FE, "mnc": 0x0A, "lac": 0, "cell_id": 0}   # Indonesia, Telkomsel
GPS_INFO_LENGTH = 0xC          # high nibble of satellites byte

GPS_FIELDS = ("latitude", "longitude", "speed", "direction", "gps_satellites", "position_status", "engine_on")
SENSOR_FIELDS = ("door1", "vcc_voltage", "analog_input_1", "fuel", "temperature_1", "humidity_1")
HEARTBEAT_FIELDS = ("battery", "battery_voltage", "signal_strength")
OPTION_FIELDS = (
    "alarm", "protocol", "serial", "language", "lbs", "terminal_info", "upload_mode", "reupload",
    "device_tz", "fence_id", "modules", "ble", "acc_in_course", "reply_text", "server_flag",
)
WORD_VALUES = {"on": 1, "open": 1, "true": 1, "off": 0, "closed": 0, "false": 0}


def crc_itu(data_hex: str) -> int:
    """CRC-ITU (CRC-16/X-25), same as kernel helper compute."""
    crc = 0xFFFF
    for byte in bytes.fromhex(data_hex):
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0x8408 if crc & 1 else crc >> 1
    return crc ^ 0xFFFF


def to_int(value) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, str) and value.strip().lower() in WORD_VALUES:
        return WORD_VALUES[value.strip().lower()]
    return int(value)


def to_hex(value, size: int, name: str) -> str:
    """int (negative = two's complement) -> hex of exactly `size` bytes."""
    number = to_int(value)
    bits = size * 8
    if not -(1 << (bits - 1)) <= number < (1 << bits):
        raise ValueError(f"{name}: value {number} does not fit in {size} bytes")
    return format(number & ((1 << bits) - 1), f"0{size * 2}x")


class ConcoxCreator(FamilyCreator):
    family = "concox"
    default_model = "gt06n"
    identifier_format = IdentifierFormat(length=15)
    initial_packets = ("login",)        # real device logs in first
    sensor_pins = {"door1": PIN_DIGITAL_1, "fuel": PIN_RS232, "temperature_1": PIN_INTERNAL_TEMPERATURE}
    supported_fields = set(GPS_FIELDS) | set(SENSOR_FIELDS) | set(HEARTBEAT_FIELDS) | set(OPTION_FIELDS)

    def __init__(self, device_tz: int = 7):
        self.device_tz = device_tz          # used by local-time protocols (0x12, 0x16)
        self.serial = 0
        self.last_sensor: dict = {}         # last sent sensor values, to send only on change

    # ---- entry point ----

    def build(self, packet: str, fields: dict, at: datetime, imei: str) -> list[str]:
        unknown = set(fields) - self.supported_fields
        if unknown:
            raise UnsupportedFieldError(f"concox: field not supported: {sorted(unknown)}")
        if "serial" in fields:
            self.serial = int(fields["serial"]) - 1

        if packet == "login":
            return [self.login(imei)]
        if packet == "location":
            return [self.location(fields, at)] + self.sensor_packets(fields, at)
        if packet == "sensor":
            return self.sensor_packets(fields, at, every=True)
        if packet == "heartbeat":
            return [self.heartbeat(fields)]
        if packet == "alarm":
            return [self.alarm(fields, at)]
        if packet == "command_reply":
            return [self.command_reply(fields)]
        raise UnsupportedFieldError(f"concox: send '{packet}' not supported")

    # ---- packets ----

    def build_batch(self, items: list[tuple[str, dict, datetime]], imei: str) -> str:
        """Buffered data: frames concatenated in one gate message (kernel _split_data splits them).
        Location 0x22 / 0xa0 get re-upload flag 1 (data from device memory) unless set in the step."""
        marked = [
            (send, {"reupload": 1, **fields} if send == "location" else fields, at)
            for send, fields, at in items
        ]
        return super().build_batch(marked, imei)

    def login(self, imei: str) -> str:
        if len(imei) != 15 or not imei.isdigit():
            raise ValueError(f"concox: imei must be 15 digits, got '{imei}'")
        return self.frame("01", "0" + imei)

    def location(self, fields: dict, at: datetime) -> str:
        protocol = self.protocol(fields, LOCATION_PROTOCOLS)
        content = self.gps(fields, at, protocol) + self.lbs(fields, protocol, with_length=False)
        if protocol != "12":
            content += (
                to_hex(fields.get("engine_on", 0), 1, "engine_on")
                + to_hex(fields.get("upload_mode", 0), 1, "upload_mode")
                + to_hex(fields.get("reupload", 0), 1, "reupload")
            )
        return self.frame(protocol, content)

    def heartbeat(self, fields: dict) -> str:
        protocol = self.protocol(fields, HEARTBEAT_PROTOCOLS)
        signal = to_hex(fields.get("signal_strength", 4), 1, "signal_strength")
        language = fields.get("language", "0002")
        if protocol == "23":
            voltage = to_hex(fields.get("battery_voltage", 400), 2, "battery_voltage")
            return self.frame(protocol, self.terminal(fields) + voltage + signal + language)
        content = self.terminal(fields) + to_hex(fields.get("battery", 6), 1, "battery") + signal + language
        if protocol == "36":
            modules = dict(fields.get("modules") or {})
            if "battery_voltage" in fields:
                modules.setdefault("0018", to_hex(fields["battery_voltage"], 2, "battery_voltage"))
            content += "".join(
                module_id + format(len(value) // 2, "02x") + value for module_id, value in modules.items()
            )
        return self.frame(protocol, content)

    def alarm(self, fields: dict, at: datetime) -> str:
        protocol = self.protocol(fields, ALARM_PROTOCOLS)
        name = fields.get("alarm")
        if name is None:
            raise ValueError("concox: send alarm needs field 'alarm'")
        code = int(name, 16) if isinstance(name, str) and name.lower().startswith("0x") else ALARM_CODES.get(name)
        if code is None:
            raise UnsupportedFieldError(f"concox: alarm '{name}' not supported, use hex code like '0x13'")

        status = (
            self.terminal(fields)
            + to_hex(fields.get("battery", 6), 1, "battery")
            + to_hex(fields.get("signal_strength", 4), 1, "signal_strength")
            + to_hex(code, 1, "alarm") + fields.get("language", "00")
        )
        if protocol in ("19", "a5"):        # LBS only, no GPS
            return self.frame(protocol, self.lbs(fields, protocol, with_length=False) + status)
        content = self.gps(fields, at, protocol) + self.lbs(fields, protocol, with_length=True) + status
        if protocol == "a4":
            content += to_hex(fields.get("fence_id", 0xFF), 1, "fence_id")
        return self.frame(protocol, content)

    def command_reply(self, fields: dict) -> str:
        """Online command reply. Kernel: 0x15 content = message[18:18 + (cmd_len - 4) * 2],
        0x21 (HVT001) content = message[20:20 + (packet_len - 10) * 2]."""
        if "reply_text" not in fields:
            raise ValueError("concox: send command_reply needs field 'reply_text'")
        text = str(fields["reply_text"]).encode().hex()
        server_flag = to_hex(fields.get("server_flag", 0), 4, "server_flag")
        protocol = self.protocol(fields, REPLY_PROTOCOLS)
        if protocol == "21":
            return self.frame("21", server_flag + "01" + text)        # 01 = ASCII encoding
        command_length = to_hex(len(text) // 2 + 4, 1, "message length")   # + server flag
        return self.frame("15", command_length + server_flag + text + fields.get("language", "0002"))

    def sensor_packets(self, fields: dict, at: datetime, every: bool = False) -> list[str]:
        """0x94 / 0xf1 packets for sensor values that changed since last sent (every: all sensor fields)."""
        if every:
            self.last_sensor = {key: value for key, value in self.last_sensor.items() if key not in fields}
        packets = []
        if self.changed(fields, "door1"):
            io_status = 0 if to_int(fields["door1"]) == 1 else 1     # kernel: value = not io_status
            packets.append(self.information("05", to_hex(io_status, 1, "door1")))
        if self.changed(fields, "vcc_voltage"):
            packets.append(self.information("00", to_hex(fields["vcc_voltage"], 2, "vcc_voltage")))
        if self.changed(fields, "analog_input_1"):                        # mV -> 0.01 V
            centivolt = round(to_int(fields["analog_input_1"]) / 10)
            packets.append(self.information("00", to_hex(centivolt, 2, "analog_input_1")))
        if self.changed(fields, "fuel"):
            text = f"0,0,0,0,0,0,{fields['fuel']},0"                   # kernel reads CSV item 6
            packets.append(self.information("0d", text.encode().hex()))
        temperature_changed = self.changed(fields, "temperature_1")
        humidity_changed = self.changed(fields, "humidity_1")
        if temperature_changed or humidity_changed:
            packets.append(self.information(
                "1e",
                to_hex(fields.get("temperature_1", 0), 4, "temperature_1")
                + to_hex(fields.get("humidity_1", 0), 4, "humidity_1"),
            ))
        if self.changed(fields, "ble"):
            packets.append(self.ble_status(fields["ble"], at))
        return packets

    def information(self, info_type: str, content: str) -> str:
        return self.frame("94", info_type + content)

    def ble_status(self, ble: dict, at: datetime) -> str:
        """0xf1: unix time + modules (door 1004, temperature 1002, battery 1007)."""
        mac = str(ble["mac_address"]).lower().replace(":", "")
        if len(mac) != 12:
            raise ValueError(f"concox: ble mac_address must be 6 bytes, got '{ble['mac_address']}'")
        modules = ""
        for module_id, key, size in (("1004", "door", 2), ("1002", "temperature", 2), ("1007", "battery", 1)):
            if key in ble:
                value = mac + to_hex(ble[key], size, f"ble {key}")
                modules += module_id + format(len(value) // 2, "02x") + value
        return self.frame("f1", to_hex(int(at.timestamp()), 4, "time") + modules)

    # ---- parts ----

    def gps(self, fields: dict, at: datetime, protocol: str) -> str:
        satellites = int(fields.get("gps_satellites", 0))
        if not 0 <= satellites <= 15:
            raise ValueError(f"concox: gps_satellites must be 0-15, got {satellites}")
        latitude = float(fields.get("latitude", 0))
        longitude = float(fields.get("longitude", 0))
        course = int(fields.get("direction", 0))
        if not 0 <= course < 1024:
            raise ValueError(f"concox: direction must be 0-1023, got {course}")

        status = course
        status |= int(bool(fields.get("position_status", True))) << 12
        status |= int(longitude < 0) << 11         # 1 = west
        status |= int(latitude >= 0) << 10         # 1 = north
        if fields.get("acc_in_course", protocol == "12"):    # bit 14 = acc valid, bit 15 = acc
            status |= 1 << 14
            status |= to_int(fields.get("engine_on", 0)) << 15

        return (
            self.datetime_hex(at, protocol)
            + format(GPS_INFO_LENGTH, "x") + format(satellites, "x")
            + to_hex(round(abs(latitude) * 1_800_000), 4, "latitude")
            + to_hex(round(abs(longitude) * 1_800_000), 4, "longitude")
            + to_hex(int(fields.get("speed", 0)), 1, "speed")
            + to_hex(status, 2, "course")
        )

    def lbs(self, fields: dict, protocol: str, with_length: bool) -> str:
        lbs = {**DEFAULT_LBS, **(fields.get("lbs") or {})}
        four_g = protocol in FOUR_G_PROTOCOLS
        content = (
            to_hex(lbs["mcc"], 2, "mcc") + to_hex(lbs["mnc"], 1, "mnc")
            + to_hex(lbs["lac"], 4 if four_g else 2, "lac")
            + to_hex(lbs["cell_id"], 8 if four_g else 3, "cell_id")
        )
        if with_length:
            content = format(len(content) // 2 + 1, "02x") + content
        return content

    def terminal(self, fields: dict) -> str:
        """Terminal info byte: bit6 = GPS positioned, bit1 = acc."""
        if "terminal_info" in fields:
            return to_hex(fields["terminal_info"], 1, "terminal_info")
        value = int(bool(fields.get("position_status", True))) << 6
        value |= to_int(fields.get("engine_on", 0)) << 1
        return to_hex(value, 1, "terminal_info")

    def datetime_hex(self, at: datetime, protocol: str) -> str:
        tz_hours = self.device_tz if protocol in LOCAL_TIME_PROTOCOLS else 0
        local = at.astimezone(timezone(timedelta(hours=tz_hours)))
        return "".join(
            format(part, "02x")
            for part in (local.year - 2000, local.month, local.day, local.hour, local.minute, local.second)
        )

    def frame(self, protocol: str, content: str) -> str:
        """start + length + protocol + content + serial + crc + stop."""
        self.serial = (self.serial + 1) & 0xFFFF
        body = protocol + content + format(self.serial, "04x")
        length = len(body) // 2 + 2                      # + crc
        if protocol in ("94", "f1", "21"):
            head = START_LONG + format(length, "04x")
        else:
            head = START_SHORT + format(length, "02x")
        crc_input = head[4:] + body
        return head + body + format(crc_itu(crc_input), "04x") + STOP

    # ---- helpers ----

    @staticmethod
    def protocol(fields: dict, allowed: tuple) -> str:
        protocol = str(fields.get("protocol", allowed[0])).lower()
        if protocol not in allowed:
            raise UnsupportedFieldError(f"concox: protocol '{protocol}' not in {allowed}")
        return protocol

    def changed(self, fields: dict, key: str) -> bool:
        if key not in fields or self.last_sensor.get(key) == fields[key]:
            return False
        self.last_sensor[key] = fields[key]
        return True
