"""Topflytech creator: fields -> raw hex packets, readable by kernel v2 parser (family/topflytech/topflytech.py).

Frame: 2727 + protocol(1) + length(2, whole frame) + serial(2) + imei(8, "0" + 15 digits) + body. No CRC.

send -> packets:
- login:         0x01
- location:      0x02 (+ 0x12 when field `ble` is set)
- sensor:        0x12 BLE + GPS, field `ble`
- heartbeat:     0x03 (kernel makes no packet from it)
- alarm:         0x04, field `alarm` = name (ALARM_CODES) or code like '0x03'
- command_reply: 0x81, field `reply_text` (UTF-16LE)

Location / alarm body (0x02 / 0x04):
- time UTC as BCD digits, latitude / longitude / altitude float32 little-endian (~1 m precision)
- speed km/h (BCD x10), direction degrees, gps_satellites 0-31 (kernel position_status = satellites > 5)
- engine_on (status bit 2), gsm_status level 0-4 (sent as signal (level + 1) x 20, kernel converts back)
- battery (%, 1-100), battery_voltage / solar_voltage (V), solar_charging, mileage (km), device_temperature (C)
- buffered data (batch): status bit 7 = 1

BLE (0x12): ble = {sensor: temperature | door, mac_address, temperature (C), humidity (%), door (open/closed),
  battery (%), voltage (V)}. Kernel pin BLE_TEMPERATURE_GENERAL / BLE_DOOR_GENERAL, matched by mac_address.
  Missing temperature = ffff (kernel: BLE temperature sensor error alarm).
"""
import struct
from datetime import datetime, timezone

from creators.base import FamilyCreator, IdentifierFormat, UnsupportedFieldError, parse_mac, to_hex, to_int

START = "2727"
LOGIN = "01"
LOCATION = "02"
HEARTBEAT = "03"
ALARM = "04"
BLE_LOCATION = "12"
COMMAND_REPLY = "81"
HEADER_BYTES = 15                       # start 2 + protocol 1 + length 2 + serial 2 + imei 8

LOGIN_VERSIONS = "0106" + "240522" + "1006" + "10"   # mcu, modem, modem app, hardware (kernel test sample)
ACCELERATION = "00" * 5
SETTINGS = "00" * 18                    # upload intervals, heartbeat, setting status, reserved (not read by kernel)
NOT_SUPPORTED = "ff"
GNSS_FLAG = 0x40                        # status bit 6: GNSS data (else LBS)
BUFFER_FLAG = 0x80                      # status bit 7: buffered data
REPLY_TYPE = "01"
BLE_RSSI = "50"
BLE_LIGHT = "0000"

# alarm name (kernel AlertCodes, lowercase) -> topflytech alarm code (enums/topflytech_enum.py)
ALARM_CODES = {
    "tamper": "01", "cover_removed": "02", "sos": "03", "device_temperature_high": "08", "vibration": "09",
    "usb_cable_connected": "12", "usb_cable_disconnected": "13", "start_moving": "16", "stop_moving": "17",
    "power_on": "20", "device_mounted": "21", "cover_putback": "22", "power_off": "29",
    "device_temperature_low": "30", "gsm_jamming_start": "66", "gsm_jamming_stop": "67", "overspeeds": "70",
}
BLE_CODES = {"temperature": "0004", "door": "0005"}

GPS_FIELDS = ("latitude", "longitude", "altitude", "speed", "direction", "gps_satellites", "engine_on")
STATUS_FIELDS = (
    "gsm_status", "battery", "battery_voltage", "solar_voltage", "solar_charging", "mileage", "device_temperature",
)
OPTION_FIELDS = ("alarm", "ble", "reply_text", "serial", "buffered")


def bcd(value, digits: int, name: str) -> str:
    """Decimal digits written as hex (kernel reads int(hex_text) without base 16)."""
    number = int(value)
    text = f"{number:0{digits}d}"
    if number < 0 or len(text) != digits:
        raise ValueError(f"{name}: value {number} does not fit in {digits} BCD digits")
    return text


def float_le(value) -> str:
    return struct.pack("<f", float(value)).hex()


def temperature_hex(value) -> str:
    """BLE temperature: 2 bytes, bit 15 = sign, rest = |C| x 100. None = ffff (invalid)."""
    if value is None:
        return "ffff"
    raw = round(abs(float(value)) * 100)
    if raw > 0x7FFF:
        raise ValueError(f"topflytech: ble temperature {value} out of range")
    return format(raw | (0x8000 if float(value) < 0 else 0), "04x")


class TopflytechCreator(FamilyCreator):
    family = "topflytech"
    default_model = "solarx-110"
    identifier_format = IdentifierFormat(length=15)
    initial_packets = ("login",)
    supported_fields = set(GPS_FIELDS) | set(STATUS_FIELDS) | set(OPTION_FIELDS)

    def __init__(self):
        self.serial = 0

    def build(self, packet: str, fields: dict, at: datetime, imei: str) -> list[str]:
        unknown = set(fields) - self.supported_fields
        if unknown:
            raise UnsupportedFieldError(f"topflytech: field not supported: {sorted(unknown)}")
        if "serial" in fields:
            self.serial = int(fields["serial"]) - 1

        if packet == "login":
            return [self.frame(LOGIN, imei, LOGIN_VERSIONS)]
        if packet == "location":
            packets = [self.frame(LOCATION, imei, self.gps_report(fields, at, "00"))]
            if "ble" in fields:
                packets.append(self.ble_location(fields, at, imei))
            return packets
        if packet == "sensor":
            if "ble" not in fields:
                raise ValueError("topflytech: send sensor needs field 'ble'")
            return [self.ble_location(fields, at, imei)]
        if packet == "heartbeat":
            return [self.frame(HEARTBEAT, imei, "")]
        if packet == "alarm":
            return [self.frame(ALARM, imei, self.gps_report(fields, at, self.alarm_code(fields)))]
        if packet == "command_reply":
            if "reply_text" not in fields:
                raise ValueError("topflytech: send command_reply needs field 'reply_text'")
            text = str(fields["reply_text"]).encode("utf-16-le").hex()
            return [self.frame(COMMAND_REPLY, imei, REPLY_TYPE + text)]
        raise UnsupportedFieldError(f"topflytech: send '{packet}' not supported")

    def build_batch(self, items: list[tuple[str, dict, datetime]], imei: str) -> str:
        """Buffered data: frames concatenated (kernel _split_data splits them), status bit 7 = buffered."""
        return super().build_batch([(send, {"buffered": True, **fields}, at) for send, fields, at in items], imei)

    # ---- bodies ----

    def gps_report(self, fields: dict, at: datetime, alarm_code: str) -> str:
        """0x02 / 0x04 body after imei (58 bytes)."""
        signal = (to_int(fields.get("gsm_status", 4)) + 1) * 20
        if not 20 <= signal <= 100:
            raise ValueError(f"topflytech: gsm_status must be 0-4, got {fields['gsm_status']}")
        status = signal << 4 | to_int(fields.get("solar_charging", 0)) << 3 | to_int(fields.get("engine_on", 0)) << 2
        return (
            self.gnss_status(fields) + alarm_code + self.time_bcd(at) + self.gnss(fields) + ACCELERATION
            + self.battery(fields.get("battery")) + self.device_temperature(fields.get("device_temperature", 0))
            + NOT_SUPPORTED                                                    # front light sensor
            + self.voltage(fields.get("battery_voltage"), "battery_voltage")
            + self.voltage(fields.get("solar_voltage"), "solar_voltage")
            + to_hex(round(float(fields.get("mileage", 0)) * 1000), 4, "mileage")
            + to_hex(status, 2, "status") + SETTINGS
        )

    def ble_location(self, fields: dict, at: datetime, imei: str) -> str:
        """0x12: time + ignition + gnss status + gnss + ble code + ble data."""
        ble = fields["ble"]
        sensor = ble.get("sensor")
        if sensor not in BLE_CODES:
            raise UnsupportedFieldError(f"topflytech: ble sensor must be one of {sorted(BLE_CODES)}, got '{sensor}'")
        voltage = to_hex(round(float(ble["voltage"]) * 100) - 200, 1, "ble voltage") if "voltage" in ble else None
        data = (
            parse_mac(ble.get("mac_address"), "topflytech: ble mac_address")
            + (voltage or NOT_SUPPORTED)
            + (to_hex(ble["battery"], 1, "ble battery") if "battery" in ble else NOT_SUPPORTED)
            + temperature_hex(ble.get("temperature"))
        )
        if sensor == "temperature":
            data += to_hex(round(float(ble.get("humidity", 0)) * 100), 2, "ble humidity") + BLE_LIGHT
        else:
            data += to_hex(ble["door"], 1, "ble door") if "door" in ble else NOT_SUPPORTED
        body = (
            self.time_bcd(at) + to_hex(fields.get("engine_on", 0), 1, "engine_on") + self.gnss_status(fields)
            + self.gnss(fields) + BLE_CODES[sensor] + data + BLE_RSSI
        )
        return self.frame(BLE_LOCATION, imei, body)

    # ---- parts ----

    @staticmethod
    def gnss_status(fields: dict) -> str:
        satellites = int(fields.get("gps_satellites", 0))
        if not 0 <= satellites <= 31:
            raise ValueError(f"topflytech: gps_satellites must be 0-31, got {satellites}")
        flags = GNSS_FLAG | (BUFFER_FLAG if fields.get("buffered") else 0)
        return format(flags | satellites, "02x")

    @staticmethod
    def gnss(fields: dict) -> str:
        """altitude, longitude, latitude float32 LE + speed BCD x10 + direction (16 bytes)."""
        return (
            float_le(fields.get("altitude", 0)) + float_le(fields.get("longitude", 0))
            + float_le(fields.get("latitude", 0))
            + bcd(round(float(fields.get("speed", 0)) * 10), 4, "speed")
            + to_hex(int(fields.get("direction", 0)), 2, "direction")
        )

    @staticmethod
    def time_bcd(at: datetime) -> str:
        utc = at.astimezone(timezone.utc)
        parts = (utc.year - 2000, utc.month, utc.day, utc.hour, utc.minute, utc.second)
        return "".join(f"{part:02d}" for part in parts)

    @staticmethod
    def battery(value) -> str:
        """Percent as BCD, kernel reads 00 as 100 and ff as not supported."""
        if value is None:
            return NOT_SUPPORTED
        percent = int(value)
        if not 1 <= percent <= 100:
            raise ValueError(f"topflytech: battery must be 1-100 %, got {percent}")
        return "00" if percent == 100 else bcd(percent, 2, "battery")

    @staticmethod
    def voltage(value, name: str) -> str:
        """Volt x 10 as BCD (0-9.9 V), ff = not supported."""
        return NOT_SUPPORTED if value is None else bcd(round(float(value) * 10), 2, name)

    @staticmethod
    def device_temperature(value) -> str:
        """bit 7 = sign, bits 0-6 = |C|."""
        degrees = round(float(value))
        if abs(degrees) > 0x7F:
            raise ValueError(f"topflytech: device_temperature {degrees} out of range")
        return format(abs(degrees) | (0x80 if degrees < 0 else 0), "02x")

    @staticmethod
    def alarm_code(fields: dict) -> str:
        name = fields.get("alarm")
        if name is None:
            raise ValueError("topflytech: send alarm needs field 'alarm'")
        if isinstance(name, str) and name.lower().startswith("0x"):
            return to_hex(int(name, 16), 1, "alarm")
        if name not in ALARM_CODES:
            raise UnsupportedFieldError(f"topflytech: alarm '{name}' not supported, use code like '0x03'")
        return ALARM_CODES[name]

    def frame(self, protocol: str, imei: str, body: str) -> str:
        if len(imei) != 15 or not imei.isdigit():
            raise ValueError(f"topflytech: imei must be 15 digits, got '{imei}'")
        self.serial = (self.serial + 1) & 0xFFFF
        length = HEADER_BYTES + len(body) // 2
        return START + protocol + format(length, "04x") + format(self.serial, "04x") + "0" + imei + body
