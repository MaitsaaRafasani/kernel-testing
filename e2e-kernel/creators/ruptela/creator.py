"""Ruptela creator: fields -> raw hex packets, readable by kernel v2 parser (family/ruptela/ruptela.py).

Frame: length(2) + imei(8, as number) + command(1) + payload + crc(2).
Length and CRC-16/Kermit cover imei..payload.

send -> command:
- login:         15 identification
- location:      68 extended records (command 1 can't carry IO acc id 409, which the kernel needs for a location)
- heartbeat:     16 (kernel makes no packet from it)
- command_reply: 7, field `reply_text`
Batch (buffered data): one packet with many records.

Record: time UTC, longitude / latitude x1e7, altitude x10, direction x100, speed km/h, hdop x10,
  priority (default 0: kernel makes location / sensor only for priority 0), event_id (default 7 = time record).
IO, device units (no scaling):
- engine_on -> acc (always sent, default off), gsm_status level 0-4 -> signal (CSQ, always sent, default 4)
- vcc_voltage (mV) -> power_supply_voltage, mileage (m) -> virtual_odometer
- door1 / door2 -> digital_input_2 / 3, temperature_1..3 (0.1 C) -> dallas_temperature_0..2
- io: {name: value} or the IO name as field (IO below), io_raw: [{id, size: 1|2|4|8, value}]
"""
from datetime import datetime, timezone

from creators.base import FamilyCreator, IdentifierFormat, UnsupportedFieldError, WORD_VALUES, to_hex

COMMAND_REPLY = 7
IDENTIFICATION = 15
HEARTBEAT = 16
EXTENDED_RECORDS = 68
MAX_RECORDS = 255                       # record count is 1 byte
MAX_IO_PER_SIZE = 255                   # io count per size is 1 byte
SIZES = (1, 2, 4, 8)
IDENTIFICATION_HEADER = "0000"          # 2 bytes skipped by kernel, no parameters after it
DEFAULT_EVENT_ID = 7                    # time based record
DEFAULT_HDOP = 1.0

# kernel PinCodes (enums/sensor_enum.py)
PIN_DIGITAL_2 = 2
PIN_DIGITAL_3 = 3
PIN_DALLAS1_WIRE1 = 11
PIN_DALLAS1_WIRE2 = 12
PIN_DALLAS1_WIRE3 = 13

# io name (kernel RuptelaIO, lowercase) -> (id, size in bytes)
IO = {
    "digital_input_1": (2, 1), "digital_input_2": (3, 1), "digital_input_3": (4, 1), "digital_input_4": (5, 1),
    "analog_input_3": (20, 2), "analog_input_4": (21, 2), "analog_input_1": (22, 2), "analog_input_2": (23, 2),
    "analog_input_1_delta": (26, 2), "signal": (27, 1), "current_profile": (28, 1),
    "power_supply_voltage": (29, 2), "battery_voltage": (30, 2), "analog_input_2_delta": (33, 2),
    "ibutton_driver_id": (34, 8), "virtual_odometer": (65, 4),
    "dallas_temperature_3": (74, 2), "dallas_temperature_0": (78, 2),
    "dallas_temperature_1": (79, 2), "dallas_temperature_2": (80, 2),
    "gsm_jamming": (88, 1), "can_engine_total_fuel_used": (92, 4), "digital_output_3": (111, 1),
    "can_bus_distance": (114, 4), "harsh_brake": (134, 1), "harsh_acceleration": (136, 1), "sharp_turn": (143, 1),
    "gsm_operator": (150, 4), "movement_sensor": (173, 1),
    "x_axis_positive": (402, 2), "x_axis_negative": (403, 2), "y_axis": (404, 2),
    "digital_output_1": (405, 1), "digital_output_2": (406, 1), "acc": (409, 1), "gprs_status": (418, 1),
    "gsm_extended_error_report": (560, 2), "network_technology": (762, 1), "network_registration_status": (763, 1),
    "axl_calibration_status": (905, 1), "analog_input_3_delta": (916, 2), "analog_input_4_delta": (917, 2),
    "unplug_detection": (985, 1), "digital_input_5": (988, 1), "digital_input_6": (989, 1),
    **{f"custom_can_{number}": (1200 + number, 8) for number in range(1, 21)},
}
SIGNED_IO = {
    "dallas_temperature_0", "dallas_temperature_1", "dallas_temperature_2", "dallas_temperature_3",
    "x_axis_positive", "x_axis_negative", "y_axis",
    "analog_input_1_delta", "analog_input_2_delta", "analog_input_3_delta", "analog_input_4_delta",
}

# generic field -> io name
GENERIC_IO = {"engine_on": "acc", "vcc_voltage": "power_supply_voltage", "mileage": "virtual_odometer"}

# sensor code -> (io name, kernel pin code), same pins as teltonika
SENSOR_IO = {
    "door1": ("digital_input_2", PIN_DIGITAL_2),
    "door2": ("digital_input_3", PIN_DIGITAL_3),
    "temperature_1": ("dallas_temperature_0", PIN_DALLAS1_WIRE1),
    "temperature_2": ("dallas_temperature_1", PIN_DALLAS1_WIRE2),
    "temperature_3": ("dallas_temperature_2", PIN_DALLAS1_WIRE3),
}

# gsm level 0-4 -> CSQ value the kernel converts back to the same level
GSM_LEVEL_SIGNAL = {0: 0, 1: 1, 2: 5, 3: 15, 4: 25}

GPS_FIELDS = ("latitude", "longitude", "altitude", "direction", "speed", "gps_satellites", "hdop")
OPTION_FIELDS = ("gsm_status", "io", "io_raw", "priority", "event_id", "reply_text")


def encode_value(value, size: int, name: str) -> str:
    """int (signed io = two's complement) or raw hex str -> hex of exactly `size` bytes."""
    if isinstance(value, str) and value.strip().lower() not in WORD_VALUES:
        raw = value.strip().lower()
        if len(raw) != size * 2:
            raise ValueError(f"{name}: raw hex must be {size} bytes, got '{value}'")
        bytes.fromhex(raw)
        return raw
    return to_hex(value, size, name, signed=name in SIGNED_IO)


def crc16_kermit(data_hex: str) -> int:
    """CRC-16/Kermit, same as kernel helper crc16_kermit."""
    crc = 0
    for byte in bytes.fromhex(data_hex):
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0x8408 if crc & 1 else crc >> 1
    return crc


class RuptelaCreator(FamilyCreator):
    family = "ruptela"
    default_model = "fm_pro4"
    identifier_format = IdentifierFormat(length=15)
    initial_packets = ("login",)        # identification, kernel turns it into LoginPacket
    sensor_pins = {code: pin for code, (_, pin) in SENSOR_IO.items()}
    supported_fields = set(GPS_FIELDS) | set(GENERIC_IO) | set(SENSOR_IO) | set(OPTION_FIELDS) | set(IO)

    def build(self, packet: str, fields: dict, at: datetime, imei: str) -> list[str]:
        if packet == "login":
            return [self.frame(imei, IDENTIFICATION, IDENTIFICATION_HEADER)]
        if packet in ("location", "sensor"):
            return [self.records([(fields, at)], imei)]
        if packet == "heartbeat":
            return [self.frame(imei, HEARTBEAT, "")]
        if packet == "command_reply":
            if "reply_text" not in fields:
                raise ValueError("ruptela: send command_reply needs field 'reply_text'")
            return [self.frame(imei, COMMAND_REPLY, str(fields["reply_text"]).encode().hex())]
        raise UnsupportedFieldError(f"ruptela: send '{packet}' not supported, use location + io")

    def build_batch(self, items: list[tuple[str, dict, datetime]], imei: str) -> str:
        """Buffered data: one records packet with many records (in the given order)."""
        if any(send not in ("location", "sensor") for send, _, _ in items):
            raise UnsupportedFieldError("ruptela: batch only supports send location / sensor")
        return self.records([(fields, at) for _, fields, at in items], imei)

    def records(self, records: list[tuple[dict, datetime]], imei: str) -> str:
        if len(records) > MAX_RECORDS:
            raise ValueError(f"ruptela: {len(records)} records, max {MAX_RECORDS} per packet")
        body = "".join(self.record(fields, at) for fields, at in records)
        payload = "00" + format(len(records), "02x") + body          # records left + record count
        return self.frame(imei, EXTENDED_RECORDS, payload)

    def record(self, fields: dict, at: datetime) -> str:
        unknown = set(fields) - self.supported_fields
        if unknown:
            raise UnsupportedFieldError(f"ruptela: field not supported: {sorted(unknown)}")
        return (
            to_hex(int(at.astimezone(timezone.utc).timestamp()), 4, "time")
            + "00"                                                    # timestamp extension (unused in 68)
            + "00"                                                    # record extension: 1 of 1 record
            + to_hex(fields.get("priority", 0), 1, "priority")
            + to_hex(round(float(fields.get("longitude", 0)) * 10_000_000), 4, "longitude", signed=True)
            + to_hex(round(float(fields.get("latitude", 0)) * 10_000_000), 4, "latitude", signed=True)
            + to_hex(round(float(fields.get("altitude", 0)) * 10), 2, "altitude")
            + to_hex(round(float(fields.get("direction", 0)) * 100), 2, "direction")
            + to_hex(fields.get("gps_satellites", 0), 1, "gps_satellites")
            + to_hex(fields.get("speed", 0), 2, "speed")
            + to_hex(round(float(fields.get("hdop", DEFAULT_HDOP)) * 10), 1, "hdop")
            + to_hex(fields.get("event_id", DEFAULT_EVENT_ID), 2, "event_id")
            + self.io_element(fields)
        )

    def collect_io(self, fields: dict) -> list[tuple[int, int, str]]:
        """[(io id, size, value hex)] in write order."""
        values = {"acc": 0, "signal": GSM_LEVEL_SIGNAL[4]}       # always sent, like a real device
        if "gsm_status" in fields:
            level = int(fields["gsm_status"])
            if level not in GSM_LEVEL_SIGNAL:
                raise ValueError(f"ruptela: gsm_status must be 0-4, got {level}")
            values["signal"] = GSM_LEVEL_SIGNAL[level]
        for field, io_name in GENERIC_IO.items():
            if field in fields:
                values[io_name] = fields[field]
        for code, (io_name, _) in SENSOR_IO.items():
            if code in fields:
                values[io_name] = fields[code]
        values.update({name: value for name, value in fields.items() if name in IO})
        for name, value in (fields.get("io") or {}).items():
            if name not in IO:
                raise UnsupportedFieldError(f"ruptela: unknown io '{name}', use io_raw")
            values[name] = value

        items = [(IO[name][0], IO[name][1], encode_value(value, IO[name][1], name)) for name, value in values.items()]
        for raw in fields.get("io_raw") or []:
            size = int(raw["size"])
            if size not in SIZES:
                raise ValueError(f"ruptela: io_raw {raw['id']} size must be one of {SIZES}")
            items.append((int(raw["id"]), size, encode_value(raw["value"], size, f"io_raw {raw['id']}")))
        return items

    def io_element(self, fields: dict) -> str:
        """Per size 1 / 2 / 4 / 8: count(1) + [id(2) + value]."""
        items = self.collect_io(fields)
        sections = ""
        for size in SIZES:
            entries = [(io_id, value) for io_id, item_size, value in items if item_size == size]
            if len(entries) > MAX_IO_PER_SIZE:
                raise ValueError(f"ruptela: more than {MAX_IO_PER_SIZE} io of {size} bytes")
            sections += format(len(entries), "02x")
            sections += "".join(to_hex(io_id, 2, "io id") + value for io_id, value in entries)
        return sections

    @staticmethod
    def frame(imei: str, command: int, payload: str) -> str:
        if len(imei) != 15 or not imei.isdigit():
            raise ValueError(f"ruptela: imei must be 15 digits, got '{imei}'")
        body = format(int(imei), "016x") + format(command, "02x") + payload
        return format(len(body) // 2, "04x") + body + format(crc16_kermit(body), "04x")
