"""Queclink creator (HEX protocol): fields -> raw hex frames, readable by kernel v2 family/queclink/queclink.py.

Frame: header(4, ASCII) + [message type(1)] + body + checksum(2) + tail 0d0a.
Checksum = CRC-16/CCITT-FALSE over message type..count number. Length field = whole frame.
No login packet (kernel v2 has none for queclink): send login builds nothing.

send -> frame:
- location:      +RSP FRI (07), +BSP when `buffered: true`. With `one_wire`: +RSP ERI (12, GV58CG layout) with the
                 1-wire block: one_wire = {sensor id (8 bytes hex, ex: 28FF4A1B6416034E): raw temperature}, raw =
                 signed 1/16 C (DS18B20, kernel C = raw x 0.0625), in the given order. A missing id = sensor not read.
- heartbeat:     +HBD
- alarm:         field `alarm` (ALARMS): sos (+RSP SOS), overspeeds (+RSP SPD, kernel needs engine_on),
                 power_on / disarm (+EVT MPN / MPF), gsm_jamming_start (+EVT JDR, field `jamming_net`),
                 charge_start / charge_stop (+EVT BTC / STC); +BVT / +BSP when `buffered: true`
- command_reply: +RESP ASCII, field `reply_text` = "+RESP:GT...". Without "$" at the end, send time + count are added.
Batch (buffered data): one +BSP / +BVT frame per gate message, the kernel parses one frame per message.

+RSP / +EVT body (report mask decides which fields exist):
- always: length, unique id (IMEI), motion status (engine_on + speed), satellites, speed, azimuth, altitude,
  longitude / latitude (x1e6), GNSS UTC time, send time (= step time), count number
- only when set: vcc_voltage (mV, external power), analog_input_1..3 (mV), mileage (km, total mileage),
  digital_input_1..4 / digital_output_1..4 / door1 (= digital_input_2) / door2 (= digital_input_3)
- position_status: GNSS accuracy 1 (true) / 0 (false). Kernel position_status = satellites > 5.
Device type is never sent: its width depends on the model.
"""
from datetime import datetime, timezone

from creators.base import FamilyCreator, IdentifierFormat, UnsupportedFieldError, to_hex, to_int

RSP, BSP, EVT, BVT, HBD, RESP = "+RSP", "+BSP", "+EVT", "+BVT", "+HBD", "+RESP:"
BUFFERED_HEADERS = {RSP: BSP, EVT: BVT}
FRI = "07"
ERI = "12"
ERI_ONE_WIRE = 1 << 1                   # <ERI Mask> bit 1: 1-wire data (enums/queclink_enum.py EriMaskBit)
ONE_WIRE_TEMPERATURE = "01"             # <1-wire Device Type> DS18B20
ONE_WIRE_DATA_LENGTH = "02"
SOS = "0a"
TAIL = "0d0a"
CHECKSUM_TAIL_BYTES = 4
HEADER_HEX = 8                          # "+RSP" etc. = 4 bytes, not in the checksum

# <+RSP Mask> / <+EVT Mask> bits (enums/queclink_enum.py ReportMaskBit + status block)
SPEED, AZIMUTH, ALTITUDE = 1 << 0, 1 << 1, 1 << 2
SEND_TIME, COUNT_NUMBER, LENGTH = 1 << 4, 1 << 5, 1 << 7
EXTERNAL_POWER, ANALOG_1, ANALOG_2, ANALOG_3 = 1 << 12, 1 << 14, 1 << 15, 1 << 30
DIGITAL_IO, MOTION_STATUS, SATELLITES, TOTAL_MILEAGE = 1 << 17, 1 << 18, 1 << 19, 1 << 21
BASE_MASK = SPEED | AZIMUTH | ALTITUDE | SEND_TIME | COUNT_NUMBER | LENGTH | MOTION_STATUS | SATELLITES
ANALOG_BITS = (("analog_input_1", ANALOG_1), ("analog_input_2", ANALOG_2), ("analog_input_3", ANALOG_3))
MAX_SHORT_VOLTAGE = 0x7FFF              # 2-byte external power (MSB set = 4-byte form)

# <+HBD Mask>: length, send time, count number, unique id
HBD_MASK = 1 << 0 | 1 << 5 | 1 << 6 | 1 << 7

# motion status (enums/queclink_enum.py MotionStatus): (engine_on, moving) -> value
MOTION = {(False, False): 0x11, (False, True): 0x12, (True, False): 0x21, (True, True): 0x22}

# alarm name -> (header, message type, body)
ALARMS = {
    "sos": (RSP, SOS), "overspeeds": (RSP, "09"),
    "power_on": (EVT, "03"), "disarm": (EVT, "04"), "gsm_jamming_start": (EVT, "14"),
    "charge_start": (EVT, "07"), "charge_stop": (EVT, "08"),
}
JDR = "14"

# kernel PinCodes (enums/sensor_enum.py)
PIN_DIGITAL_2 = 2
PIN_DIGITAL_3 = 3

# digital input bit 0 = ignition, bit n = input n. door sensors use the same pins as teltonika.
DOOR_INPUTS = {"door1": "digital_input_2", "door2": "digital_input_3"}
DIGITAL_INPUTS = tuple(f"digital_input_{number}" for number in range(1, 5))
DIGITAL_OUTPUTS = tuple(f"digital_output_{number}" for number in range(1, 5))

GPS_FIELDS = ("latitude", "longitude", "altitude", "speed", "direction", "gps_satellites", "position_status")
STATUS_FIELDS = (
    ("engine_on", "vcc_voltage", "mileage", "motion_status")
    + tuple(name for name, _ in ANALOG_BITS) + DIGITAL_INPUTS + DIGITAL_OUTPUTS + tuple(DOOR_INPUTS)
)
OPTION_FIELDS = ("alarm", "jamming_net", "reply_text", "buffered", "count", "one_wire")


def crc16_ccitt(data_hex: str) -> int:
    """CRC-16/CCITT-FALSE, same as kernel helper crc16_ccitt."""
    crc = 0xFFFF
    for byte in bytes.fromhex(data_hex):
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def unique_id(imei: str) -> str:
    """IMEI packed in 8 bytes: 7 bytes of 2 digits each + 1 byte of the last digit."""
    if len(imei) != 15 or not imei.isdigit():
        raise ValueError(f"queclink: imei must be 15 digits, got '{imei}'")
    return "".join(format(int(imei[i:i + 2]), "02x") for i in range(0, 14, 2)) + format(int(imei[14]), "02x")


def time_hex(at: datetime) -> str:
    """7 bytes UTC: year(2) month day hour minute second."""
    utc = at.astimezone(timezone.utc)
    return format(utc.year, "04x") + "".join(
        format(part, "02x") for part in (utc.month, utc.day, utc.hour, utc.minute, utc.second))


def fraction(value, int_bytes: int, name: str) -> str:
    """Integer part + 1 byte of tenths (speed, mileage)."""
    tenths = round(float(value) * 10)
    return to_hex(tenths // 10, int_bytes, name) + to_hex(tenths % 10, 1, name)


class QueclinkCreator(FamilyCreator):
    family = "queclink"
    default_model = "gv58cg"
    identifier_format = IdentifierFormat(length=15)
    sensor_pins = {"door1": PIN_DIGITAL_2, "door2": PIN_DIGITAL_3}
    supported_fields = set(GPS_FIELDS) | set(STATUS_FIELDS) | set(OPTION_FIELDS)

    def __init__(self):
        self.count = 0

    def build(self, packet: str, fields: dict, at: datetime, imei: str) -> list[str]:
        unknown = set(fields) - self.supported_fields
        if unknown:
            raise UnsupportedFieldError(f"queclink: field not supported: {sorted(unknown)}")
        if "count" in fields:
            self.count = int(fields["count"]) - 1

        if packet == "login":
            return []                   # queclink has no login packet: nothing is sent
        if packet == "location":
            return [self.report(RSP, FRI, fields, at, imei)]
        if packet == "alarm":
            name = fields.get("alarm")
            if name not in ALARMS:
                raise UnsupportedFieldError(f"queclink: alarm '{name}' not supported, use one of {sorted(ALARMS)}")
            header, message_type = ALARMS[name]
            return [self.report(header, message_type, fields, at, imei)]
        if packet == "heartbeat":
            return [self.heartbeat(at, imei)]
        if packet == "command_reply":
            return [self.command_reply(fields, at)]
        raise UnsupportedFieldError(f"queclink: send '{packet}' not supported")

    def build_batch(self, items: list[tuple[str, dict, datetime]], imei: str) -> str:
        raise UnsupportedFieldError("queclink: kernel parses one frame per message, use build_batch_messages")

    def build_batch_messages(self, items: list[tuple[str, dict, datetime]], imei: str) -> list[str]:
        """Buffered data: one +BSP / +BVT frame per gate message (kernel parses one frame per message)."""
        return [
            packet for send, fields, at in items
            for packet in self.build(send, {"buffered": True, **fields}, at, imei)
        ]

    # ---- frames ----

    def report(self, header: str, message_type: str, fields: dict, at: datetime, imei: str) -> str:
        """+RSP / +EVT: mask [+ ERI mask] + length + unique id + status + [report id] [+ reserved + 1-wire block]
        + position + mileage + send time + count."""
        mask, status = self.status(fields)
        eri_mask = ""
        if "one_wire" in fields:
            if (header, message_type) != (RSP, FRI):
                raise UnsupportedFieldError("queclink: one_wire only in send location")
            message_type, eri_mask = ERI, to_hex(ERI_ONE_WIRE, 4, "eri mask")
            status += "00" + "00" + self.one_wire_block(fields["one_wire"])    # report id|type + reserved
        elif header == RSP:
            status += "00" + ("00" if message_type == SOS else "")      # report id|type [+ SOS reserved]
        elif message_type == JDR:
            status += to_hex(fields.get("jamming_net", 0), 1, "jamming_net")
        body = (
            status + "01" + to_hex(int(bool(fields.get("position_status", True))), 1, "position_status")
            + fraction(fields.get("speed", 0), 2, "speed")
            + to_hex(int(fields.get("direction", 0)), 2, "direction")
            + to_hex(round(float(fields.get("altitude", 0))), 2, "altitude", signed=True)
            + to_hex(round(float(fields.get("longitude", 0)) * 1_000_000), 4, "longitude", signed=True)
            + to_hex(round(float(fields.get("latitude", 0)) * 1_000_000), 4, "latitude", signed=True)
            + time_hex(at)
            + (fraction(fields["mileage"], 4, "mileage") if "mileage" in fields else "")
            + time_hex(at) + self.next_count()
        )
        if fields.get("buffered"):
            header = BUFFERED_HEADERS[header]
        head = header.encode().hex() + message_type + to_hex(mask, 4, "mask") + eri_mask
        # length (2 bytes) counts the whole frame
        length = (len(head) + len(body)) // 2 + 2 + 8 + CHECKSUM_TAIL_BYTES
        return self.frame(head, to_hex(length, 2, "length") + unique_id(imei) + body)

    def heartbeat(self, at: datetime, imei: str) -> str:
        head = HBD.encode().hex() + to_hex(HBD_MASK, 1, "mask")
        body = unique_id(imei) + time_hex(at) + self.next_count()
        length = (len(head) + len(body)) // 2 + 1 + CHECKSUM_TAIL_BYTES
        return self.frame(head, to_hex(length, 1, "length") + body)

    def command_reply(self, fields: dict, at: datetime) -> str:
        """+RESP ASCII, no checksum. Kernel reads send time from text[-20:-6]."""
        text = str(fields.get("reply_text", ""))
        if not text.startswith(RESP):
            raise ValueError(f"queclink: reply_text must start with '{RESP}', got '{text}'")
        if not text.endswith("$"):
            text += f",{at.astimezone(timezone.utc):%Y%m%d%H%M%S},{self.next_count().upper()}$"
        return text.encode().hex()

    # ---- parts ----

    def status(self, fields: dict) -> tuple[int, str]:
        """Mask + status block in wire order: external power, AI1, AI2, AI3, digital io, motion, satellites."""
        mask = BASE_MASK | (TOTAL_MILEAGE if "mileage" in fields else 0)
        status = ""
        if "vcc_voltage" in fields:
            voltage = to_int(fields["vcc_voltage"])
            if not 0 <= voltage <= MAX_SHORT_VOLTAGE:
                raise ValueError(f"queclink: vcc_voltage must be 0-{MAX_SHORT_VOLTAGE} mV, got {voltage}")
            mask |= EXTERNAL_POWER
            status += to_hex(voltage, 2, "vcc_voltage")
        for name, bit in ANALOG_BITS:
            if name in fields:
                mask |= bit
                status += to_hex(fields[name], 2, name)
        inputs = {**{name: fields[name] for name in DIGITAL_INPUTS if name in fields},
                  **{DOOR_INPUTS[code]: fields[code] for code in DOOR_INPUTS if code in fields}}
        outputs = {name: fields[name] for name in DIGITAL_OUTPUTS if name in fields}
        engine_on = bool(to_int(fields.get("engine_on", 0)))
        if inputs or outputs:
            mask |= DIGITAL_IO
            input_bits = int(engine_on) | sum(to_int(value) << int(name[-1]) for name, value in inputs.items())
            output_bits = sum(to_int(value) << (int(name[-1]) - 1) for name, value in outputs.items())
            status += to_hex(input_bits, 1, "digital inputs") + to_hex(output_bits, 1, "digital outputs")
        moving = float(fields.get("speed", 0)) > 0
        status += to_hex(fields.get("motion_status", MOTION[(engine_on, moving)]), 1, "motion_status")
        status += to_hex(fields.get("gps_satellites", 0), 1, "gps_satellites")
        return mask, status

    @staticmethod
    def one_wire_block(sensors: dict) -> str:
        """<Number> + per sensor: id(8) + device type(1) + data length(1) + temperature(2, signed 1/16 C)."""
        block = to_hex(len(sensors), 1, "one_wire number")
        for sensor_id, raw in sensors.items():
            sensor_hex = str(sensor_id).strip().lower()
            if len(sensor_hex) != 16:
                raise ValueError(f"queclink: one_wire id must be 8 bytes hex, got '{sensor_id}'")
            bytes.fromhex(sensor_hex)
            block += (sensor_hex + ONE_WIRE_TEMPERATURE + ONE_WIRE_DATA_LENGTH
                      + to_hex(raw, 2, "one_wire temperature", signed=True))
        return block

    def next_count(self) -> str:
        self.count = (self.count + 1) & 0xFFFF
        return format(self.count, "04x")

    @staticmethod
    def frame(head: str, body: str) -> str:
        """head + body + checksum (from message type / mask, 4-byte header excluded) + tail."""
        checked = head[HEADER_HEX:] + body
        return head + body + format(crc16_ccitt(checked), "04x") + TAIL
