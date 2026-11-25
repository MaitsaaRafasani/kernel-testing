"""Teltonika creator: fields -> raw hex (Codec 8 / Codec 8 Extended), readable by kernel v2 parser.

Step fields:
- GPS: latitude, longitude, altitude, direction, speed, gps_satellites
- generic: engine_on, gsm_status, vcc_voltage, mileage
- sensor: door1, door2, temperature_1..3, rfid (see SENSOR_IO)
- io:      {io_name: value}  any IO in io_table.IO (value: int, raw hex str, {ascii: str}, beacon MAC list)
           an io name can also be a field itself, ex: digital_input_2: 1
- io_raw:  [{id: int, size: 1|2|4|8|x, value: ...}]  IO not in io_table (ex: manual CAN)
- codec:   "8e" (default) or "8"
- priority, event_io: record header (default 0)
- reply_text: reply text for send command_reply (Codec 12 response)

Values are device-level (raw), no scaling.
"""
from datetime import datetime, timezone

from creators.base import FamilyCreator, IdentifierFormat, UnsupportedFieldError
from creators.teltonika.io_table import EYE_BEACON_IO, IO, X_BYTE

CODEC_8 = "08"
CODEC_8E = "8e"
CODEC_12 = "0c"                 # GPRS command / response
RESPONSE_TYPE = "06"
MAX_RECORDS = 255               # record count is 1 byte
SIZES = (1, 2, 4, 8)

# kernel PinCodes (enums/sensor_enum.py)
PIN_DIGITAL_2 = 2
PIN_DIGITAL_3 = 3
PIN_DALLAS1_WIRE1 = 11
PIN_DALLAS1_WIRE2 = 12
PIN_DALLAS1_WIRE3 = 13

# generic field -> io name
GENERIC_IO = {
    "engine_on": "ignition_id",
    "gsm_status": "gsm_signal_strength",
    "vcc_voltage": "external_voltage",
    "mileage": "total_odometer",
}

# sensor code -> (io name, kernel pin code)
SENSOR_IO = {
    "door1": ("digital_input_2", PIN_DIGITAL_2),
    "door2": ("digital_input_3", PIN_DIGITAL_3),
    "temperature_1": ("dallas_temperature_1", PIN_DALLAS1_WIRE1),
    "temperature_2": ("dallas_temperature_2", PIN_DALLAS1_WIRE2),
    "temperature_3": ("dallas_temperature_3", PIN_DALLAS1_WIRE3),
}

GPS_FIELDS = ("latitude", "longitude", "altitude", "direction", "speed", "gps_satellites")
OPTION_FIELDS = ("io", "io_raw", "codec", "priority", "event_io", "reply_text")

DEFAULT_GSM_SIGNAL = 5      # GsmLevelTeltonika.STRONG_SIGNAL
BEACON_RSSI = {"eye_beacon_visible": "ba", "eye_beacon_found": "ba", "eye_beacon_lost": "bc"}
BEACON_ID_PARAM = "0110b0000000000000000f75000000000f75"
BEACON_BATTERY_PARAM = "0d0100"
BEACON_VOLTAGE_PARAM = "0e020c01"


def crc16_arc(data_hex: str) -> int:
    """CRC-16/ARC (IBM), same as kernel helper compute16."""
    crc = 0
    for byte in bytes.fromhex(data_hex):
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


WORD_VALUES = {"on": 1, "open": 1, "true": 1, "off": 0, "closed": 0, "false": 0}


def to_int(value) -> int:
    """bool / on-off / open-closed / number -> int."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, str) and value.strip().lower() in WORD_VALUES:
        return WORD_VALUES[value.strip().lower()]
    return int(value)


def encode_fixed(value, size: int, name: str) -> str:
    """int (negative = two's complement) or raw hex str -> hex of exactly `size` bytes."""
    if isinstance(value, str) and value.strip().lower() not in WORD_VALUES:
        raw = value.strip().lower()
        if len(raw) != size * 2:
            raise ValueError(f"{name}: raw hex must be {size} bytes, got '{value}'")
        bytes.fromhex(raw)
        return raw
    number = to_int(value)
    bits = size * 8
    if not -(1 << (bits - 1)) <= number < (1 << bits):
        raise ValueError(f"{name}: value {number} does not fit in {size} bytes")
    return format(number & ((1 << bits) - 1), f"0{size * 2}x")


def encode_beacons(name: str, macs: list) -> str:
    """Eye beacon X-byte value (same layout as kernel v2 TeltonikaCreator)."""
    rssi = "0001" + BEACON_RSSI[name]
    content = ""
    for mac in macs:
        mac = str(mac).lower()
        mac_param = "0f" + format(len(mac) // 2, "02x") + mac
        body = rssi + BEACON_ID_PARAM + mac_param + BEACON_BATTERY_PARAM + BEACON_VOLTAGE_PARAM
        content += format(len(body) // 2, "02x") + body
    return "01" + content


def encode_x(value, name: str) -> str:
    """X-byte value: raw hex str, {ascii: str}, or MAC list for eye beacon."""
    if name in EYE_BEACON_IO and isinstance(value, list):
        return encode_beacons(name, value)
    if isinstance(value, dict) and "ascii" in value:
        return str(value["ascii"]).encode().hex()
    if isinstance(value, str):
        raw = value.strip().lower()
        bytes.fromhex(raw)
        return raw
    raise ValueError(f"{name}: X-byte value must be hex str, {{ascii: ...}} or beacon MAC list")


class TeltonikaCreator(FamilyCreator):
    family = "teltonika"
    default_model = "fmb130"
    initial_packets = ("login",)        # IMEI handshake, kernel parser turns it into LoginPacket
    identifier_format = IdentifierFormat(length=15)
    sensor_pins = {code: pin for code, (_, pin) in SENSOR_IO.items()}
    # io names (io_table.IO) are also accepted as fields, ex: digital_input_2: 1 (same as io: {digital_input_2: 1})
    supported_fields = (
        set(GPS_FIELDS) | set(GENERIC_IO) | set(SENSOR_IO) | {"rfid"} | set(OPTION_FIELDS) | set(IO)
    )

    def build(self, packet: str, fields: dict, at: datetime, imei: str) -> list[str]:
        if packet == "login":
            return [self.login(imei)]
        if packet in ("location", "sensor"):
            return [self.avl_packet([(fields, at)], fields.get("codec", CODEC_8E))]
        if packet == "command_reply":
            return [self.command_reply(fields)]
        raise UnsupportedFieldError(f"teltonika: send '{packet}' not supported, use location + io")

    def command_reply(self, fields: dict) -> str:
        """Codec 12 response (type 06). Kernel: size = message[22:30], content = message[30:30 + size * 2]."""
        if "reply_text" not in fields:
            raise ValueError("teltonika: send command_reply needs field 'reply_text'")
        text = str(fields["reply_text"]).encode().hex()
        data = CODEC_12 + "01" + RESPONSE_TYPE + format(len(text) // 2, "08x") + text + "01"
        return "00000000" + format(len(data) // 2, "08x") + data + format(crc16_arc(data), "08x")

    def build_batch(self, items: list[tuple[str, dict, datetime]], imei: str) -> str:
        """Buffered data: one AVL packet with many records (in the given order)."""
        if any(send not in ("location", "sensor") for send, _, _ in items):
            raise UnsupportedFieldError("teltonika: batch only supports send location / sensor")
        if len(items) > MAX_RECORDS:
            raise ValueError(f"teltonika: batch has {len(items)} records, max {MAX_RECORDS} per AVL packet")
        codec = items[0][1].get("codec", CODEC_8E)
        return self.avl_packet([(fields, at) for _, fields, at in items], codec)

    def login(self, imei: str) -> str:
        """IMEI handshake: 2-byte length + ASCII IMEI."""
        return format(len(imei), "04x") + imei.encode().hex()

    def avl_packet(self, records: list[tuple[dict, datetime]], codec: str = CODEC_8E) -> str:
        codec = str(codec).lower().zfill(2)
        if codec not in (CODEC_8, CODEC_8E):
            raise UnsupportedFieldError(f"teltonika: codec '{codec}' not supported")
        count = format(len(records), "02x")
        body = "".join(self.avl_record(fields, at, codec) for fields, at in records)
        data = codec + count + body + count
        return "00000000" + format(len(data) // 2, "08x") + data + format(crc16_arc(data), "08x")

    def avl_record(self, fields: dict, at: datetime, codec: str) -> str:
        unknown = set(fields) - self.supported_fields
        if unknown:
            raise UnsupportedFieldError(f"teltonika: field not supported: {sorted(unknown)}")
        timestamp = int(at.astimezone(timezone.utc).timestamp() * 1000)
        priority = encode_fixed(fields.get("priority", 0), 1, "priority")
        return format(timestamp, "016x") + priority + self.gps_element(fields) + self.io_element(fields, codec)

    def gps_element(self, fields: dict) -> str:
        return (
            encode_fixed(round(float(fields.get("longitude", 0)) * 10_000_000), 4, "longitude")
            + encode_fixed(round(float(fields.get("latitude", 0)) * 10_000_000), 4, "latitude")
            + encode_fixed(int(fields.get("altitude", 0)), 2, "altitude")
            + encode_fixed(int(fields.get("direction", 0)), 2, "direction")
            + encode_fixed(int(fields.get("gps_satellites", 0)), 1, "gps_satellites")
            + encode_fixed(int(fields.get("speed", 0)), 2, "speed")
        )

    def collect_io(self, fields: dict) -> list[tuple[int, int | str, object, str]]:
        """Return [(io id, size, value, name)] in write order."""
        items: dict[str, tuple[int, int | str, object]] = {}

        def add(name: str, value):
            if name not in IO:
                raise UnsupportedFieldError(f"teltonika: unknown io '{name}'")
            io_id, size = IO[name]
            items[name] = (io_id, size, value)

        for field, io_name in GENERIC_IO.items():
            if field in fields:
                add(io_name, fields[field])
        for code, (io_name, _) in SENSOR_IO.items():
            if code in fields:
                add(io_name, fields[code])
        if "rfid" in fields:
            add("i_button", self.rfid_value(fields["rfid"]))
        for name, value in fields.items():
            if name in IO:
                add(name, value)
        for io_name, value in (fields.get("io") or {}).items():
            add(io_name, value)
        if "gsm_signal_strength" not in items:
            add("gsm_signal_strength", DEFAULT_GSM_SIGNAL)

        result = [(io_id, size, value, name) for name, (io_id, size, value) in items.items()]
        for raw in fields.get("io_raw") or []:
            size = raw["size"] if raw["size"] == X_BYTE else int(raw["size"])
            result.append((int(raw["id"]), size, raw["value"], f"io_raw {raw['id']}"))
        return result

    def io_element(self, fields: dict, codec: str) -> str:
        id_len = 4 if codec == CODEC_8E else 2       # hex chars for io id and counters
        items = self.collect_io(fields)
        if codec == CODEC_8 and any(item[1] == X_BYTE for item in items):
            raise UnsupportedFieldError("teltonika: X-byte io needs codec 8e")

        sections = []
        for size in SIZES + ((X_BYTE,) if codec == CODEC_8E else ()):
            entries = [item for item in items if item[1] == size]
            hex_entries = ""
            for io_id, _, value, name in entries:
                if io_id >= 1 << (id_len * 4):
                    raise UnsupportedFieldError(f"teltonika: io '{name}' id {io_id:#x} needs codec 8e")
                if size == X_BYTE:
                    value_hex = encode_x(value, name)
                    hex_entries += format(io_id, f"0{id_len}x") + format(len(value_hex) // 2, "04x") + value_hex
                else:
                    hex_entries += format(io_id, f"0{id_len}x") + encode_fixed(value, size, name)
            sections.append(format(len(entries), f"0{id_len}x") + hex_entries)

        event_io = fields.get("event_io", 0)
        event_id = IO[event_io][0] if isinstance(event_io, str) else int(event_io)
        return format(event_id, f"0{id_len}x") + format(len(items), f"0{id_len}x") + "".join(sections)

    @staticmethod
    def rfid_value(rfid) -> str:
        """iButton: 8 hex bytes, written byte-reversed (kernel parser reverses it back)."""
        text = str(rfid).strip().lower()
        if len(text) != 16:
            raise ValueError(f"rfid must be 16 hex characters, got '{rfid}'")
        bytes.fromhex(text)
        return "".join(reversed([text[i:i + 2] for i in range(0, 16, 2)]))
