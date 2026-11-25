"""Base interface for Family Creator: YAML fields -> raw device hex."""
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class IdentifierFormat:
    length: int
    charset: str = "0123456789"


class UnsupportedFieldError(ValueError):
    """Field is not supported by this family."""


WORD_VALUES = {"on": 1, "open": 1, "true": 1, "off": 0, "closed": 0, "false": 0}


def to_int(value) -> int:
    """bool / on-off / open-closed / number -> int."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, str) and value.strip().lower() in WORD_VALUES:
        return WORD_VALUES[value.strip().lower()]
    return int(value)


def to_hex(value, size: int, name: str, signed: bool = False) -> str:
    """int -> big-endian hex of exactly `size` bytes (negative = two's complement when signed)."""
    number = to_int(value)
    bits = size * 8
    low = -(1 << (bits - 1)) if signed else 0
    high = (1 << (bits - 1)) if signed else (1 << bits)
    if not low <= number < high:
        raise ValueError(f"{name}: value {number} does not fit in {size} bytes")
    return format(number & ((1 << bits) - 1), f"0{size * 2}x")


def parse_mac(value, name: str = "mac_address") -> str:
    mac = str(value).lower().replace(":", "")
    if len(mac) != 12:
        raise ValueError(f"{name} must be 6 bytes, got '{value}'")
    bytes.fromhex(mac)
    return mac


class FamilyCreator:
    family: str = ""
    default_model: str = ""          # kernel DeviceModelEnum value
    identifier_format: IdentifierFormat = IdentifierFormat(length=15)

    # sensor code -> kernel pin code used when encoding (also used by setup)
    sensor_pins: dict = {}

    # step fields this creator accepts (others raise UnsupportedFieldError)
    supported_fields: set = set()

    # packets sent automatically before the first step (ex: login for concox)
    initial_packets: tuple = ()

    def build(self, packet: str, fields: dict, at: datetime, imei: str) -> list[str]:
        """Return raw hex packets for one step (can be more than one)."""
        raise NotImplementedError

    def build_batch(self, items: list[tuple[str, dict, datetime]], imei: str) -> str:
        """Buffered data: several steps (send, fields, at) sent in ONE gate message.
        Default: all packets concatenated (kernel splits frames, ex: concox _split_data)."""
        return "".join(packet for send, fields, at in items for packet in self.build(send, fields, at, imei))

    def build_batch_messages(self, items: list[tuple[str, dict, datetime]], imei: str) -> list[str]:
        """Gate messages for buffered data. Default: one message (build_batch).
        Override when the kernel parses only one frame per message (ex: queclink)."""
        return [self.build_batch(items, imei)]
