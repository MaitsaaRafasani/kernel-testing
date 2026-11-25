"""Expand a scenario into a time-ordered list of steps.

Scenario `timeline` = list of rows, same as the Excel rows (legacy: `steps` then `segments`).
Result is sorted by time; at the same time the row order is kept.
- row without `to`: one packet. Time: `from` (HH:MM:SS from start), `at` (absolute / now-relative),
            `after` (gap from previous row) or `interval` (default gap).
- row with `to` (segment): one packet every `every` (default `interval`) from `from` (inclusive)
            to `to` (exclusive). Value `[a, b]` is interpolated from a (first packet) to b (last packet).
            `step_name` = name of the last packet, `first_step_name` = name of the first packet.
- batch:    consecutive steps with the same `batch` value are sent in ONE gate message
            (buffered data: device sends stored records at once). Works on steps and segments.
- sent_at:  send moment (offset from start) when it differs from the packet time, ex: buffered records
            with old packet time sent after reconnect. Send order = sent_at (else packet time).
- carry_over: false (case setting): each row is sent as written, no defaults and no carry-over,
            so an empty cell = the device does not send that value (ex: data imported from a kernel scenario).

Timing is computed for all steps first, then `families:` filters per family,
so a step skipped for one family never shifts the time of the others.
Then aliases, defaults and carry-over are applied in time order.
"""
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

ALIASES = {"lat": "latitude", "lng": "longitude", "acc": "engine_on", "satellites": "gps_satellites"}

# step control keys, never sent to the creator
STEP_KEYS = {
    "step_name", "first_step_name", "packet", "at", "from", "to", "every", "offset", "after", "wait", "duplicate",
    "corrupt", "families", "batch", "sent_at",
}
SEGMENT_KEYS = {"from", "to", "every", "step_name", "first_step_name", "packet", "families"}

# per-step options, not carried to the next step
NO_CARRY = {
    "protocol", "serial", "alarm", "terminal_info", "language", "modules", "fence_id",
    "event_io", "priority", "io_raw", "event_id", "count", "jamming_net",
    # gate message fields (connect / disconnect / xmit)
    "ip_gate", "port_gate", "gate_start_time", "last_connect", "command", "message", "ip", "port",
    "reply_text",                     # command_reply
}

DEFAULT_INTERVAL = "00:00:30"
COORDINATE_DECIMALS = 7
DURATION_UNITS = {"ms": "milliseconds", "s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
DURATION_RE = re.compile(r"^(\d+)(ms|s|m|h|d)$")
CLOCK_RE = re.compile(r"^(\d+):([0-5]\d):([0-5]\d)$")          # HH:MM:SS, 00:00:00 = scenario start
RELATIVE_RE = re.compile(r"^now(?:([+-])(.+))?$")
TODAY_RE = re.compile(r"^today(?:([+-])(\d+)d)?$")             # 00:00 local time, ex: today-2d


@dataclass
class Step:
    index: int
    packet: str
    at: datetime
    fields: dict
    id: str | None = None
    wait: timedelta | None = None
    duplicate: bool = False
    batch: str | None = None          # same batch = sent together in one gate message (buffered data)
    send_at: datetime | None = None   # send moment (buffered data: later than `at`)


def parse_duration(value) -> timedelta:
    """'HH:MM:SS' (ex: 00:10:00), '30s' / '5m' / '1h', or a number of seconds."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return timedelta(seconds=value)
    text = str(value).strip()
    clock = CLOCK_RE.match(text)
    if clock:
        hours, minutes, seconds = (int(part) for part in clock.groups())
        return timedelta(hours=hours, minutes=minutes, seconds=seconds)
    match = DURATION_RE.match(text)
    if not match:
        raise ValueError(f"invalid time '{value}', use HH:MM:SS like 00:10:00")
    return timedelta(**{DURATION_UNITS[match.group(2)]: int(match.group(1))})


def format_duration(delta: timedelta) -> str:
    """timedelta -> 'HH:MM:SS'."""
    seconds = int(delta.total_seconds())
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def parse_time(value, now: datetime, tz: timezone = timezone.utc) -> datetime:
    """'now', 'now-00:20:00' / 'now-20m', 'now+00:00:05', ISO string or datetime (naive = UTC).
    'today' / 'today-2d' / 'today+1d' = 00:00 in timezone `tz` (device time), today or N days before / after."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    match = RELATIVE_RE.match(text)
    if match:
        if not match.group(1):
            return now
        delta = parse_duration(match.group(2))
        return now + delta if match.group(1) == "+" else now - delta
    today = TODAY_RE.match(text)
    if today:
        midnight = now.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)
        days = int(today.group(2) or 0)
        return midnight + timedelta(days=days if today.group(1) == "+" else -days)
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def initial_fields(config: dict, family: str, supported_fields: set) -> dict:
    """Start values from env.yaml: field_defaults (if the family supports the field) + family_defaults."""
    common = {
        name: value for name, value in (config.get("field_defaults") or {}).items()
        if name in supported_fields
    }
    return {**common, **((config.get("family_defaults") or {}).get(family) or {})}


def normalize(raw: dict) -> dict:
    return {ALIASES.get(key, key): value for key, value in raw.items() if key not in STEP_KEYS}


def for_family(item: dict, family: str | None) -> bool:
    return family is None or not item.get("families") or family in item["families"]


def scenario_items(scenario: dict) -> list[dict]:
    """Timeline rows in order: `timeline` (Excel row order), or legacy `steps` then `segments`.
    Legacy key `send` is read as `packet`."""
    if "timeline" in scenario:
        items = scenario["timeline"] or []
    else:
        items = [*(scenario.get("steps") or []), *(scenario.get("segments") or [])]
    return [
        {("packet" if key == "send" else key): value for key, value in item.items()} if "send" in item else item
        for item in items
    ]


def is_segment(item: dict) -> bool:
    return "to" in item or "every" in item


def row_times(items: list[dict], start: datetime, interval: timedelta, now: datetime) -> list:
    """Packet time per single row (None for segment rows), in row order."""
    result = []
    at = start
    first = True
    for row, item in enumerate(items, start=1):
        if "packet" not in item:
            raise ValueError(f"row {row}: missing 'packet'")
        if is_segment(item):
            result.append(None)
            continue
        if "corrupt" in item:
            raise NotImplementedError(f"row {row}: 'corrupt' not supported yet")
        if "at" in item:
            at = parse_time(item["at"], now)
        elif "from" in item or "offset" in item:          # from scenario start
            at = start + parse_duration(item.get("from", item.get("offset")))
        elif "after" in item:                             # gap from the previous row
            at = at + parse_duration(item["after"])
        elif not first:
            at = at + interval
        first = False
        result.append(at)
    return result


def interpolate(value, position: int, count: int):
    if isinstance(value, list) and len(value) == 2 and all(isinstance(v, (int, float)) for v in value):
        first, last = value
        ratio = position / (count - 1) if count > 1 else 0
        return round(first + (last - first) * ratio, COORDINATE_DECIMALS)
    return value


def expand_segment(segment: dict, start: datetime, interval: timedelta) -> list[tuple]:
    """(time, raw step) for one periodic segment. No `every` = scenario interval."""
    begin, end = parse_duration(segment["from"]), parse_duration(segment["to"])
    every = parse_duration(segment["every"]) if "every" in segment else interval
    count = math.ceil((end - begin) / every) if every.total_seconds() > 0 else 0   # all from + k*every < to
    if "packet" not in segment or count <= 0 or every.total_seconds() <= 0:
        raise ValueError(f"invalid segment {segment}: needs packet, every > 0 and to > from")

    values = {key: value for key, value in segment.items() if key not in SEGMENT_KEYS}
    result = []
    for position in range(count):
        raw = {key: interpolate(value, position, count) for key, value in values.items()}
        raw["packet"] = segment["packet"]
        if segment.get("families"):
            raw["families"] = segment["families"]
        if position == 0 and segment.get("first_step_name"):
            raw["step_name"] = segment["first_step_name"]
        if position == count - 1 and segment.get("step_name"):
            raw["step_name"] = segment["step_name"]
        result.append((start + begin + every * position, raw))
    return result


def expand_steps(
    scenario: dict, defaults: dict, now: datetime | None = None, timing: dict | None = None,
    family: str | None = None, tz: timezone = timezone.utc,
) -> list[Step]:
    """timing: env.yaml `scenario` (default start / interval when the case has none).
    family: skip steps / segments whose `families:` list does not include it.
    tz: device timezone for `today-Nd` times."""
    now = now or datetime.now(timezone.utc)
    timing = timing or {}
    start = parse_time(scenario.get("start", timing.get("start", "now")), now, tz)
    interval = parse_duration(scenario.get("interval", timing.get("interval", DEFAULT_INTERVAL)))

    entries = []                                  # (send moment, packet time, raw) in row order
    items = scenario_items(scenario)
    for item, at in zip(items, row_times(items, start, interval, now)):
        for packet_at, raw in expand_segment(item, start, interval) if at is None else [(at, item)]:
            send = start + parse_duration(raw["sent_at"]) if "sent_at" in raw else packet_at
            entries.append((send, packet_at, raw))
    entries.sort(key=lambda entry: entry[0])      # stable sort: same moment keeps the row order

    carry_over = scenario.get("carry_over", True)
    steps = []
    state = dict(defaults)
    for send, at, raw in entries:
        if not for_family(raw, family):
            continue
        if not carry_over:                        # row as written: empty cell = not sent
            fields = normalize(raw)
        else:
            values = normalize(raw)
            carried = {key: value for key, value in state.items() if key not in NO_CARRY}
            if "io" in values and "io" in carried:
                values["io"] = {**carried["io"], **values["io"]}
            state = {**carried, **values}
            fields = dict(state)
        steps.append(Step(
            index=len(steps) + 1,
            packet=raw["packet"],
            at=at,
            fields=fields,
            id=raw.get("step_name"),
            wait=parse_duration(raw["wait"]) if "wait" in raw else None,
            duplicate=bool(raw.get("duplicate", False)),
            batch=str(raw["batch"]) if raw.get("batch") is not None else None,
            send_at=send,
        ))
    return steps
