"""Import a kernel v2 send-scenario file (.txt, cli/send_scenario_device.py format, Teltonika) into a case timeline.

The .txt stays in the kernel repo, e2e keeps only the generated YAML timeline (editable via Excel).
- 1 file row = 1 packet at `second` from 00:00:00. Empty cell = not sent, so the case gets `carry_over: false`.
- `event` login / connected / disconnected -> packet login / connect / disconnect.
- Values in device units like the kernel sender: dallas x10, analog / external voltage x1000, fuel x100.
- Consecutive rows with the same gap whose values are constant or linear become one repeated row
  (from / to / every, `a -> b`). Coordinates may differ up to COORDINATE_TOLERANCE (~2 cm), other values exact.
- A row older than the previous one is buffered data: `sent_at` = latest time so far (file order kept).
- The last packet is step `end`.
"""
import csv
from datetime import timedelta
from pathlib import Path

from harness.excel import dump_yaml
from harness.loader import CASES_DIR
from harness.config import load_yaml
from harness.timeline import format_duration, interpolate

END_STEP = "end"
INTERVAL = 10                          # seconds, case `interval` (rows with this gap have no `every`)
MIN_REPEAT = 3                         # shorter runs stay single rows (easier to read)
ALTITUDE = 10                          # kernel sender default
COORDINATES = {"lng", "lat"}
COORDINATE_TOLERANCE = 2e-7            # degrees
SKIPPED_COLUMNS = {"second", "comment", "event"}
EVENTS = {"login": "login", "connected": "connect", "disconnected": "disconnect"}


def whole(text: str) -> int:
    return int(float(text))


def on_off(text: str) -> bool:
    return bool(whole(text))


# column -> (field, type)
GPS_COLUMNS = {
    "Longitude": ("lng", float), "Latitude": ("lat", float), "Satellites": ("satellites", whole),
    "Speed": ("speed", whole), "acc": ("acc", on_off),
}

# column -> (teltonika io name, multiplier), same as kernel SendScenarioDevice.model_data
IO_COLUMNS = {
    "digitalInput2": ("digital_input_2", 1),
    "digitalInput3": ("digital_input_3", 1),
    "analogInput2": ("analog_input_2", 1000),
    "dallasTemperature1": ("dallas_temperature_1", 10),
    "dallasTemperature2": ("dallas_temperature_2", 10),
    "dallasTemperature3": ("dallas_temperature_3", 10),
    "dallasTemperatureId1": ("dallas_temperature_1_id", 1),
    "ultrasonicFuel": ("ultrasonic_fuel", 100),
    "externalVoltage": ("external_voltage", 1000),
    "gnssStatus": ("gnss", 1),
}


def row_to_raw(row: dict, line: int) -> dict:
    """CSV row -> {packet, fields...} (only the values the device sends)."""
    event = (row.get("event") or "").strip()
    if event:
        if event not in EVENTS:
            raise ValueError(f"line {line}: unknown event '{event}', use {sorted(EVENTS)}")
        return {"packet": EVENTS[event]}

    raw = {"packet": "location"}
    gps = {}
    for column, text in row.items():
        text = (text or "").strip()
        if not text or column in SKIPPED_COLUMNS:
            continue
        if column in GPS_COLUMNS:
            field, cast = GPS_COLUMNS[column]
            gps[field] = cast(text)
        elif column in IO_COLUMNS:
            io_name, multiplier = IO_COLUMNS[column]
            raw[io_name] = round(float(text) * multiplier)
        else:
            raise ValueError(f"line {line}: column '{column}' not supported, add it to IO_COLUMNS")
    # fixed field order: readable columns in YAML / Excel
    ordered = {"packet": "location"}
    for field in ("lat", "lng"):
        if field in gps:
            ordered[field] = gps[field]
    ordered["altitude"] = ALTITUDE
    for field in ("satellites", "speed", "acc"):
        if field in gps:
            ordered[field] = gps[field]
    ordered.update({key: value for key, value in raw.items() if key != "packet"})
    return ordered


def read_scenario(path: Path) -> list[tuple[int, dict]]:
    """[(second, raw)] in file order. Buffered rows get `sent_at` (second)."""
    rows = []
    latest = 0
    with open(path, encoding="utf-8", newline="") as file:
        for line, row in enumerate(csv.DictReader(file), start=2):
            if not any((value or "").strip() for value in row.values() if isinstance(value, str)):
                continue
            second = whole(row["second"])
            raw = row_to_raw(row, line)
            if second < latest:
                raw["sent_at"] = latest
            latest = max(latest, second)
            rows.append((second, raw))
    if not rows:
        raise ValueError(f"scenario file is empty: {path}")
    return rows


def fits(field: str, values: list) -> bool:
    """Constant, or the linear interpolation first -> last of a repeated row."""
    if all(value == values[0] for value in values):
        return True
    if not all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values):
        return False
    count = len(values)
    for position, value in enumerate(values):
        expected = interpolate([values[0], values[-1]], position, count)
        if field in COORDINATES:
            if abs(expected - value) > COORDINATE_TOLERANCE:
                return False
        elif expected != value:
            return False
    return True


def segment_fits(rows: list, first: int, last: int) -> bool:
    raws = [raw for _, raw in rows[first:last + 1]]
    return all(fits(field, [raw[field] for raw in raws]) for field in raws[0] if field != "packet")


def is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def can_join(rows: list, first: int, index: int, gap: int) -> bool:
    """Quick test: row `index` has the same fields and gap, each value changes by the same step as
    between the first two rows (constant = step 0). segment_fits() verifies the exact result."""
    (second, raw), (previous_second, previous) = rows[index], rows[index - 1]
    head, next_row = rows[first][1], rows[first + 1][1]
    if raw["packet"] != "location" or raw.keys() != head.keys() or second - previous_second != gap:
        return False
    for field, value in raw.items():
        if field == "packet":
            continue
        if field == "sent_at" or not is_number(value):
            if value != head[field]:
                return False
            continue
        step = next_row[field] - head[field]
        tolerance = 2 * COORDINATE_TOLERANCE if field in COORDINATES else 0
        if abs((value - previous[field]) - step) > tolerance:
            return False
    return True


def segment_end(rows: list, first: int) -> int:
    """Last row index of the repeated row starting at `first`."""
    if first + 1 >= len(rows) or rows[first][1]["packet"] != "location":
        return first
    gap = rows[first + 1][0] - rows[first][0]
    if gap <= 0 or not can_join(rows, first, first + 1, gap):
        return first
    last = first + 1
    while last + 1 < len(rows) and can_join(rows, first, last + 1, gap):
        last += 1
    while last > first and not segment_fits(rows, first, last):       # quick test drifted: shorten
        last -= 1
    return last if last - first + 1 >= MIN_REPEAT else first


def to_timeline_row(rows: list, first: int, last: int, is_end: bool) -> dict:
    second, head = rows[first]
    row = {"from": format_duration(timedelta(seconds=second))}
    if last > first:
        gap = rows[first + 1][0] - second
        row["to"] = format_duration(timedelta(seconds=rows[last][0] + gap))
        if gap != INTERVAL:
            row["every"] = format_duration(timedelta(seconds=gap))
    if is_end:
        row["step_name"] = END_STEP
    row["packet"] = head["packet"]
    if "sent_at" in head:
        row["sent_at"] = format_duration(timedelta(seconds=head["sent_at"]))
    for field in head:
        if field in ("packet", "sent_at"):
            continue
        values = (head[field], rows[last][1][field])
        row[field] = values[0] if values[0] == values[1] else list(values)
    return row


def to_timeline(rows: list) -> list[dict]:
    timeline = []
    first = 0
    while first < len(rows):
        last = segment_end(rows, first)
        timeline.append(to_timeline_row(rows, first, last, last == len(rows) - 1))
        first = last + 1
    return timeline


def import_scenario(path: Path, usecase: str) -> tuple[Path, int, int]:
    """Write the timeline into cases/<usecase>.yaml (other keys kept). Return (case path, packets, rows)."""
    rows = read_scenario(path)
    timeline = to_timeline(rows)
    target = CASES_DIR / f"{usecase}.yaml"
    case = load_yaml(target) if target.exists() else {"usecase": usecase, "families": ["teltonika"]}
    case = {key: value for key, value in case.items() if key != "timeline"}
    case["interval"] = format_duration(timedelta(seconds=INTERVAL))
    case["carry_over"] = False
    case["timeline"] = timeline
    dump_yaml(case, target, usecase)
    return target, len(rows), len(timeline)
