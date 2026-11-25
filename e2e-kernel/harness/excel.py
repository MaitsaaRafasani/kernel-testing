"""Convert cases/excel/<usecase>.xlsx <-> YAML. 1 Excel = 1 usecase = 1 timeline + its expected.

Workbook (file name = usecase):
- `notes`        (optional) usecase description, 1 line per row in column A
- `db_requirements` DB conditions to prepare before the run, 1 requirement per row in column A
- `settings`     key | value: families, start, interval, tags, imei, models
                   families / tags: "concox, teltonika"; imei / models: "concox=358..., teltonika=..."
- `setup`        (optional) kind | <item fields...>   1 row = 1 item (ex: kind sensors, code door1)
- `timeline`     from | to | every | step_name | first_step_name | send | families | batch | sent_at | <fields...>
                   time columns     = HH:MM:SS from timeline start (00:00:00), like a scheduler
                   row without `to` = 1 packet at `from`
                   row with `to`    = repeated packet every `every` (empty = interval) from `from` to `to` (excl.)
                   same time        = row order
                   "a -> b"         = interpolated along the repeated rows, "{..}"/"[..]" = inline YAML
- `expected_<v>` step_name | family | table | where | column | expected | note   (1 row = 1 check, ex: expected_v2)
                   family    empty = all families, filled = only for that family (override)
                   where     "vehicle_id = $vehicle_id; imei = $imei", "sent_on between $at.a and $at.b"
                   column    DB column, `count` = number of rows matching `where`, `timeout` = max wait (HH:MM:SS)
                   expected  value / $variable, "~ x" approx, "~ x ± t" approx with tolerance

YAML is the source in git: cases/<usecase>.yaml and versions/<v>/expected/<usecase>.yaml.
Excel is only a temporary edit tool (gitignored): yaml2excel -> edit -> excel2yaml -> delete xlsx.

YAML layout, the same for every writer (excel2yaml, generate, import-scenario, format), see yaml_text():
- case keys in CASE_KEYS order, db_requirements 1 item per line
- 1 timeline row = 1 line: control columns (TIMELINE_COLUMNS), then ROW_FIELDS, then other fields; acc true / false
- 1 expected check = 1 line, keys in CHECK_KEYS order
- header: Excel edit hint, or the generator to change when generators/<usecase>.py exists
"""
import re
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import yaml
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font

from harness.config import ROOT, YAML_LOADER, load_yaml
from harness.loader import as_scenarios
from harness.timeline import (
    DEFAULT_INTERVAL, format_duration, is_segment, parse_duration, parse_time, row_times, scenario_items,
)

CASES_DIR = ROOT / "cases"
EXCEL_DIR = CASES_DIR / "excel"
VERSIONS_DIR = ROOT / "versions"
GENERATORS_DIR = ROOT / "generators"

SETTING_KEYS = ["families", "start", "interval", "tags", "imei", "models", "carry_over", "realtime", "received_on"]
TIMELINE_COLUMNS = [
    "from", "to", "every", "step_name", "first_step_name", "packet", "families", "batch", "sent_at",
]
CASE_KEYS = ["usecase", "description", "db_requirements", *SETTING_KEYS, "setup", "timeline"]
ROW_FIELDS = ["lat", "lng", "altitude", "satellites", "speed", "direction", "acc"]   # after TIMELINE_COLUMNS
CHECK_KEYS = ["table", "where", "count", "match", "timeout", "known_bug", "note"]
ACC_WORDS = {"on": True, "off": False}
EXPECTED_COLUMNS = ["step_name", "family", "table", "where", "column", "expected", "note"]
EXPECTED_SHEET = "expected_"
DEFAULT_FAMILY = "default"
COUNT_COLUMN = "count"
TIMEOUT_COLUMN = "timeout"                                        # max wait for this check, HH:MM:SS
TIME_COLUMNS = {"from", "to", "every", "sent_at", "interval"}     # HH:MM:SS, 00:00:00 = timeline start
TEXT_FORMAT = "@"                                                 # keep 00:10:00 as text, Excel won't convert it
YAML_WIDTH = 4096                                                 # no line wrap: 1 timeline row = 1 line
LIST_KEYS = {"families", "tags"}
MAP_KEYS = {"imei", "models"}
ARROW = "->"
APPROX_RE = re.compile(r"^[~≈]\s*(-?[\d.]+)(?:\s*(?:±|\+-|\+/-)\s*([\d.]+))?$")
BETWEEN_RE = re.compile(r"^(\w+)\s+between\s+(.+?)\s+and\s+(.+)$", re.IGNORECASE)
EDIT_HINT = (
    "# Edit lewat Excel: python cli.py yaml2excel --usecase {usecase} -> edit cases/excel/{usecase}.xlsx\n"
    "#                   -> python cli.py excel2yaml --usecase {usecase} -> hapus xlsx (tidak masuk git)\n\n"
)
GENERATED_HINT = (
    "# Dibuat oleh generators/{usecase}.py: ubah generatornya lalu python cli.py generate --usecase {usecase}\n"
    "# (bukan edit YAML / Excel, generate ulang menimpa file ini)\n\n"
)
CASE_TITLE = "# Test case {usecase}: timeline (sheet notes/db_requirements/settings/setup/timeline).\n"
EXPECTED_TITLE = "# Expected {version} usecase {usecase} (sheet expected_{version}).\n"
REQUIREMENTS_SHEET = "db_requirements"


# ---------- cell values ----------

def number(text: str):
    value = float(text)
    return int(value) if value.is_integer() else value


def scalar(text: str):
    """'12' -> 12, '1.5' -> 1.5, other text unchanged."""
    try:
        return number(text)
    except ValueError:
        return text


def from_cell(value, parse_text: bool = True):
    """Excel cell -> YAML value. parse_text: inline YAML {..}/[..] and numeric "a -> b" (timeline / setup)."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, time):                    # Excel turned 00:10:00 into a time cell
        return format_duration(timedelta(hours=value.hour, minutes=value.minute, seconds=value.second))
    if isinstance(value, timedelta):
        return format_duration(value)
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not parse_text:
        return text
    if text[0] in "{[":
        return yaml.safe_load(text)
    if ARROW in text:
        first, last = (scalar(part.strip()) for part in text.split(ARROW, 1))
        if all(isinstance(side, (int, float)) for side in (first, last)):
            return [first, last]                   # interpolated, ex: 106.8167 -> 106.8393
    return text


def to_cell(value, segment: bool = False):
    """YAML value -> Excel cell."""
    if segment and isinstance(value, list) and len(value) == 2 and all(isinstance(v, (int, float)) for v in value):
        return f"{value[0]} {ARROW} {value[1]}"
    if isinstance(value, (dict, list)):
        return yaml.safe_dump(value, default_flow_style=True, sort_keys=False).strip()
    return value


def split_list(value) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def split_map(value) -> dict:
    return {key.strip(): val.strip() for key, val in (item.split("=", 1) for item in split_list(value))}


def join_value(key: str, value):
    if key in LIST_KEYS and isinstance(value, list):
        return ", ".join(value)
    if key in MAP_KEYS and isinstance(value, dict):
        return ", ".join(f"{k}={v}" for k, v in value.items())
    return value


def clock(row: dict) -> dict:
    """Time columns as HH:MM:SS."""
    return {key: format_duration(parse_duration(value)) if key in TIME_COLUMNS else value
            for key, value in row.items()}


# ---------- read sheets ----------

def read_rows(sheet, parse_text: bool = True) -> list[tuple[int, dict]]:
    """(excel line, row dict) for non-empty rows."""
    rows = list(sheet.iter_rows(values_only=True))
    if not rows:
        return []
    header = [str(cell).strip() if cell is not None else None for cell in rows[0]]
    result = []
    for line, row in enumerate(rows[1:], start=2):
        item = {}
        for key, cell in zip(header, row):
            value = from_cell(cell, parse_text)
            if key and value is not None:
                item[key] = value
        if item:
            result.append((line, item))
    return result


def timeline_item(row: dict, line: int) -> dict:
    """Validate one timeline row (kept as is, row order = YAML `timeline` order)."""
    if "packet" not in row:
        raise ValueError(f"sheet 'timeline' baris {line}: kolom 'packet' kosong")
    if "families" in row:
        row["families"] = split_list(row["families"])
    if is_segment(row):
        for key in ("from", "to"):
            if key not in row:
                raise ValueError(f"sheet 'timeline' baris {line}: baris berulang butuh kolom '{key}'")
    elif "first_step_name" in row:
        raise ValueError(f"sheet 'timeline' baris {line}: 'first_step_name' hanya untuk baris berulang (isi 'to')")
    return row


def parse_where(text) -> dict:
    """'a = 1; b between $x and $y' -> {a: 1, b: {between: [$x, $y]}}."""
    where = {}
    for part in (p.strip() for p in str(text).split(";") if p.strip()):
        between = BETWEEN_RE.match(part)
        if between:
            where[between.group(1)] = {"between": [scalar(between.group(2)), scalar(between.group(3))]}
            continue
        if "=" not in part:
            raise ValueError(f"where '{part}': pakai format 'kolom = nilai' atau 'kolom between a and b'")
        key, value = (side.strip() for side in part.split("=", 1))
        where[key] = scalar(value)
    return where


def format_where(where: dict) -> str:
    parts = []
    for key, value in where.items():
        if isinstance(value, dict) and "between" in value:
            first, last = value["between"]
            parts.append(f"{key} between {first} and {last}")
        else:
            parts.append(f"{key} = {value}")
    return "; ".join(parts)


def parse_expected_value(value):
    if isinstance(value, str):
        approx = APPROX_RE.match(value.strip())
        if approx:
            result = {"approx": number(approx.group(1))}
            if approx.group(2):
                result["tolerance"] = number(approx.group(2))
            return result
    return value


def format_expected_value(value):
    if isinstance(value, dict) and "approx" in value:
        text = f"~ {value['approx']}"
        return f"{text} ± {value['tolerance']}" if "tolerance" in value else text
    return to_cell(value)


def read_expected(sheet, usecase: str) -> dict:
    """Sheet expected_<v> -> {usecase: {default|family: {steps: {step: {db: [check]}}}}}."""
    blocks: dict = {}
    for line, row in read_rows(sheet, parse_text=False):
        for key in ("step_name", "table", "column"):
            if key not in row:
                raise ValueError(f"sheet '{sheet.title}' baris {line}: kolom '{key}' kosong")
        family = str(row.get("family") or DEFAULT_FAMILY)
        where = parse_where(row.get("where", ""))
        checks = blocks.setdefault(family, {}).setdefault("steps", {}).setdefault(str(row["step_name"]), {})
        db = checks.setdefault("db", [])
        check = next((c for c in db if c["table"] == row["table"] and c["where"] == where), None)
        if check is None:
            check = {"table": row["table"], "where": where}
            db.append(check)
        value = parse_expected_value(row.get("expected"))
        if row["column"] == COUNT_COLUMN:
            check["count"] = value
        elif row["column"] == TIMEOUT_COLUMN:
            check["timeout"] = value
        else:
            check.setdefault("match", {})[row["column"]] = value
        if row.get("note"):
            check["note"] = f"{check['note']} | {row['note']}" if check.get("note") else row["note"]
    return {usecase: blocks}


def excel_to_case(path: Path) -> dict:
    workbook = load_workbook(path, data_only=True)        # data_only: formula -> computed value
    for sheet in ("settings", "timeline"):
        if sheet not in workbook.sheetnames:
            raise ValueError(f"{path.name}: sheet '{sheet}' tidak ada")

    case = {"usecase": path.stem}
    if "notes" in workbook.sheetnames:
        lines = [row[0] if row[0] is not None else "" for row in workbook["notes"].iter_rows(values_only=True)]
        case["description"] = "\n".join(lines).strip() + "\n"
    if REQUIREMENTS_SHEET in workbook.sheetnames:
        case["db_requirements"] = [
            str(row[0]).strip() for row in workbook[REQUIREMENTS_SHEET].iter_rows(values_only=True)
            if row[0] is not None and str(row[0]).strip()
        ]

    for line, row in read_rows(workbook["settings"], parse_text=False):
        key, value = row.get("key"), row.get("value")
        if key not in SETTING_KEYS:
            raise ValueError(f"sheet 'settings' baris {line}: key '{key}' tidak dikenal, pilih {SETTING_KEYS}")
        if key in LIST_KEYS:
            value = split_list(value)
        elif key in MAP_KEYS:
            value = split_map(value)
        case[key] = value

    if "setup" in workbook.sheetnames:
        setup: dict = {}
        for _, row in read_rows(workbook["setup"]):
            kind = row.pop("kind")
            setup.setdefault(kind, []).append(row)
        if setup:
            case["setup"] = setup

    case["timeline"] = [timeline_item(row, line) for line, row in read_rows(workbook["timeline"])]
    return case


def excel_to_expected(path: Path) -> dict:
    """{version: expected} from sheets expected_<version>."""
    workbook = load_workbook(path, data_only=True)
    return {
        name[len(EXPECTED_SHEET):]: read_expected(workbook[name], path.stem)
        for name in workbook.sheetnames if name.startswith(EXPECTED_SHEET)
    }


class CaseDumper(getattr(yaml, "CSafeDumper", yaml.SafeDumper)):     # libyaml when installed, same text
    """Multi-line strings as literal block (|), readable description in YAML."""


CaseDumper.add_representer(
    str,
    lambda dumper, value: dumper.represent_scalar(
        "tag:yaml.org,2002:str", value, style="|" if "\n" in value else None),
)


class TimelineRow(dict):
    """Timeline row / expected check, dumped on one line (1 YAML line = 1 Excel row)."""


CaseDumper.add_representer(
    TimelineRow,
    lambda dumper, value: dumper.represent_mapping("tag:yaml.org,2002:map", value, flow_style=True),
)


class BlockList(list):
    """List dumped 1 item per line (db_requirements: easy to read)."""


CaseDumper.add_representer(
    BlockList,
    lambda dumper, value: dumper.represent_sequence("tag:yaml.org,2002:seq", value, flow_style=False),
)


def ordered(item: dict, keys: list[str]) -> dict:
    """Known keys first in `keys` order, then the other keys as they are."""
    return {**{key: item[key] for key in keys if key in item}, **item}


def timeline_line(row: dict) -> TimelineRow:
    """1 timeline row = 1 line, standard column order, acc on / off written as true / false."""
    row = ordered(row, TIMELINE_COLUMNS + ROW_FIELDS)
    acc = row.get("acc")
    if isinstance(acc, str) and acc.strip().lower() in ACC_WORDS:
        row["acc"] = ACC_WORDS[acc.strip().lower()]
    return TimelineRow(row)


def one_line_checks(expected: dict) -> dict:
    """{usecase: {block: {steps: {step: {db: [check]}}}}} with each check on 1 line, keys in CHECK_KEYS order."""
    return {
        usecase: {
            block: {
                **(content or {}),
                "steps": {
                    step: {**checks, "db": [TimelineRow(ordered(c, CHECK_KEYS)) for c in checks.get("db") or []]}
                    for step, checks in ((content or {}).get("steps") or {}).items()
                },
            }
            for block, content in (blocks or {}).items()
        }
        for usecase, blocks in expected.items()
    }


def is_generated(usecase: str) -> bool:
    return (GENERATORS_DIR / f"{usecase}.py").exists()


def header(usecase: str, version: str | None = None) -> str:
    """Case header (version None) or expected header, with how to edit the file."""
    title = CASE_TITLE if version is None else EXPECTED_TITLE
    hint = GENERATED_HINT if is_generated(usecase) else EDIT_HINT
    return (title + hint).format(usecase=usecase, version=version)


def yaml_text(data: dict, usecase: str, version: str | None = None) -> str:
    """Case (version None) or expected dict -> file text in the standard layout."""
    if version is None:
        data = ordered(data, CASE_KEYS)
        if isinstance(data.get("db_requirements"), list):
            data["db_requirements"] = BlockList(data["db_requirements"])
        if isinstance(data.get("timeline"), list):
            data["timeline"] = [timeline_line(row) for row in data["timeline"]]
    else:
        data = one_line_checks(data)
    return header(usecase, version) + yaml.dump(
        data, Dumper=CaseDumper, sort_keys=False, allow_unicode=True, default_flow_style=None, width=YAML_WIDTH,
    )


def dump_yaml(data: dict, path: Path, usecase: str, version: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml_text(data, usecase, version), encoding="utf-8")


# ---------- write sheets ----------

def write_sheet(sheet, columns: list[str], rows: list[dict], arrow_rows: set | None = None) -> None:
    sheet.append(columns)
    for cell in sheet[1]:
        cell.font = Font(bold=True)
    for index, row in enumerate(rows):
        arrow = bool(arrow_rows and index in arrow_rows)
        sheet.append([to_cell(join_value(column, row.get(column)), arrow) for column in columns])
    # time columns as text, also for empty rows below, so typed 00:10:00 stays text
    for index, column in enumerate(columns, start=1):
        if column in TIME_COLUMNS:
            for (cell,) in sheet.iter_rows(min_row=2, max_row=len(rows) + 200, min_col=index, max_col=index):
                cell.number_format = TEXT_FORMAT
    sheet.freeze_panes = "A2"


def ordered_columns(base: list[str], rows: list[dict]) -> list[str]:
    columns = list(base)
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    return columns


def timeline_rows(scenario: dict) -> list[dict]:
    """Excel rows in timeline order. Single rows get an explicit `from` (resolves at / after / interval)."""
    now = datetime.now(timezone.utc)
    start = parse_time(scenario.get("start", "now"), now)
    interval = parse_duration(scenario.get("interval", DEFAULT_INTERVAL))
    items = scenario_items(scenario)
    rows = []
    for item, at in zip(items, row_times(items, start, interval, now)):
        if at is None:
            rows.append(clock(item))
            continue
        row = {"from": format_duration(at - start)}
        row.update({key: value for key, value in item.items() if key not in ("at", "offset", "after", "from")})
        rows.append(clock(row))
    return rows


def expected_rows(expected: dict) -> list[dict]:
    """Expected YAML of one usecase -> 1 row per checked column."""
    rows = []
    for family, block in expected.items():
        for step_name, checks in (block.get("steps") or {}).items():
            unsupported = set(checks) - {"db"}
            if unsupported:
                raise ValueError(f"expected step '{step_name}': {sorted(unsupported)} belum didukung di excel")
            for check in checks.get("db") or []:
                base = {
                    "step_name": step_name,
                    "family": None if family == DEFAULT_FAMILY else family,
                    "table": check["table"],
                    "where": format_where(check.get("where") or {}),
                }
                note = check.get("note")
                for column in (TIMEOUT_COLUMN, COUNT_COLUMN):
                    if column in check:
                        rows.append({**base, "column": column, "expected": check[column], "note": note})
                        note = None
                for column, value in (check.get("match") or {}).items():
                    rows.append({**base, "column": column, "expected": format_expected_value(value), "note": note})
                    note = None
    return rows


def case_to_excel(case: dict, expected_by_version: dict, path: Path) -> None:
    scenarios = as_scenarios(case)
    if len(scenarios) != 1:
        raise ValueError(f"{case['usecase']}: 1 excel = 1 timeline, ada {len(scenarios)} scenario")
    scenario = scenarios[0]

    workbook = Workbook()
    settings = workbook.active
    settings.title = "settings"
    settings_rows = [{"key": key, "value": join_value(key, clock({key: scenario[key]})[key])}
                     for key in SETTING_KEYS if key in scenario]
    write_sheet(settings, ["key", "value"], settings_rows)

    if case.get("description"):
        notes = workbook.create_sheet("notes", 0)
        for line in str(case["description"]).rstrip("\n").split("\n"):
            notes.append([line])
        notes.column_dimensions["A"].width = 120
    if case.get("db_requirements"):
        requirements = workbook.create_sheet(REQUIREMENTS_SHEET, 1 if case.get("description") else 0)
        for item in case["db_requirements"]:
            requirements.append([item])
        requirements.column_dimensions["A"].width = 120

    setup_rows = [{"kind": kind, **item} for kind, items in (scenario.get("setup") or {}).items() for item in items]
    if setup_rows:
        write_sheet(workbook.create_sheet("setup"), ordered_columns(["kind"], setup_rows), setup_rows)

    rows = timeline_rows(scenario)
    write_sheet(workbook.create_sheet("timeline"), ordered_columns(TIMELINE_COLUMNS, rows), rows,
                arrow_rows={index for index, row in enumerate(rows) if is_segment(row)})

    for version, expected in sorted(expected_by_version.items()):
        rows = expected_rows(expected.get(scenario["scenario"]) or {})
        sheet = workbook.create_sheet(f"{EXPECTED_SHEET}{version}")
        write_sheet(sheet, EXPECTED_COLUMNS, rows)
        for column, width in zip("ABCDEFG", (16, 10, 16, 48, 16, 28, 48)):
            sheet.column_dimensions[column].width = width

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)


# ---------- entry points ----------

def expected_path(version: str, usecase: str) -> Path:
    return VERSIONS_DIR / version / "expected" / f"{usecase}.yaml"


def excel2yaml(usecases: list[str] | None = None) -> list[Path]:
    paths = [EXCEL_DIR / f"{u}.xlsx" for u in usecases] if usecases else sorted(EXCEL_DIR.glob("*.xlsx"))
    written = []
    for path in paths:
        if path.name.startswith("~$"):             # excel lock file
            continue
        if not path.exists():
            raise FileNotFoundError(f"excel not found: {path}")
        usecase = path.stem
        if is_generated(usecase):
            raise ValueError(f"{usecase}: dibuat oleh generators/{usecase}.py, ubah generatornya lalu "
                             f"python cli.py generate --usecase {usecase}")
        target = CASES_DIR / f"{usecase}.yaml"
        dump_yaml(excel_to_case(path), target, usecase)
        written.append(target)
        for version, expected in excel_to_expected(path).items():
            target = expected_path(version, usecase)
            dump_yaml(expected, target, usecase, version)
            written.append(target)
    return written


def format_yaml(usecases: list[str] | None = None, write: bool = True) -> list[Path]:
    """Case + expected files not in the standard layout (values kept). write: rewrite them."""
    paths = [CASES_DIR / f"{u}.yaml" for u in usecases] if usecases else sorted(CASES_DIR.glob("*.yaml"))
    changed = []
    for path in paths:
        files = [(path, None)] + [
            (expected_path(version_dir.name, path.stem), version_dir.name)
            for version_dir in sorted(VERSIONS_DIR.iterdir())
            if expected_path(version_dir.name, path.stem).exists()
        ]
        for target, version in files:
            text = target.read_text(encoding="utf-8")
            formatted = yaml_text(yaml.load(text, Loader=YAML_LOADER) or {}, path.stem, version)
            if formatted != text:
                changed.append(target)
                if write:
                    target.write_text(formatted, encoding="utf-8")
    return changed


def yaml2excel(usecases: list[str] | None = None, force: bool = False) -> list[Path]:
    paths = [CASES_DIR / f"{u}.yaml" for u in usecases] if usecases else sorted(CASES_DIR.glob("*.yaml"))
    written = []
    for path in paths:
        target = EXCEL_DIR / f"{path.stem}.xlsx"
        if target.exists() and not force:
            raise FileExistsError(f"{target} sudah ada, pakai --force untuk menimpa")
        expected = {
            version_dir.name: load_yaml(version_dir / "expected" / f"{path.stem}.yaml")
            for version_dir in sorted(VERSIONS_DIR.iterdir())
            if (version_dir / "expected" / f"{path.stem}.yaml").exists()
        }
        case_to_excel(load_yaml(path), expected, target)
        written.append(target)
    return written


def stale_excel() -> list[str]:
    """Excel files newer than their YAML (forgot to run excel2yaml)."""
    stale = []
    for path in EXCEL_DIR.glob("*.xlsx"):
        target = CASES_DIR / f"{path.stem}.yaml"
        if not path.name.startswith("~$") and (not target.exists() or path.stat().st_mtime > target.stat().st_mtime):
            stale.append(path.name)
    return stale
