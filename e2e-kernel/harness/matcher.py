"""Compare expected values with DB values.

expected:
- {approx: x, tolerance: t} -> |actual - x| <= t (default tolerance from env.yaml match.approx_tolerance)
- datetime                  -> same moment, within DATETIME_TOLERANCE (packet time is whole seconds / ms)
- number                    -> numeric equal (DB Decimal / text number is fine)
- list                      -> DB array, same length and each item equal (ex: vehicle_status.temperature)
- other                     -> text equal
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

DATETIME_TOLERANCE = timedelta(seconds=1)


def to_number(value) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def same(expected, actual, approx_tolerance: float) -> bool:
    if actual is None:
        return expected is None
    if isinstance(expected, dict) and "approx" in expected:
        number = to_number(actual)
        return number is not None and abs(number - expected["approx"]) <= expected.get("tolerance", approx_tolerance)
    if isinstance(expected, list):
        return (isinstance(actual, (list, tuple)) and len(actual) == len(expected)
                and all(same(item, value, approx_tolerance) for item, value in zip(expected, actual)))
    if isinstance(expected, datetime):
        return isinstance(actual, datetime) and abs(aware(actual) - aware(expected)) <= DATETIME_TOLERANCE
    if isinstance(expected, bool) or isinstance(actual, bool):
        return str(expected).lower() == str(actual).lower()
    expected_number, actual_number = to_number(expected), to_number(actual)
    if expected_number is not None and actual_number is not None:
        return expected_number == actual_number
    return str(expected) == str(actual)


def describe(value) -> str:
    if isinstance(value, dict) and "approx" in value:
        return f"~{value['approx']}" + (f" ±{value['tolerance']}" if "tolerance" in value else "")
    if isinstance(value, datetime):
        return aware(value).astimezone(timezone.utc).isoformat(timespec="seconds")
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(describe(item) for item in value) + "]"
    return repr(value)


def match(expected: dict, actual: dict, approx_tolerance: float) -> list[str]:
    """Return mismatches (empty = all match)."""
    problems = []
    for column, value in expected.items():
        if column not in actual:
            problems.append(f"{column}: kolom tidak ada di tabel")
        elif not same(value, actual[column], approx_tolerance):
            problems.append(f"{column}: expected {describe(value)}, actual {describe(actual[column])}")
    return problems
