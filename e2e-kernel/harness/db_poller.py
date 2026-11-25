"""Check expected rows in DB: poll until all match or timeout.

check: {table, where, match?, count?, timeout?}
- where: {column: value} or {column: {between: [a, b]}}. A datetime value matches ± DATETIME_TOLERANCE.
- match: columns of the latest row (ordered by the table time column)
- count: number of rows matching `where`
Freshness (anti false-pass) from versions/<v>/tables.yaml: {table: {time: column, since: sent | start}}
- since sent  -> time column >= moment the step was published (server time columns)
- since start -> time column >= timeline start (packet time columns)
Table / column names are validated, values are always query parameters.
"""
import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from harness.matcher import DATETIME_TOLERANCE, describe, match

IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass
class CheckResult:
    ok: bool
    message: str


def identifier(name: str) -> str:
    if not IDENTIFIER_RE.match(str(name)):
        raise ValueError(f"nama tabel / kolom tidak valid: '{name}'")
    return name


def build_where(where: dict, freshness: tuple[str, datetime] | None) -> tuple[str, list]:
    parts, params = [], []
    for column, value in where.items():
        if isinstance(value, dict) and "between" in value:
            parts.append(f"{identifier(column)} BETWEEN %s AND %s")
            params += list(value["between"])
        elif isinstance(value, datetime):         # same moment as matcher: ± DATETIME_TOLERANCE
            parts.append(f"{identifier(column)} BETWEEN %s AND %s")
            params += [value - DATETIME_TOLERANCE, value + DATETIME_TOLERANCE]
        else:
            parts.append(f"{identifier(column)} = %s")
            params.append(value)
    if freshness:
        column, since = freshness
        parts.append(f"{identifier(column)} >= %s")
        params.append(since)
    return (" WHERE " + " AND ".join(parts)) if parts else "", params


async def read_once(db, check: dict, time_column: str | None, where_sql: str, params: list) -> dict:
    """{'count': n} and / or {'row': {...} | None}."""
    table = identifier(check["table"])
    result = {}
    if "count" in check:
        row = await db.get_one(f"SELECT count(*) AS count FROM {table}{where_sql}", params)
        result["count"] = row["count"]
    if check.get("match"):
        order = f" ORDER BY {identifier(time_column)} DESC NULLS LAST" if time_column else ""
        result["row"] = await db.get_one(f"SELECT * FROM {table}{where_sql}{order} LIMIT 1", params)
    return result


def evaluate(check: dict, state: dict, approx_tolerance: float) -> list[str]:
    problems = []
    if "count" in check and state["count"] != check["count"]:
        problems.append(f"count: expected {check['count']}, actual {state['count']}")
    if check.get("match"):
        if state["row"] is None:
            problems.append("tidak ada baris yang cocok dengan where")
        else:
            problems += match(check["match"], state["row"], approx_tolerance)
    return problems


async def check_db(
    db, check: dict, table_info: dict, sent_at: datetime, timeline_start: datetime,
    timeout: timedelta, poll: timedelta, approx_tolerance: float, clock_skew: timedelta,
) -> CheckResult:
    """check: already resolved (no $variables)."""
    time_column = table_info.get("time")
    freshness = None
    if time_column:
        since = sent_at - clock_skew if table_info.get("since", "sent") == "sent" else timeline_start
        freshness = (time_column, since)
    where_sql, params = build_where(check.get("where") or {}, freshness)

    deadline = asyncio.get_running_loop().time() + timeout.total_seconds()
    while True:
        state = await read_once(db, check, time_column, where_sql, params)
        problems = evaluate(check, state, approx_tolerance)
        if not problems:
            return CheckResult(True, summary(check))
        if asyncio.get_running_loop().time() >= deadline:
            freshness_note = f" ({time_column} >= {describe(freshness[1])})" if freshness else ""
            return CheckResult(False, f"{summary(check)}{freshness_note}: " + "; ".join(problems))
        await asyncio.sleep(poll.total_seconds())


def summary(check: dict) -> str:
    where = ", ".join(f"{key}={describe(value)}" for key, value in (check.get("where") or {}).items())
    columns = list((check.get("match") or {}).keys()) + (["count"] if "count" in check else [])
    return f"{check['table']} [{where}] {', '.join(columns)}"
