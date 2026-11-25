"""Expected checks: load versions/<v>/expected/<usecase>.yaml, pick checks per family, resolve variables.

Format: {<scenario>: {default: {steps: {<step_name>: {db: [check]}}}, <family>: {steps: {...}}}}
A family block replaces the default checks of the same step.

Variables in where / expected:
- $step_at         packet time of the checked step
- $at.<step_name>  packet time of another step
- $imei, $vehicle_id, $company_id   device test (from DB)
- $<field>         value sent in the checked step (ex: $ip_gate, $port_gate, $reply_text)
- H<day> HH:MM:SS  time on the timeline, H1 00:00:00 = timeline start (ex: H2 00:30:00 = start + 1 day 30 min)
"""
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from harness.config import ROOT, load_yaml

VERSIONS_DIR = ROOT / "versions"
VARIABLE_RE = re.compile(r"^\$([A-Za-z_]\w*)(?:\.(\w+))?$")
DAY_TIME_RE = re.compile(r"^H(\d+) (\d{2}):([0-5]\d):([0-5]\d)$")


@dataclass
class CheckContext:
    device: dict                                         # imei, vehicle_id, company_id
    step_times: dict[str, datetime]                      # step_name -> packet time
    step_values: dict[str, dict] = field(default_factory=dict)   # step_name -> values sent
    timeline_start: datetime | None = None               # H1 00:00:00


def load_expected(version: str, usecase: str) -> dict | None:
    path = VERSIONS_DIR / version / "expected" / f"{usecase}.yaml"
    return load_yaml(path) if path.exists() else None


def steps_for(expected: dict, scenario: str, family: str) -> dict:
    """{step_name: checks} for one scenario x family (family block overrides default per step)."""
    blocks = expected.get(scenario) or {}
    steps = dict((blocks.get("default") or {}).get("steps") or {})
    steps.update((blocks.get(family) or {}).get("steps") or {})
    return steps


def resolve(value, context: CheckContext, step_name: str):
    """Replace $variables (recursively in dict / list)."""
    if isinstance(value, dict):
        return {key: resolve(item, context, step_name) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve(item, context, step_name) for item in value]
    if not isinstance(value, str):
        return value
    day_time = DAY_TIME_RE.match(value.strip())
    if day_time:
        if context.timeline_start is None:
            raise KeyError(f"'{value}': timeline start unknown")
        day, hours, minutes, seconds = (int(part) for part in day_time.groups())
        return context.timeline_start + timedelta(days=day - 1, hours=hours, minutes=minutes, seconds=seconds)
    variable = VARIABLE_RE.match(value.strip())
    if not variable:
        return value
    name, sub = variable.groups()
    if name == "at" and sub:
        if sub not in context.step_times:
            raise KeyError(f"$at.{sub}: step '{sub}' tidak ada di timeline")
        return context.step_times[sub]
    if name == "step_at":
        return context.step_times[step_name]
    values = context.step_values.get(step_name) or {}
    if name in values:
        return values[name]
    if name in context.device:
        return context.device[name]
    raise KeyError(f"${name}: variabel tidak dikenal (step '{step_name}')")
