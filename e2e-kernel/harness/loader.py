"""Load cases/<usecase>.yaml (input), versions/<ver>/expected/<usecase>.yaml and gate_format.yaml.

Case format: 1 usecase = 1 timeline
    usecase, description, db_requirements, families, start, interval, (imei, models, tags, carry_over, realtime),
    setup, timeline
    db_requirements: required list, 1 item = 1 DB condition to prepare before the run (ex: sensor pin, company setting)
    carry_over: false = empty cell is not sent (no defaults / carry-over), see harness/timeline.py
    realtime:   true = send each packet at its own time (wait), for logic that uses server time
    received_on: gate message received_on. send (default) = the moment e2e sends it,
                packet = the packet send moment on the timeline, like a live device without delay (needed when the
                kernel takes the packet time from received_on, ex: concox 0x94 has no time)
Internally a case is turned into one scenario named like the usecase (expected key = usecase).
"""
from harness.config import ROOT, load_yaml

CASES_DIR = ROOT / "cases"
VERSIONS_DIR = ROOT / "versions"

SCENARIO_KEYS = (
    "families", "start", "interval", "tags", "imei", "models", "carry_over", "realtime", "received_on", "setup",
    "timeline",
)


def as_scenarios(case: dict) -> list[dict]:
    """Case -> list with one scenario (name = usecase). Legacy `scenarios:` list is returned as is."""
    if "scenarios" in case:
        return case["scenarios"] or []
    scenario = {"scenario": case["usecase"]}
    scenario.update({key: case[key] for key in SCENARIO_KEYS if key in case})
    return [scenario]


def load_cases(usecases: list[str] | None = None) -> list[dict]:
    """All cases, or only the given usecases (file name = usecase). Each case gets `scenarios`."""
    if usecases:
        paths = [CASES_DIR / f"{usecase}.yaml" for usecase in usecases]
        missing = [path.name for path in paths if not path.exists()]
        if missing:
            raise FileNotFoundError(f"case file not found: {missing}")
    else:
        paths = sorted(CASES_DIR.glob("*.yaml"))
    cases = []
    for path in paths:
        case = load_yaml(path)
        check_db_requirements(case, path.name)
        case["scenarios"] = as_scenarios(case)
        cases.append(case)
    return cases


def check_db_requirements(case: dict, name: str) -> None:
    """Every case lists what must be prepared in the DB (readable, separate from description)."""
    requirements = case.get("db_requirements")
    valid = isinstance(requirements, list) and requirements and all(isinstance(item, str) for item in requirements)
    if not valid:
        raise ValueError(f"{name}: 'db_requirements' wajib, list teks (1 baris = 1 syarat DB)")


def load_gate_format(version: str) -> dict:
    path = VERSIONS_DIR / version / "gate_format.yaml"
    if not path.exists():
        raise FileNotFoundError(f"gate format not found: {path}")
    return load_yaml(path)
