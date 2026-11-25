"""Write cases/<usecase>.yaml + versions/v2/expected/<usecase>.yaml from generators/<usecase>.py.

A generator module has build() -> (case, expected): case dict (usecase, ..., timeline) and expected dict
({usecase: {default: {steps: ...}}}) for v2. Use it when the timeline is computed (ex: real road routes).
"""
import importlib
from pathlib import Path

from harness.excel import CASES_DIR, dump_yaml, expected_path, is_generated

GENERATED_VERSION = "v2"


def generate_case(usecase: str) -> list[Path]:
    if not is_generated(usecase):
        raise FileNotFoundError(f"generator not found: generators/{usecase}.py")
    case, expected = importlib.import_module(f"generators.{usecase}").build()
    case_path = CASES_DIR / f"{usecase}.yaml"
    expected_file = expected_path(GENERATED_VERSION, usecase)
    dump_yaml(case, case_path, usecase)
    dump_yaml(expected, expected_file, usecase, GENERATED_VERSION)
    return [case_path, expected_file]
