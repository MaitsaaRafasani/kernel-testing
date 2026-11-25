"""Load config: env.yaml (test settings) + .env (connection / credential, not in git).

${VAR} / ${VAR:-default} in env.yaml are read from environment variables, then from .env
(environment variable wins, like kernel v2 decouple). Names are the same as kernel v2.
"""
import os
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT / "env.yaml"
DOTENV_FILE = ROOT / ".env"
VAR_RE = re.compile(r"\$\{(\w+)(?::-([^}]*))?\}")
QUOTES = ("'", '"')
YAML_LOADER = getattr(yaml, "CSafeLoader", yaml.SafeLoader)     # libyaml when installed: same result, ~4x faster


def load_yaml(path: Path) -> dict:
    with open(path, encoding="utf-8") as file:
        return yaml.load(file, Loader=YAML_LOADER) or {}


def read_dotenv(path: Path) -> dict:
    """KEY=VALUE per line. Empty lines, `#` comments and `export ` prefix are allowed."""
    if not path.exists():
        return {}
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in QUOTES:
            value = value[1:-1]
        values[key.strip().removeprefix("export ").strip()] = value
    return values


def expand_vars(value, env: dict):
    """Replace ${VAR} / ${VAR:-default} in parsed YAML values (recursive).
    A string that is only one variable gets a number / bool type (ex: port 5432, use_rds_proxy false)."""
    if isinstance(value, dict):
        return {key: expand_vars(item, env) for key, item in value.items()}
    if isinstance(value, list):
        return [expand_vars(item, env) for item in value]
    if not isinstance(value, str) or "${" not in value:
        return value
    text = VAR_RE.sub(lambda match: env.get(match.group(1), match.group(2) or ""), value)
    if not VAR_RE.fullmatch(value):
        return text
    try:
        typed = yaml.safe_load(text)
    except yaml.YAMLError:
        return text
    return typed if isinstance(typed, (bool, int, float)) else text


def load_config(dotenv: Path = DOTENV_FILE) -> dict:
    env = {**read_dotenv(dotenv), **os.environ}
    return expand_vars(load_yaml(ENV_FILE), env)
