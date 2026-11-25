"""Resolve IMEI and model per scenario x family.

Priority: CLI --imei > case `imei: {family: ...}` > env.yaml `devices.<family>`.
"""
from creators.base import IdentifierFormat


def resolve_imei(scenario: dict, family: str, config: dict, cli_imei: str | None = None) -> str:
    imei = (
        cli_imei
        or (scenario.get("imei") or {}).get(family)
        or ((config.get("devices") or {}).get(family) or {}).get("imei")
    )
    if not imei:
        raise KeyError(f"no IMEI for family '{family}': set env.yaml devices.{family}.imei or case `imei:`")
    return str(imei)


def resolve_model(scenario: dict, family: str, config: dict, default_model: str) -> str:
    return str(
        (scenario.get("models") or {}).get(family)
        or ((config.get("devices") or {}).get(family) or {}).get("model")
        or default_model
    )


def validate_imei(imei: str, identifier_format: IdentifierFormat, family: str) -> None:
    if len(imei) != identifier_format.length or any(c not in identifier_format.charset for c in imei):
        raise ValueError(
            f"{family}: IMEI '{imei}' must be {identifier_format.length} chars of '{identifier_format.charset}'"
        )
