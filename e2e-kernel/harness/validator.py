"""`e2e check`: validate cases, expected, gate_format, fields, IMEI format. No data is sent."""


def validate(envelope: str, expect: str) -> list[str]:
    """Return list of error messages (empty = valid)."""
    raise NotImplementedError
