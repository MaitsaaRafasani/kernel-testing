"""Every case lists its DB requirements in `db_requirements`, separate from the description."""
import re

import pytest
import yaml

from harness.loader import CASES_DIR, check_db_requirements

# read only the case header (timelines are large)
HEADER_END = re.compile(r"^(families|timeline):", re.MULTILINE)


@pytest.mark.parametrize("path", sorted(CASES_DIR.glob("*.yaml")), ids=lambda path: path.stem)
def test_every_case_has_db_requirements(path):
    text = path.read_text(encoding="utf-8")
    header = yaml.safe_load(text[:HEADER_END.search(text).start()])
    check_db_requirements(header, path.name)
    assert "Syarat DB (diatur manual)" not in header["description"]          # not mixed into description


@pytest.mark.parametrize("value", [None, [], "park_timeout <= 10", [1]])
def test_missing_or_bad_db_requirements_rejected(value):
    with pytest.raises(ValueError, match="db_requirements"):
        check_db_requirements({"db_requirements": value} if value is not None else {}, "x.yaml")
