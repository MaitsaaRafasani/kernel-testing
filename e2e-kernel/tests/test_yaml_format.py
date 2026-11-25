"""Standard YAML layout of cases + expected (harness/excel.py yaml_text). Fix: python cli.py format."""
import pytest

from harness import excel
from harness.excel import format_yaml, yaml_text


def test_all_case_and_expected_files_use_the_standard_layout():
    changed = [path.name for path in format_yaml(write=False)]
    assert changed == [], f"jalankan: python cli.py format ({changed})"


def test_case_keys_timeline_columns_and_acc_in_standard_order():
    case = {
        "timeline": [{"packet": "location", "speed": 30, "lng": 106.8, "acc": "on", "door1": "open", "lat": -6.1,
                      "from": "00:00:00"}],
        "families": ["teltonika"], "db_requirements": ["sensor door1"], "usecase": "uc_x",
    }
    lines = yaml_text(case, "uc_x").splitlines()
    assert lines[4:] == [                                   # after the 3 header lines + empty line
        "usecase: uc_x",
        "db_requirements:",
        "- sensor door1",
        "families: [teltonika]",
        "timeline:",
        "- {from: '00:00:00', packet: location, lat: -6.1, lng: 106.8, speed: 30, acc: true, door1: open}",
    ]
    assert lines[1].startswith("# Edit lewat Excel")


def test_expected_check_on_one_line_in_standard_key_order():
    expected = {"uc_x": {"default": {"steps": {"end": {"db": [
        {"note": "n", "match": {"type": "M"}, "count": 1, "where": {"vehicle_id": "$vehicle_id"}, "table": "t"},
    ]}}}}}
    assert yaml_text(expected, "uc_x", "v2").splitlines()[-1] == (
        "        - {table: t, where: {vehicle_id: $vehicle_id}, count: 1, match: {type: M}, note: n}")


def test_generated_usecase_header_and_excel2yaml_refused(tmp_path, monkeypatch):
    assert "generators/device_normal.py" in yaml_text({"usecase": "device_normal"}, "device_normal")
    monkeypatch.setattr(excel, "EXCEL_DIR", tmp_path)
    (tmp_path / "device_normal.xlsx").touch()
    with pytest.raises(ValueError, match="generate"):
        excel.excel2yaml(["device_normal"])
