"""Stage 1 harness tests (no NATS / DB needed)."""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from harness.config import expand_vars, load_config, read_dotenv
from service.db import build_conninfo
from harness.identifiers import resolve_imei, resolve_model
from harness.injector import Injector, gate_time
from harness.loader import load_gate_format
from harness.timeline import expand_steps, initial_fields, parse_duration, parse_time

NOW = datetime(2026, 11, 18, 3, 0, 0, tzinfo=timezone.utc)


def test_parse_time_and_duration():
    assert parse_time("now", NOW) == NOW
    assert parse_time("now-20m", NOW) == NOW - timedelta(minutes=20)
    assert parse_time("now+5s", NOW) == NOW + timedelta(seconds=5)
    assert parse_time("2026-01-01T00:00:00", NOW) == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert parse_duration("30s") == timedelta(seconds=30)
    with pytest.raises(ValueError):
        parse_duration("30")


def test_expand_steps_time_alias_carry_over():
    scenario = {
        "start": "now-5m", "interval": "30s",
        "steps": [
            {"step_name": "a", "packet": "location", "lat": -6.2, "lng": 106.8, "acc": True, "protocol": "12"},
            {"packet": "location", "speed": 20},
            {"packet": "location", "at": "now", "door1": "open"},
        ],
    }
    steps = expand_steps(scenario, {"gps_satellites": 10}, NOW)
    assert [step.at for step in steps] == [NOW - timedelta(minutes=5), NOW - timedelta(minutes=4, seconds=30), NOW]
    assert steps[0].fields == {"gps_satellites": 10, "latitude": -6.2, "longitude": 106.8,
                               "engine_on": True, "protocol": "12"}
    # carry-over device fields, but not per-step options
    assert steps[1].fields == {"gps_satellites": 10, "latitude": -6.2, "longitude": 106.8,
                               "engine_on": True, "speed": 20}
    assert steps[2].fields["door1"] == "open" and steps[2].fields["speed"] == 20


def test_step_after_gap():
    scenario = {"start": "now-20m", "interval": "30s", "steps": [
        {"packet": "location"}, {"packet": "location"}, {"packet": "location", "after": "10m"}, {"packet": "location"},
    ]}
    times = [s.at for s in expand_steps(scenario, {}, NOW)]
    start = NOW - timedelta(minutes=20)
    assert times == [start, start + timedelta(seconds=30), start + timedelta(minutes=10, seconds=30),
                     start + timedelta(minutes=11)]
    assert "after" not in expand_steps(scenario, {}, NOW)[2].fields


def test_step_families_filter():
    scenario = {"steps": [
        {"packet": "login"},
        {"packet": "heartbeat", "families": ["concox"]},
        {"packet": "command_reply", "reply_text": "ok"},
    ]}
    concox = [s.packet for s in expand_steps(scenario, {}, NOW, family="concox")]
    teltonika = [s.packet for s in expand_steps(scenario, {}, NOW, family="teltonika")]
    assert concox == ["login", "heartbeat", "command_reply"]
    assert teltonika == ["login", "command_reply"]
    assert "families" not in expand_steps(scenario, {}, NOW, family="concox")[1].fields


def test_io_block_merged_across_steps():
    scenario = {"steps": [
        {"packet": "location", "io": {"digital_input_1": 1}},
        {"packet": "location", "io": {"external_voltage": 12000}},
    ]}
    steps = expand_steps(scenario, {}, NOW)
    assert steps[1].fields["io"] == {"digital_input_1": 1, "external_voltage": 12000}


def test_resolve_imei_and_model_priority():
    scenario = {"imei": {"concox": "358000000000099"}, "models": {"concox": "x3"}}
    config = {"devices": {"teltonika": {"imei": "350000000000099", "model": "fmb125"}}}
    assert resolve_imei(scenario, "concox", config) == "358000000000099"
    assert resolve_imei(scenario, "teltonika", config) == "350000000000099"
    assert resolve_imei(scenario, "concox", config, "358000000000777") == "358000000000777"
    with pytest.raises(KeyError):
        resolve_imei({}, "concox", {})
    assert resolve_model(scenario, "concox", config, "gt06n") == "x3"
    assert resolve_model({}, "teltonika", config, "fmb130") == "fmb125"
    assert resolve_model({}, "concox", config, "gt06n") == "gt06n"


KERNEL_ENV_VARS = (
    "ENDPOINT_NATS_HOST", "ENDPOINT_NATS_PORT", "HOST_DB_IOT", "PORT_DB_IOT", "USER_DB_IOT",
    "PASSWORD_DB_IOT", "DB_IOT", "SCHEMA_IOT", "USE_RDS_PROXY",
)


NO_DOTENV = Path("__no_dotenv__")          # tests never read the real .env


def test_expand_vars_types_only_single_variable():
    env = {"A": "abc", "PORT": "5432", "FLAG": "true", "PASS": "p@ss: #1"}
    value = {"x": "${A}|${MISSING}|${MISSING:-x}", "port": "${PORT}", "flag": "${FLAG}", "pass": "${PASS}",
             "list": ["${A}"], "plain": 7}
    assert expand_vars(value, env) == {"x": "abc||x", "port": 5432, "flag": True, "pass": "p@ss: #1",
                                       "list": ["abc"], "plain": 7}


def test_read_dotenv(tmp_path):
    dotenv = tmp_path / "test.env"
    dotenv.write_text('# comment\n\nHOST_DB_IOT=db.local\nexport USER_DB_IOT = "kernel"\nPASSWORD_DB_IOT=a=b\n',
                      encoding="utf-8")
    assert read_dotenv(dotenv) == {"HOST_DB_IOT": "db.local", "USER_DB_IOT": "kernel", "PASSWORD_DB_IOT": "a=b"}
    assert read_dotenv(NO_DOTENV) == {}


def test_env_yaml_kernel_defaults(monkeypatch):
    for name in KERNEL_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    config = load_config(NO_DOTENV)
    assert config["nats"] == {"stream": "iot-gateway", "tube": "PACKET", "host": "127.0.0.1", "port": 4222}
    assert config["db"] == {
        "host": "localhost", "port": 5432, "user": "kernel", "password": "password", "name": "iot",
        "schema": "vsms", "use_rds_proxy": False, "max_conn": 5,
    }
    assert config["devices"]["concox"]["imei"] and config["devices"]["teltonika"]["model"] == "fmb130"
    conninfo = build_conninfo(config["db"])
    assert "dbname=iot" in conninfo and "options='-c search_path=vsms'" in conninfo
    assert "search_path" not in build_conninfo({**config["db"], "use_rds_proxy": True})


def test_env_var_wins_over_dotenv(monkeypatch, tmp_path):
    dotenv = tmp_path / "test.env"
    dotenv.write_text("ENDPOINT_NATS_HOST=10.0.0.1\nHOST_DB_IOT=db.dotenv\nUSE_RDS_PROXY=true\n", encoding="utf-8")
    monkeypatch.setenv("HOST_DB_IOT", "db.os")
    monkeypatch.delenv("ENDPOINT_NATS_HOST", raising=False)
    monkeypatch.delenv("USE_RDS_PROXY", raising=False)
    config = load_config(dotenv)
    assert config["nats"]["host"] == "10.0.0.1" and config["db"]["host"] == "db.os"
    assert config["db"]["use_rds_proxy"] is True


def test_initial_fields_filtered_by_family():
    config = load_config(NO_DOTENV)
    concox = initial_fields(config, "concox", {"speed", "gps_satellites", "lbs", "battery", "signal_strength"})
    assert concox["lbs"]["mcc"] == 510 and "altitude" not in concox
    assert initial_fields(config, "teltonika", {"altitude", "codec"}) == {"altitude": 10, "codec": "8e"}


INJECTOR = Injector({"stream": "iot-gateway", "tube": "PACKET"}, load_gate_format("v2"))
IMEI = "358000000000001"
TIME = "2026-11-18T03:00:00.000Z"


def test_gate_data_message():
    stream, subject, body = INJECTOR.render("data", {
        "imei": IMEI, "family": "concox", "model": "gt06n", "payload": "78780d01", "received_on": TIME,
    })
    assert (stream, subject) == ("iot-gateway", f"PACKET.{IMEI}")
    assert json.loads(body) == {
        "data": "78780d01", "identifier": IMEI, "model": "gt06n", "family": "concox", "received_on": TIME,
    }


def test_gate_connect_message():
    stream, subject, body = INJECTOR.render("connect", {
        "imei": IMEI, "time": TIME, "ip_gate": "10.0.0.5", "port_gate": 5023, "gate_start_time": TIME,
    })
    assert (stream, subject) == ("iot-gateway", f"PACKET.{IMEI}")
    data = json.loads(body)
    assert data["message_type"] == "connection_status" and data["identifier"] == IMEI and data["time"] == TIME
    # message is a JSON string, port_gate stays a number
    assert json.loads(data["message"]) == {
        "status": "connected", "ip_gate": "10.0.0.5", "port_gate": 5023, "gate_start_time": TIME,
    }


def test_gate_disconnect_message():
    _, _, body = INJECTOR.render("disconnect", {"imei": IMEI, "time": TIME, "last_connect": TIME})
    assert json.loads(json.loads(body)["message"]) == {"status": "disconnected", "last_connect": TIME}


def test_gate_xmit_message():
    stream, subject, body = INJECTOR.render("xmit", {"command": "7000,EC,6869", "ip": "10.0.0.9", "port": 40000})
    assert (stream, subject) == ("iot-gateway-command", "PACKET_COMMAND.COMMAND")
    assert json.loads(body) == {"command": "7000,EC,6869", "ip": "10.0.0.9", "port": 40000}


def test_gate_missing_value_raises():
    with pytest.raises(KeyError):
        INJECTOR.render("xmit", {"command": "x"})


def test_gate_time_format():
    assert gate_time(datetime(2026, 11, 18, 3, 0, 0, 123456, tzinfo=timezone.utc)) == "2026-11-18T03:00:00.123Z"
