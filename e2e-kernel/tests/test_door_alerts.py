"""Door alert simulator (generators/door_common.py) vs kernel DoorSensor rules (usecase/sensor/door/test_door.py),
and the door_concox generator. No network."""
from generators import door, door_concox
from generators.door_common import (
    CLOSE, OPEN, OPEN_TOO_LONG, DoorAlerts, add_health_status, analog_value, digital_value,
)


def run(messages, min_time=None):
    """messages: ("loc", sec) or (sec, door, value). Door 1 starts closed (confirmed at 0)."""
    sim = DoorAlerts(min_time or {1: 5, 2: None})
    sim.location(0)
    sim.door(1, 1, CLOSE)
    sim.location(10)
    sim.alerts.clear()
    for message in messages:
        if message[0] == "loc":
            sim.location(message[1])
        else:
            sim.door(*message)
    return sim


def alerts(sim):
    return [(alert.code, alert.time, alert.created) for alert in sim.alerts]


def test_pin_conversion_digital_and_analog():
    assert digital_value(1) == OPEN and digital_value(0) == CLOSE
    assert analog_value(0) == CLOSE and analog_value(4.99) == CLOSE
    assert analog_value(5) == OPEN and analog_value(1024) == OPEN


def test_first_packet_after_reset_is_an_alert_even_closed():
    sim = DoorAlerts({1: None})
    sim.location(0)
    sim.door(5, 1, CLOSE)
    assert sim.alerts == []                          # pending: no newer location yet
    sim.location(10)
    assert alerts(sim) == [(19, 5, 10)] and sim.status[1] == CLOSE


def test_change_confirmed_by_next_location_more_than_3_s_later():
    sim = run([(100, 1, OPEN), ("loc", 102), ("loc", 104)])
    assert alerts(sim) == [(4, 100, 104)]            # 2 s: still waiting, 4 s: DOOR1OPEN with the change time
    assert sim.last_alert[1] == 100


def test_open_close_within_3_s_is_a_glitch():
    sim = run([(100, 1, OPEN), (102, 1, CLOSE), ("loc", 110)])
    assert sim.alerts == [] and sim.status[1] == CLOSE and sim.last_open[1] is None
    assert [(glitch.value, glitch.start, glitch.end) for glitch in sim.glitches] == [(OPEN, 100, 102)]


def test_exactly_3_s_is_still_a_glitch_4_s_is_a_change():
    assert run([(100, 1, OPEN), (103, 1, CLOSE), ("loc", 110)]).alerts == []
    assert alerts(run([(100, 1, OPEN), (104, 1, CLOSE), ("loc", 110)])) == [(4, 100, 104), (19, 104, 110)]


def test_open_too_long_once_with_min_time_trigger_and_time_of_the_check():
    sim = run([(100, 1, OPEN), ("loc", 110), ("loc", 390), ("loc", 400), ("loc", 410)])     # limit 5 min = 400
    too_long = [alert for alert in sim.alerts if alert.code == OPEN_TOO_LONG]
    assert [(alert.time, alert.data, alert.trigger) for alert in too_long] == [(400, 5, 4)]
    assert sim.last_open[1] is None


def test_open_too_long_checked_on_the_door_packet_too():
    sim = DoorAlerts({2: 10})
    sim.location(0)
    sim.door(5, 2, OPEN)
    sim.location(10)
    sim.door(604, 2, OPEN)                           # same value (voltage changed): 599 s, not yet
    sim.door(605, 2, OPEN)                           # 600 s
    assert [(alert.code, alert.time) for alert in sim.alerts] == [(5, 5), (OPEN_TOO_LONG, 605)]


def test_no_open_too_long_without_alert_min_time():
    sim = DoorAlerts({2: None})
    sim.location(0)
    sim.door(5, 2, OPEN)
    sim.location(5000)
    assert [alert.code for alert in sim.alerts] == [5]


def test_close_glitch_while_open_keeps_the_open_too_long_start():
    sim = run([(100, 1, OPEN), ("loc", 110), (200, 1, CLOSE), (202, 1, OPEN), ("loc", 400)])
    assert [(alert.code, alert.time) for alert in sim.alerts] == [(4, 100), (OPEN_TOO_LONG, 400)]


def test_close_before_the_limit_but_confirmed_after_it_is_flagged():
    sim = run([(100, 1, OPEN), ("loc", 110), (395, 1, CLOSE), ("loc", 400)])
    assert [alert.code for alert in sim.alerts] == [4, OPEN_TOO_LONG, 19]
    assert [alert.time for alert in sim.late_too_long] == [400]


def closed_doors(min_time: dict) -> DoorAlerts:
    """All doors confirmed closed at 10 s, alerts cleared."""
    sim = DoorAlerts(min_time)
    sim.record(0, dict.fromkeys(min_time, CLOSE))
    sim.record(10, dict.fromkeys(min_time, CLOSE))
    sim.alerts.clear()
    return sim


def test_monitoring_record_confirms_the_change_of_an_event_record():
    sim = closed_doors({1: 15, 2: None})
    sim.record(15, {1: OPEN, 2: CLOSE})              # event record
    sim.record(20, {1: OPEN, 2: CLOSE})              # monitoring record, 5 s later
    assert alerts(sim) == [(4, 15, 20)]


def test_door3_is_not_staged_only_its_own_packet_confirms():
    door1, door3 = closed_doors({1: None}), closed_doors({3: None})
    door1.door(20, 1, OPEN)
    door3.door(20, 3, OPEN)
    door1.location(30)
    door3.location(30)
    assert alerts(door1) == [(4, 20, 30)] and door3.alerts == []     # sensor_staged_packet: DOOR1 / DOOR2 only
    door3.record(40, {3: OPEN})
    assert alerts(door3) == [(144, 20, 40)]


def test_door_build_matches_cases():
    case, expected = door.build()
    steps = expected["door"]["default"]["steps"]
    assert case["families"] == ["teltonika", "ruptela", "queclink"]
    assert [item["pin"] for item in case["setup"]["sensors"]] == ["digital_2", "digital_3"]
    named = {row["step_name"] for row in case["timeline"] if "step_name" in row}
    assert named == set(steps)
    assert all({"door1", "door2"} <= set(row) for row in case["timeline"])     # every record
    assert not any("analog_input_2" in row for row in case["timeline"])
    tables = [check["table"] for check in steps["end"]["db"]]
    assert tables.count("summary_device_error") == 1 + len(door.HEALTH)


def test_door_concox_build_matches_cases():
    case, expected = door_concox.build()
    steps = expected["door_concox"]["default"]["steps"]
    assert case["received_on"] == "packet" and case["setup"]["sensors"][1] == {"code": "door2", "pin": "analog_1"}
    named = {row["step_name"] for row in case["timeline"] if "step_name" in row}
    assert named == set(steps) and "end" in named
    sensor_rows = [row for row in case["timeline"] if row["packet"] == "sensor"]
    assert {key for row in sensor_rows for key in row} <= {"from", "step_name", "packet", "door1", "analog_input_1"}
    assert sum("door1" in row for row in sensor_rows) == len(door_concox.DOOR1_EVENTS)
    analog = [row["from"] for row in sensor_rows if "analog_input_1" in row]
    assert analog[:2] == ["00:00:05", "00:00:15"] and "02:45:05" not in analog      # every 10 s, none offline
    statuses = health_statuses(steps)
    assert statuses["cek_h1_025200"] == {"door_1_error_status": None, "door_2_error_status": "DOOR-01"}
    assert statuses["end"] == {"door_1_error_status": None, "door_2_error_status": None}
    assert statuses["cek_h5_000000"]["door_1_error_status"] == "DOOR-04"
    assert statuses["cek_h6_000000"]["door_2_error_status"] == "DOOR-04"


def health_statuses(steps: dict) -> dict:
    return {name: check["match"] for name, step in steps.items() for check in step["db"]
            if check["table"] == "device_health"}


def test_health_status_checked_where_it_changes_and_at_the_end():
    statuses = {0: (None, None), 10: ("DOOR-03", None), 20: ("DOOR-03", None), 30: (None, None), 40: (None, None)}
    checks = add_health_status({20: ["alert"]}, statuses, last_sec=40)
    assert list(checks) == [10, 20, 30, 40] and checks[20] == ["alert"]
    assert checks[10][0]["match"] == {"door_1_error_status": "DOOR-03", "door_2_error_status": None}
    assert checks[10][0]["where"] == {"imei": "$imei"} and checks[10][0]["note"].startswith("door-1 normal -> DOOR-03")
    assert checks[30][0]["match"]["door_1_error_status"] is None


def test_door_health_status_follows_the_summaries():
    _, expected = door.build()
    statuses = health_statuses(expected["door"]["default"]["steps"])
    assert "cek_h1_010523" not in statuses                                     # C: glitch, not a valid change
    assert statuses["cek_h1_013020"]["door_1_error_status"] == "DOOR-03"       # E: 2nd valid change confirmed
    assert statuses["cek_h1_023410"]["door_1_error_status"] is None            # 10 km: DOOR-03 summary
    assert statuses["cek_h1_035700"]["door_2_error_status"] == "DOOR-02"       # J: open 10 km while driving
    assert statuses["cek_h1_040530"]["door_2_error_status"] is None            # close confirmed: DOOR-02 summary
    assert statuses["cek_h2_081020"]["door_2_error_status"] == "DOOR-03"       # O: valid changes while driving
    assert statuses["cek_h5_000000"]["door_1_error_status"] == "DOOR-04"       # T: 3rd day > 10 km
    assert statuses["cek_h5_083500"]["door_1_error_status"] is None            # T: open confirmed
    assert statuses["cek_h6_000000"]["door_2_error_status"] == "DOOR-04"       # U: door2 3rd day
    assert statuses["cek_h6_083500"]["door_2_error_status"] is None
    assert all(value is None for value in statuses["end"].values())


def test_door04_summaries_for_both_doors_without_known_bug():
    """Kernel dc0e9eea5 counts DOOR-04 days on the location packet time: a replay changes day, no known_bug."""
    for module in (door, door_concox):
        _, expected = module.build()
        end = expected[module.USECASE]["default"]["steps"]["end"]["db"]
        door04 = [check["where"] for check in end if check["where"].get("error_type") == "DOOR-04"]
        assert [(where["description"], where["start_time"], where["end_time"]) for where in door04] == [
            ("door-1", "H1 05:52:40", "H5 08:30:15"), ("door-2", "H3 08:23:15", "H6 08:30:05")]
        steps = expected[module.USECASE]["default"]["steps"]
        assert not any("known_bug" in check for step in steps.values() for check in step["db"])
