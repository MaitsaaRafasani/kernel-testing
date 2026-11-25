"""Device health generator helpers (generators/health_common.py), small synthetic data. No network."""
from datetime import datetime, timezone

from generators.door_common import DoorMessage, simulate_door_health
from generators.health_common import DAY, CLOSE, OPEN, fmt, m, schedule, simulate_fuel, simulate_gps, voltage
from harness.runner import data_values, received_time


def record(sec, acc=True, sats=12, lng=107.0, lat=-6.2, fuel=None, volt=13.9, speed=40, doors=None):
    return {"sec": sec, "lng": lng, "lat": lat, "sats": sats, "acc": acc, "volt": volt, "fuel": fuel, "speed": speed,
            "doors": doors}


def spans(summaries):
    return [(item.label, item.start, item.end) for item in summaries]


def test_schedule_10_s_acc_on_5_min_acc_off_and_record_at_acc_change():
    rows = schedule(m(0, 12), [(m(0, 1), m(0, 11))])
    assert rows[:7] == [(0, True), (10, True), (20, True), (30, True), (40, True), (50, True), (60, False)]
    assert (m(0, 11), True) in rows and len([row for row in rows if not row[1]]) == 2
    assert fmt(m(1, 2, 3, day=1)) == "H2 01:02:03"
    assert voltage(0, True) > 13.3 > voltage(0, False)


def test_gps01_after_16_minutes_without_location():
    moving = [record(sec, lng=107.0 + sec / 1e5) for sec in range(0, 2400, 10)]
    no_location = [dict(item, acc=None) if 600 <= item["sec"] < 1800 else item for item in moving]
    assert spans(simulate_gps(no_location)) == [("GPS-01", 590, 1800)]
    assert simulate_gps([dict(item, acc=None) if 600 <= item["sec"] < 1200 else item for item in moving]) == []


def test_gps03_parked_with_engine_on_one_hour():
    parked = [record(sec) for sec in range(0, 4000, 10)] + [record(4000, lng=107.01)]
    assert [(item.label, item.start) for item in simulate_gps(parked)] == [("GPS-03", 0)]
    engine_off = [dict(item, volt=12.5) for item in parked]
    assert simulate_gps(engine_off) == []
    assert simulate_gps([dict(item, volt=None) for item in parked]) == []      # no accu voltage = engine off


def test_gps03_active_is_kept_while_engine_off_until_the_location_changes():
    """Kernel test_check_GPS03_flow_engine_off_keeps_error_until_location_changes: idle engine on > 1 h (GPS-03),
    engine off at the same place, then driving: 1 summary from the stop to the first new location."""
    idle = [record(sec) for sec in range(0, 4800, 10)]                          # engine on, GPS-03 from 3600
    off = [record(sec, volt=12.5) for sec in range(4800, 6600, 300)]            # engine off, same place
    drive = [record(6600, lng=107.001)]
    assert spans(simulate_gps(idle + off + drive)) == [("GPS-03", 0, 6600)]
    short_idle = [record(sec) for sec in range(0, 1800, 10)]                    # 30 min: not GPS-03 yet
    off_restart = [record(sec, volt=12.5) for sec in range(1800, 6600, 300)]    # engine off restarts the timer
    assert simulate_gps(short_idle + off_restart + drive) == []


def test_fuel01_and_fuel03_by_distance():
    def drive(sec):                       # ~111 m per record, fuel value changes every record
        return record(sec, lng=107.0 + sec / 1e4, fuel=round(40 - sec / 1e4, 4))

    lost = [dict(drive(sec), fuel=None) if 600 <= sec < 1500 else drive(sec) for sec in range(0, 2000, 10)]
    assert spans(simulate_fuel(lost)[0]) == [("FUEL-01", 590, 1500)]
    stuck = [dict(drive(sec), fuel=39.0) if 600 <= sec < 1800 else drive(sec) for sec in range(0, 2000, 10)]
    summaries, active = simulate_fuel(stuck)
    assert [item.label for item in summaries] == ["FUEL-03"]       # 120 records x 111 m > 10 km


def drive_with_door(events, until, step_m=111):
    """Driving ~40 km/h (111 m per 10 s record), door value only on the event seconds (on change)."""
    return [record(sec, lng=107.0 + sec / 1e4 * step_m / 111, doors=[events.get(sec)]) for sec in range(0, until, 10)]


def door_health(data, on_change, extra_doors=()):
    """Location message per record, then a door message (concox 0x94). extra_doors: (sec, value) between records."""
    messages = [DoorMessage(item["sec"], [], item) for item in data]
    messages += [DoorMessage(item["sec"], [(1, item["doors"][0], on_change)]) for item in data
                 if item["doors"][0] is not None]
    messages += [DoorMessage(sec, [(1, value, on_change)]) for sec, value in extra_doors]
    messages.sort(key=lambda message: (message.sec, bool(message.doors)))
    return simulate_door_health(messages, 1, {1: None})[0]


def test_door_on_change_silence_is_not_door01_except_before_the_first_door_packet():
    data = drive_with_door({1200: OPEN, 1210: CLOSE, 3000: OPEN}, 3100)       # silent 1210 -> 3000
    on_change = door_health(data, on_change=True)
    assert spans(on_change) == [("DOOR-01", 0, 1200)]       # first window, closed by the first on change packet
    assert on_change[0].description == "door-1"
    periodic = door_health(data, on_change=False)
    assert spans(periodic) == [("DOOR-01", 0, 1200), ("DOOR-01", 1210, 3000)]


def test_door02_open_while_driving_10_km_and_door03_two_changes_in_10_km():
    long_open = drive_with_door({0: CLOSE, 100: OPEN, 1300: CLOSE}, 1400)     # open 1200 s at 40 km/h = 13 km
    assert [(item.label, item.note) for item in door_health(long_open, on_change=True)] == [
        ("DOOR-02", "jarak terbuka 13.3 km")]
    short_open = drive_with_door({0: CLOSE, 100: OPEN, 600: CLOSE}, 1400)     # 5.5 km open, 2 changes
    assert spans(door_health(short_open, on_change=True)) == [("DOOR-03", 100, 600)]


def test_door03_counts_only_valid_changes():
    """Glitches (back within 3 s) while driving are 4 raw changes but no valid one: no DOOR-03."""
    data = drive_with_door({0: CLOSE}, 1400)
    glitches = [(101, OPEN), (103, CLOSE), (201, OPEN), (203, CLOSE)]
    assert door_health(data, on_change=True, extra_doors=glitches) == []
    valid = [(101, OPEN), (105, CLOSE), (201, OPEN), (205, CLOSE)]            # 4 s each: 4 valid changes
    assert spans(door_health(data, on_change=True, extra_doors=valid)) == [("DOOR-03", 101, 205)]


def test_door04_after_3_days_driven_over_10_km_without_a_valid_change():
    """Kernel test_check_door_DOOR04_*: a day counts when driven > 10 km after the last valid change, the 3rd day
    makes DOOR-04, the next valid change closes it. A day under 10 km does not count."""
    def days(records_per_day):
        data, lng = [], 107.0
        for day, count in enumerate(records_per_day):
            for i in range(count):                                         # ~111 m per record, 40 km/h
                data.append(record(day * DAY + 8 * 3600 + i * 10, lng=lng, doors=[CLOSE if not data else None]))
                lng += 0.001
        return data

    data = days([130, 130, 130, 20])                                          # 14.4 km a day, H4 20 records
    open_sec = data[-1]["sec"] + 5
    confirm = dict(data[-1], sec=open_sec + 5, doors=[None])                  # next location confirms the open
    assert spans(door_health(data + [confirm], on_change=True, extra_doors=[(open_sec, OPEN)])) == [
        ("DOOR-04", data[0]["sec"], open_sec)]
    short = days([130, 50, 130, 20])                                          # H2 5.5 km: not a day
    confirm = dict(short[-1], sec=short[-1]["sec"] + 10, doors=[None])
    assert door_health(short + [confirm], on_change=True, extra_doors=[(short[-1]["sec"] + 5, OPEN)]) == []


def test_received_on_packet_time_or_send_time():
    at = datetime(2026, 11, 19, 1, 2, 3, tzinfo=timezone.utc)
    assert received_time("packet", at) == "2026-11-19T01:02:03.000Z"
    assert data_values("1", "concox", "gt06n", "aa", received_time("packet", at))["received_on"] == \
        "2026-11-19T01:02:03.000Z"
    assert received_time("send", at) is None
    assert data_values("1", "concox", "gt06n", "aa")["received_on"].startswith(f"{datetime.now(timezone.utc):%Y-%m-%d}")
