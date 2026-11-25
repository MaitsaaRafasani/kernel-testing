"""door: door alerts + door device health for devices with wired door inputs (teltonika, ruptela, queclink).

python cli.py generate --usecase door
A distribution truck drives routes/health_loop.json: H1 00:00 - 05:40 deliveries, then one trip a day
H2 - H5 and the depot until H6 09:00 (DOOR-04 needs 3 days). Every location record carries the door inputs
(monitoring: 10 s with acc on, 5 min with acc off), and the device sends an extra record at the moment a door
changes (event):
- door1: `door1` digital (pin DIGITAL_2), alert_min_time 15 minutes
- door2: `door2` digital (pin DIGITAL_3), no alert_min_time (no open too long)
Kernel order in a record: door packets, then the location (all three families).
Expected = simulated kernel DoorSensor (generators/door_common.py: raw_alert + vehicle_status at the step where the
kernel creates each alert, glitches at the next record, totals at step `end`) + DeviceHealth DOOR / GPS
(generators/health_common.py: summary_device_error at step `end`).
Topflytech has no wired door input (BLE door only), concox: generators/door_concox.py.
"""
from generators.door_common import (
    DAYS_DOOR1, DAYS_DOOR2, DAYS_END, DAYS_OFF, OPEN_TOO_LONG, DoorAlerts, DoorMessage,
    add_health_status, alert_steps, build_checks, check_simulation, db_requirements, describe_cases, digital_value,
    simulate_door_health, step_name,
)
from generators.health_common import (
    Odometer, drive_speed, fmt, health_checks, is_in, m, satellites, schedule, simulate_gps, to_case_timeline,
)

USECASE = "door"
FAMILIES = ["teltonika", "ruptela", "queclink"]
IMEI = {"teltonika": "350000000000016", "ruptela": "352000000000016", "queclink": "353000000000016"}
MODELS = {"teltonika": "fmb125", "ruptela": "fm_pro4", "queclink": "gv58cg"}
TOTAL = DAYS_END                        # H6 09:00
MIN_TIME = {1: 15, 2: None}             # sensor alert_min_time (minutes)

# acc off (parked): a record every 5 min
OFF = [
    (m(0, 30), m(0, 55)),               # stop 1
    (m(1, 40), m(2, 30)),               # loading
    (m(3, 10), m(3, 40)),               # stop 2
    (m(4, 20), m(4, 50)),               # stop 3
    *DAYS_OFF,                          # depot from 05:40, then one trip a day H2 - H5
]
OFFLINE = [(m(2, 40), m(3))]            # H  no signal while driving: no record at all

# (second, door, open / closed). Each one = event record at that second, except inside OFFLINE: the state changes
# silently and the first record after it carries the new value.
DOOR_EVENTS = [
    (m(0, 31, 20), 1, "open"), (m(0, 42, 40), 1, "closed"),                    # A
    (m(0, 33, 45), 2, "open"), (m(0, 52, 5), 2, "closed"),                     # B
    (m(1, 5, 21), 1, "open"), (m(1, 5, 23), 1, "closed"),                      # C  glitch 2 s
    (m(1, 12, 28), 1, "open"), (m(1, 12, 31), 1, "closed"),                    # D  glitch 3 s over a record
    (m(1, 30, 12), 1, "open"), (m(1, 30, 16), 1, "closed"),                    # E  4 s
    (m(1, 42, 30), 1, "open"), (m(2, 21, 50), 1, "closed"),                    # F  too long by a door2 event
    (m(1, 57, 40), 2, "open"), (m(2, 10, 15), 2, "closed"),                    # G
    (m(2, 50), 1, "open"), (m(3, 10, 45), 1, "closed"),                        # H  opened while offline
    (m(3, 12, 1), 2, "open"), (m(3, 12, 3), 2, "closed"), (m(3, 12, 5), 2, "open"),
    (m(3, 30, 10), 2, "closed"),                                               # I  open, closed, open
    (m(3, 41, 15), 2, "open"), (m(4, 5, 25), 2, "closed"),                     # J  door2 open while driving
    (m(4, 21, 30), 1, "open"), (m(4, 27, 4), 1, "closed"), (m(4, 27, 6), 1, "open"),
    (m(4, 47, 30), 1, "closed"),                                               # K  closed 2 s while open
    (m(5, 5, 15), 1, "open"), (m(5, 25, 5), 1, "closed"),                      # L  door1 open while driving
    (m(5, 41, 14), 1, "open"), (m(5, 52, 40), 1, "closed"),                    # M
    *[(sec, 1, value) for sec, value in DAYS_DOOR1],                           # N P R T (H2 - H5)
    *[(sec, 2, value) for sec, value in DAYS_DOOR2],                           # O Q S U
]

GLITCH = "tidak ada (glitch <= 3 detik)"
# (label, what happens, alert result, health note or None). Health summaries of the case are added from HEALTH.
CASES = [
    ("Awal", "Record pertama 00:00:00, semua door closed", "close door1 + door2 00:00:00", "tidak ada"),
    ("A", "Door 1 (digital_2), normal open close, kendaraan parkir", "open 00:31:20, close 00:42:40", "tidak ada"),
    ("B", "Door 2 (digital_3), normal open close (18 menit), kendaraan parkir", "open 00:33:45, close 00:52:05",
     "tidak ada"),
    ("C", "Door1 open 01:05:21, close 01:05:23 dalam 2 detik, kendaraan jalan", GLITCH,
     "tidak ada: glitch bukan perubahan valid, tidak dihitung DOOR-03"),
    ("D", "Door1 open 01:12:28, close 01:12:31 dalam 3 detik, kendaraan jalan", GLITCH,
     "tidak ada: C + D = 4 perubahan mentah, tapi tidak ada yang valid"),
    ("E", "Door1 open 01:30:12, close 01:30:16 dalam 4 detik, kendaraan jalan", "open 01:30:12, close 01:30:16",
     None),
    ("F", "Door1 open 01:42:30, close 02:21:50 (39 menit), kendaraan parkir",
     "open 01:42:30, open too long 01:57:40, close 02:21:50", "tidak ada"),
    ("G", "Door2 open 01:57:40, close 02:10:15, kendaraan parkir", "open 01:57:40, close 02:10:15", "tidak ada"),
    ("H", "Sinyal hilang 02:40:00 - 03:00:00, door1 open saat offline, close 03:10:45, kendaraan jalan",
     "open 03:00:00 (record pertama setelah online), close 03:10:45", None),
    ("I", "Door2 open 03:12:01, close 03:12:03, open 03:12:05, close 03:30:10, kendaraan parkir",
     "open 03:12:05, close 03:30:10 (03:12:01 glitch)", "tidak ada"),
    ("J", "Door2 open 03:41:15, close 04:05:25 (24 menit), kendaraan jalan", "open 03:41:15, close 04:05:25", None),
    ("K", "Door1 open 04:21:30, close 04:27:04 dalam 2 detik, close 04:47:30, kendaraan parkir",
     "open 04:21:30, open too long 04:40:00, close 04:47:30", "tidak ada"),
    ("L", "Door1 open 05:05:15, close 05:25:05 (20 menit), kendaraan jalan",
     "open 05:05:15, open too long 05:20:20, close 05:25:05", None),
    ("M", "Door1 open 05:41:14, close 05:52:40 (11 menit), kendaraan parkir", "open 05:41:14, close 05:52:40",
     "tidak ada"),
    ("N", "H2 door1 glitch 4x (08:05:21 - 08:06:53, tiap glitch 2 detik), kendaraan jalan < 10 km", GLITCH,
     "tidak ada: glitch tidak dihitung DOOR-03"),
    ("O", "H2 door2 open close 4x (08:10:05 - 08:11:45, tiap open 10 detik), kendaraan jalan < 10 km",
     "open 4x, close 4x", None),
    ("P", "H2 door1 glitch 4x (14:00:21 - 14:03:23, tiap glitch 2 detik), kendaraan parkir mesin mati", GLITCH,
     "tidak ada"),
    ("Q", "H2 door2 open close 4x (15:00:05 - 15:35:05, tiap open 5 menit), kendaraan parkir mesin mati",
     "open 4x, close 4x", "tidak ada: parkir tidak dihitung DOOR-03"),
    ("R", "H3 door1 glitch 4x (08:04:21 - 08:24:23, tiap glitch 2 detik), kendaraan jalan 12.9 km (> 10 km)", GLITCH,
     "tidak ada"),
    ("S", "H3 door2 open close 4x (08:03:05 - 08:23:15, tiap open 10 detik), kendaraan jalan 13.2 km (> 10 km)",
     "open 4x, close 4x", None),
    ("T", "Door1 closed 3 hari jalan (H2 32 km, H3 38 km, H4 29 km), open H5 08:30:15, close H5 08:40:15",
     "open H5 08:30:15, close H5 08:40:15", None),
    ("U", "Door2 closed 3 hari jalan (H3 23 km, H4 29 km, H5 29 km), open H6 08:30:05, close H6 08:35:05",
     "open H6 08:30:05, close H6 08:35:05", None),
]

# device health summaries the timeline produces: (case, label, description, start, end, explanation)
HEALTH = [
    ("E", "DOOR-03", "door-1", m(1, 30, 12), m(2, 21, 50), "2 perubahan valid saat jalan, end = close F"),
    ("H", "DOOR-01", "door-1", m(2, 39, 50), m(3), "tidak ada data door 20 menit"),
    ("H", "DOOR-01", "door-2", m(2, 39, 50), m(3), None),
    ("J", "DOOR-02", "door-2", m(3, 41, 15), m(4, 5, 25), "terbuka 15.8 km saat jalan"),
    ("L", "DOOR-02", "door-1", m(5, 5, 15), m(5, 25, 5), "terbuka 12.5 km saat jalan"),
    ("O", "DOOR-03", "door-2", m(8, 10, 5, day=1), m(8, 11, 45, day=1), "8 perubahan dalam 10 km"),
    ("S", "DOOR-03", "door-2", m(8, 3, 5, day=2), m(8, 13, 15, day=2), "6 perubahan dalam 10 km pertama"),
    ("S", "DOOR-03", "door-2", m(8, 23, 5, day=2), m(8, 23, 15, day=2), "2 perubahan dalam 10 km berikutnya"),
    ("T", "DOOR-04", "door-1", m(5, 52, 40), m(8, 30, 15, day=4), "aktif mulai H5 00:00:00"),
    ("U", "DOOR-04", "door-2", m(8, 23, 15, day=2), m(8, 30, 5, day=5), "aktif mulai H6 00:00:00"),
    ("H", "GPS-01", None, m(2, 39, 50), m(3), "tidak ada lokasi 20 menit"),
]

SENSORS = [{"code": "door1", "pin": "digital_2"}, {"code": "door2", "pin": "digital_3"}]
DB_REQUIREMENTS = db_requirements([(family, MODELS[family], IMEI[family]) for family in FAMILIES], SENSORS, MIN_TIME)

# raw_alert rows per alert code the cases must produce
EXPECTED_COUNTS = {4: 8, 19: 9, 5: 17, 20: 18, OPEN_TOO_LONG: 3}
EXPECTED_TOO_LONG = [(1, m(1, 57, 40)), (1, m(4, 40)), (1, m(5, 20, 20))]       # (door, time)
EXPECTED_GLITCHES = 16


def door_value(value: str) -> int:
    """Kernel door value of a digital input."""
    return digital_value(1 if value == "open" else 0)


def records() -> list[dict]:
    """Monitoring records + event records, door values = state at that second."""
    events = {}
    for sec, door, value in DOOR_EVENTS:
        events.setdefault(sec, []).append((door, value))
    grid = {sec: acc_on for sec, acc_on in schedule(TOTAL, OFF)}
    clash = sorted(set(grid) & {sec for sec in events if not is_in(sec, OFFLINE)})
    if clash:
        raise ValueError(f"door event on a monitoring record second: {[fmt(sec) for sec in clash]}")

    odometer, rows = Odometer(), []
    state = {1: "closed", 2: "closed"}
    for sec in sorted(set(grid) | set(events)):
        acc_on = not is_in(sec, OFF)
        speed = drive_speed(sec) if acc_on else 0
        lat, lng = odometer.advance(sec, speed, acc_on)
        state.update(events.get(sec, []))
        if is_in(sec, OFFLINE):
            continue
        rows.append({
            "sec": sec, "lat": lat, "lng": lng, "speed": speed, "sats": satellites(sec), "acc": acc_on,
            "event": sec in events, "doors": {door: door_value(value) for door, value in state.items()},
            "io": {f"door{door}": value for door, value in state.items()},
        })
    return rows


def simulate(rows: list[dict]) -> tuple[DoorAlerts, dict]:
    """Kernel door alerts, one gate message per record. Return the simulator + vehicle_status after each record."""
    sim, states = DoorAlerts(MIN_TIME), {}
    for row in rows:
        sim.record(row["sec"], row["doors"])
        states[row["sec"]] = (dict(sim.status), dict(sim.last_alert))
    return sim, states


def health(rows: list[dict]) -> tuple[list, dict]:
    """DeviceHealth DOOR (door packets before the location, not on change, only valid changes) + GPS summaries,
    and the door error status after each record {second: (door-1, door-2)}."""
    messages = [DoorMessage(row["sec"], [(door, value, False) for door, value in row["doors"].items()], row)
                for row in rows]
    summaries, statuses = simulate_door_health(messages, 2, MIN_TIME)
    return summaries + simulate_gps(rows), statuses


def verify_health(summaries: list):
    actual = [(item.label, item.description, item.start, item.end) for item in summaries]
    if actual != [item[1:5] for item in HEALTH]:
        found = [(label, description, fmt(start), fmt(end)) for label, description, start, end in actual]
        raise ValueError(f"device health changed: {found}")


def build() -> tuple[dict, dict]:
    rows = records()
    sim, states = simulate(rows)
    check_simulation(sim, EXPECTED_COUNTS, EXPECTED_TOO_LONG, EXPECTED_GLITCHES)
    summaries, statuses = health(rows)
    verify_health(summaries)
    last_sec = rows[-1]["sec"]
    checks = add_health_status(build_checks(sim, states, [row["sec"] for row in rows]), statuses, last_sec)

    for row in rows:
        if row["sec"] in checks or row["sec"] == last_sec:
            row["step"] = step_name(row["sec"], last_sec)
    steps = alert_steps(checks, last_sec, EXPECTED_COUNTS, health_checks(summaries))

    case = {
        "usecase": USECASE,
        "description": describe_cases(CASES, HEALTH),
        "db_requirements": DB_REQUIREMENTS,
        "families": FAMILIES,
        "start": "today-6d",
        "interval": "00:00:10",
        "tags": ["door"],
        "imei": IMEI,
        "models": MODELS,
        "carry_over": False,
        "setup": {"sensors": SENSORS},
        "timeline": to_case_timeline(rows),
    }
    return case, {USECASE: {"default": {"steps": steps}}}
