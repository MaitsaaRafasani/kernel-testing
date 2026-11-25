"""door_concox: door alerts of a concox, door1 digital (0x94 IO detection) + door2 analog (0x94 external voltage).

python cli.py generate --usecase door_concox
A distribution truck (concox gt06n) drives routes/health_loop.json: H1 00:00 - 05:40 deliveries, then one trip a
day H2 - H5 and the depot until H6 09:00 (DOOR-04 needs 3 days). Door data = own 0x94 packets (timeline
`packet: sensor`) between the location packets:
- door1: pin DIGITAL_1, `door1: open / closed` (0x94 type 05, IO detection), sent on change, alert_min_time 15 min
- door2: pin ANALOG_1, `analog_input_1: <mV>` (0x94 type 00, kernel volt = mV / 1000), open when >= 5 V,
  sent every 10 s on H1 (concox analog upload), from H1 06:00 5 s after every location, alert_min_time 10 minutes
0x94 has no time: the kernel uses the gate received_on, so the case sends received_on = packet time.
Expected = simulated kernel DoorSensor (generators/door_common.py): raw_alert + vehicle_status at the step where
the kernel creates each alert, glitches (no alert) at the next location step, totals at step `end`
+ DeviceHealth DOOR / GPS (generators/health_common.py: summary_device_error at step `end`).
"""
from generators.door_common import (
    DAYS_DOOR1, DAYS_DOOR2, DAYS_END, DAYS_OFF, OPEN, OPEN_TOO_LONG, DoorAlerts, DoorMessage,
    add_health_status, alert_steps, analog_value, build_checks, check_simulation, db_requirements, describe_cases,
    digital_value, simulate_door_health, step_name,
)
from generators.health_common import (
    Odometer, drive_speed, fmt, health_checks, is_in, m, satellites, schedule, simulate_gps, to_case_timeline,
)

USECASE = "door_concox"
FAMILY = "concox"
MODEL = "gt06n"
IMEI = "358000000000011"
TOTAL = DAYS_END                        # H6 09:00
MIN_TIME = {1: 15, 2: 10}               # sensor alert_min_time (minutes)

# acc off (parked): a location record every 5 min
OFF = [
    (m(0, 30), m(0, 55)),               # stop 1
    (m(1, 40), m(2, 30)),               # loading
    (m(3, 10), m(3, 40)),               # stop 2
    (m(4, 20), m(4, 50)),               # stop 3
    *DAYS_OFF,                          # depot from 05:40, then one trip a day H2 - H5
]
OFFLINE = [(m(2, 40), m(2, 52))]        # L  no signal while driving: no location, no analog sample

# door1 changes (second, open / closed): sent on change. Off the location (x0 s) and analog (x5 s) seconds.
DOOR1_EVENTS = [
    (m(0, 0, 3), "closed"),                                                    # first door1 data after reset
    (m(0, 31, 20), "open"), (m(0, 42, 40), "closed"),                          # A
    (m(1, 5, 21), "open"), (m(1, 5, 23), "closed"),                            # C  glitch 2 s
    (m(1, 30, 12), "open"), (m(1, 30, 16), "closed"),                          # E  4 s
    (m(1, 42, 30), "open"), (m(2, 21, 50), "closed"),                          # F  too long by location
    (m(4, 21, 30), "open"), (m(4, 27, 4), "closed"), (m(4, 27, 6), "open"),
    (m(4, 47, 30), "closed"),                                                  # I  closed 2 s while open
    (m(5, 41, 14), "open"), (m(5, 52, 40), "closed"),                          # K
    *DAYS_DOOR1,                                                               # M O Q S (H2 - H6)
]

# door2 analog sample every 10 s from ANALOG_START until ANALOG_UNTIL (H1), then 5 s after every location (10 s
# driving, 5 min parked: H2 - H6 stay small). Set values (second, mV); the samples between keep the state with a
# little noise (closed 0.02-0.05 V, open 12.28-12.32 V)
ANALOG_START, ANALOG_UNTIL, ANALOG_EVERY = m(0, 0, 5), m(6), 10
DOOR2_VALUES = [
    (m(0, 0, 5), 30),                                                          # first sample after reset
    (m(0, 33, 45), 12310), (m(0, 38, 5), 20),                                  # B
    (m(1, 20, 35), 5120), (m(1, 20, 45), 40),                                  # D  open 1 sample
    (m(1, 44, 15), 12310), (m(2, 5, 25), 20),                                  # G
    (m(3, 12, 15), 4990), (m(3, 13, 45), 5000), (m(3, 19, 15), 50),            # H  analog threshold
    (m(5, 5, 15), 12400), (m(5, 21, 5), 30),                                   # J
    *[(sec, 12310 if value == "open" else 20) for sec, value in DAYS_DOOR2],   # N P R T
]

# (second, door, value, reason): door packet without state change, checked as "no alert" at the next location step
NO_ALERT = [(m(3, 12, 15), 2, OPEN, "door2 4.99 V < 5 V tetap closed")]

GLITCH = "tidak ada (glitch <= 3 detik)"
# (label, what happens, alert result, health note or None). Health summaries of the case are added from HEALTH.
CASES = [
    ("Awal", "Data pertama: door1 closed 00:00:03, door2 0.03 V 00:00:05", "close door1 00:00:03, close door2 00:00:05",
     "tidak ada"),
    ("A", "Door 1 (digital_1), normal open close, kendaraan parkir", "open 00:31:20, close 00:42:40", "tidak ada"),
    ("B", "Door 2 (analog_1), normal open close 12.31 V / 0.02 V, kendaraan parkir", "open 00:33:45, close 00:38:05",
     "tidak ada"),
    ("C", "Door1 open 01:05:21, close 01:05:23 dalam 2 detik, kendaraan jalan", GLITCH,
     "tidak ada: glitch bukan perubahan valid, tidak dihitung DOOR-03"),
    ("D", "Door2 open 01:20:35, close 01:20:45 (1 sampel 10 detik), kendaraan jalan", "open 01:20:35, close 01:20:45",
     None),
    ("E", "Door1 open 01:30:12, close 01:30:16 dalam 4 detik, kendaraan jalan", "open 01:30:12, close 01:30:16",
     None),
    ("F", "Door1 open 01:42:30, close 02:21:50 (39 menit), kendaraan parkir",
     "open 01:42:30, open too long 02:00:00, close 02:21:50", "tidak ada"),
    ("G", "Door2 open 01:44:15, close 02:05:25 (21 menit), kendaraan parkir",
     "open 01:44:15, open too long 01:54:15, close 02:05:25", "tidak ada"),
    ("H", "Door2 4.99 V 03:12:15, 5.00 V 03:13:45, 0.05 V 03:19:15, kendaraan parkir",
     "4.99 V tidak ada (< 5 V), open 03:13:45, close 03:19:15", "tidak ada"),
    ("I", "Door1 open 04:21:30, close 04:27:04 dalam 2 detik, close 04:47:30, kendaraan parkir",
     "open 04:21:30, open too long 04:40:00, close 04:47:30", "tidak ada"),
    ("J", "Door2 open 05:05:15, close 05:21:05 (16 menit), kendaraan jalan",
     "open 05:05:15, open too long 05:15:15, close 05:21:05", None),
    ("K", "Door1 open 05:41:14, close 05:52:40 (11 menit), kendaraan parkir", "open 05:41:14, close 05:52:40",
     "tidak ada"),
    ("L", "Sinyal hilang 02:40:00 - 02:52:00, door tertutup, kendaraan jalan", "tidak ada",
     "door1 bukan DOOR-01 (dikirim saat berubah), belum GPS-01 (< 16 menit)"),
    ("M", "H2 door1 glitch 4x (08:05:21 - 08:06:53, tiap glitch 2 detik), kendaraan jalan < 10 km", GLITCH,
     "tidak ada: glitch tidak dihitung DOOR-03"),
    ("N", "H2 door2 open close 4x (08:10:05 - 08:11:45, tiap open 10 detik), kendaraan jalan < 10 km",
     "open 4x, close 4x", None),
    ("O", "H2 door1 glitch 4x (14:00:21 - 14:03:23, tiap glitch 2 detik), kendaraan parkir mesin mati", GLITCH,
     "tidak ada"),
    ("P", "H2 door2 open close 4x (15:00:05 - 15:35:05, tiap open 5 menit), kendaraan parkir mesin mati",
     "open 4x, close 4x", "tidak ada: parkir tidak dihitung DOOR-03"),
    ("Q", "H3 door1 glitch 4x (08:04:21 - 08:24:23, tiap glitch 2 detik), kendaraan jalan 12.9 km (> 10 km)", GLITCH,
     "tidak ada"),
    ("R", "H3 door2 open close 4x (08:03:05 - 08:23:15, tiap open 10 detik), kendaraan jalan 13.2 km (> 10 km)",
     "open 4x, close 4x", None),
    ("S", "Door1 closed 3 hari jalan (H2 32 km, H3 38 km, H4 29 km), open H5 08:30:15, close H5 08:40:15",
     "open H5 08:30:15, close H5 08:40:15", None),
    ("T", "Door2 closed 3 hari jalan (H3 23 km, H4 29 km, H5 29 km), open H6 08:30:05, close H6 08:35:05",
     "open H6 08:30:05, close H6 08:35:05", None),
]

# device health summaries the timeline produces: (case, label, description, start, end, explanation)
HEALTH = [
    ("D", "DOOR-03", "door-2", m(1, 20, 35), m(1, 20, 45), "2 perubahan valid saat jalan"),
    ("E", "DOOR-03", "door-1", m(1, 30, 12), m(2, 21, 50), "2 perubahan valid saat jalan, end = close F"),
    ("L", "DOOR-01", "door-2", m(2, 39, 55), m(2, 52, 5), "sampel door2 hilang 12 menit"),
    ("J", "DOOR-02", "door-2", m(5, 5, 15), m(5, 21, 5), "terbuka 10.1 km saat jalan"),
    ("N", "DOOR-03", "door-2", m(8, 10, 5, day=1), m(8, 11, 45, day=1), "8 perubahan dalam 10 km"),
    ("R", "DOOR-03", "door-2", m(8, 3, 5, day=2), m(8, 13, 15, day=2), "6 perubahan dalam 10 km pertama"),
    ("R", "DOOR-03", "door-2", m(8, 23, 5, day=2), m(8, 23, 15, day=2), "2 perubahan dalam 10 km berikutnya"),
    ("S", "DOOR-04", "door-1", m(5, 52, 40), m(8, 30, 15, day=4), "aktif mulai H5 00:00:00"),
    ("T", "DOOR-04", "door-2", m(8, 23, 15, day=2), m(8, 30, 5, day=5), "aktif mulai H6 00:00:00"),
]

SENSORS = [{"code": "door1", "pin": "digital_1"}, {"code": "door2", "pin": "analog_1"}]
DB_REQUIREMENTS = db_requirements([(FAMILY, MODEL, IMEI)], SENSORS, MIN_TIME)

# raw_alert rows per alert code the cases must produce
EXPECTED_COUNTS = {4: 6, 19: 7, 5: 18, 20: 19, OPEN_TOO_LONG: 4}
EXPECTED_TOO_LONG = [(2, m(1, 54, 15)), (1, m(2)), (1, m(4, 40)), (2, m(5, 15, 15))]       # (door, time)
EXPECTED_GLITCHES = 14


def door_value(door: int, value) -> int:
    """Kernel door value of the packet: door1 digital, door2 analog volt (0x94 type 00 = 0.01 V)."""
    if door == 1:
        return digital_value(1 if value == "open" else 0)
    return analog_value(round(value / 10) / 100)


def location_records() -> list[dict]:
    odometer, rows = Odometer(), []
    for sec, acc_on in schedule(TOTAL, OFF):
        speed = drive_speed(sec) if acc_on else 0
        lat, lng = odometer.advance(sec, speed, acc_on)
        if not is_in(sec, OFFLINE):
            rows.append({"sec": sec, "lat": lat, "lng": lng, "speed": speed, "sats": satellites(sec), "acc": acc_on})
    return rows


def door_row(sec: int, door: int, value) -> dict:
    io = {"door1": value} if door == 1 else {"analog_input_1": value}
    return {"sec": sec, "packet": "sensor", "door": door, "value": door_value(door, value), "io": io}


def door1_records() -> list[dict]:
    """IO detection is sent only on change: no same value twice in a row."""
    rows, last = [], None
    for sec, value in DOOR1_EVENTS:
        if is_in(sec, OFFLINE):
            raise ValueError(f"door1 change while the device is offline: {fmt(sec)}")
        if value == last:
            raise ValueError(f"door1 {value} twice in a row at {fmt(sec)}: IO detection is sent on change")
        last = value
        rows.append(door_row(sec, 1, value))
    return rows


def door2_records(locations: list[dict]) -> list[dict]:
    """Analog samples before the last location: DOOR2_VALUES at their second, else the current state + noise."""
    until = locations[-1]["sec"]
    seconds = [*range(ANALOG_START, ANALOG_UNTIL, ANALOG_EVERY),
               *(location["sec"] + 5 for location in locations if ANALOG_UNTIL <= location["sec"] < until)]
    values = dict(DOOR2_VALUES)
    missing = sorted(set(values) - set(seconds))
    if missing:
        raise ValueError(f"door2 value not on an analog sample second: {[fmt(sec) for sec in missing]}")
    rows, is_open = [], False
    for index, sec in enumerate(seconds):
        if is_in(sec, OFFLINE):
            continue
        if sec in values:
            value = values[sec]
            is_open = door_value(2, value) == OPEN
        else:
            value = 12280 + index * 7 % 5 * 10 if is_open else 20 + index * 3 % 4 * 10
        rows.append(door_row(sec, 2, value))
    return rows


def simulate(records: list[dict]) -> tuple[DoorAlerts, dict]:
    """Kernel door alerts, one gate message per record. Return the simulator + vehicle_status after each message."""
    sim, states = DoorAlerts(MIN_TIME), {}
    for record in records:
        if record.get("packet") == "sensor":
            sim.door(record["sec"], record["door"], record["value"])
        else:
            sim.location(record["sec"])
        states[record["sec"]] = (dict(sim.status), dict(sim.last_alert))
    return sim, states


def health(records: list[dict], locations: list[dict]) -> tuple[list, dict]:
    """DeviceHealth DOOR + GPS summaries, and the door error status after each packet {second: (door-1, door-2)}.
    door1 0x94 IO detection is on change, door2 0x94 external voltage is not (family/concox: on_change only for
    IO_DETECTION)."""
    messages = [
        DoorMessage(record["sec"], [(record["door"], record["value"], record["door"] == 1)])
        if record.get("packet") == "sensor" else DoorMessage(record["sec"], [], record)
        for record in records
    ]
    summaries, statuses = simulate_door_health(messages, 2, MIN_TIME)
    return summaries + simulate_gps(locations), statuses


def verify_health(summaries: list):
    actual = [(item.label, item.description, item.start, item.end) for item in summaries]
    if actual != [item[1:5] for item in HEALTH]:
        found = [(label, description, fmt(start), fmt(end)) for label, description, start, end in actual]
        raise ValueError(f"device health changed: {found}")


def build() -> tuple[dict, dict]:
    locations = location_records()
    records = sorted(locations + door1_records() + door2_records(locations), key=lambda row: row["sec"])
    seconds = [record["sec"] for record in records]
    if len(seconds) != len(set(seconds)):
        raise ValueError("two packets on the same second")
    sim, states = simulate(records)
    check_simulation(sim, EXPECTED_COUNTS, EXPECTED_TOO_LONG, EXPECTED_GLITCHES)
    summaries, statuses = health(records, locations)
    verify_health(summaries)
    last_sec = seconds[-1]
    checks = add_health_status(
        build_checks(sim, states, [location["sec"] for location in locations], NO_ALERT), statuses, last_sec)

    for record in records:
        if record["sec"] in checks or record["sec"] == last_sec:
            record["step"] = step_name(record["sec"], last_sec)
    steps = alert_steps(checks, last_sec, EXPECTED_COUNTS, health_checks(summaries))

    case = {
        "usecase": USECASE,
        "description": describe_cases(CASES, HEALTH),
        "db_requirements": DB_REQUIREMENTS,
        "families": [FAMILY],
        "start": "today-6d",
        "interval": "00:00:10",
        "tags": ["door"],
        "imei": {FAMILY: IMEI},
        "models": {FAMILY: MODEL},
        "carry_over": False,
        "received_on": "packet",
        "setup": {"sensors": SENSORS},
        "timeline": to_case_timeline(records, altitude=None),
    }
    return case, {USECASE: {"default": {"steps": steps}}}
