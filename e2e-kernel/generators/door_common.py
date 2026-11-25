"""Kernel v2 door alerts (usecase/sensor/door/door.py DoorSensor), simulated per gate message, + the expected
checks of the door usecases (raw_alert, vehicle_status, device_health door_N_error_status).

A gate message = its door packets (in order), its location packet (vehicle_status.last_packet), then
Sensor.sensor_staged_packet for DOOR1 / DOOR2 only (DOOR3 / DOOR4 are not staged):
- a door change waits (pending) until a later packet more than DOOR_TIMEOUT (3 s) after it: the next door packet of
  that door, or (door1 / door2) the end of a message with a newer location. Then alert DOORnOPEN / DOORnCLOSE with the
  change time, vehicle_status doorN_status / last_doorN_alert.
- a change back within 3 s (<= 3 s) is a glitch: no alert. Glitch while closed resets the open-too-long start,
  glitch while open keeps it.
- open too long: sensor alert_min_time (minutes) from the open change, checked on every door packet of that door
  and (door1 / door2) at the end of every message (time = last location). Alert time = that time,
  alert_data = alert_min_time, alert_trigger = door sensor code. Once per opening (reset on close or after the alert).
Values = kernel DoorCodes: OPEN 0, CLOSE 1 (vehicle_status doorN_status, raw_alert alert_data).
Sensor status_bool (bypass) and status_text 'reversed' are not set.
"""
from typing import NamedTuple

from generators.health_common import DAY, DoorSimulator, Summary, fmt, m
from versions.v2 import setup as v2_setup

OPEN, CLOSE = 0, 1                  # DoorCodes
DOOR_TIMEOUT = 3                    # seconds, Constants.DOOR_TIMEOUT
ANALOG_OPEN_VOLT = 5                # analog pin: open when value >= 5
STAGED_DOORS = (1, 2)               # Sensor.sensor_staged_packet
SENSOR_CODES = {1: 4, 2: 5, 3: 66}  # SensorCodes DOOR1..3 (= raw_alert alert_trigger of open too long)
OPEN_ALERTS = {1: 4, 2: 5, 3: 144}  # AlertCodes DOOR1OPEN..DOOR3OPEN
CLOSE_ALERTS = {1: 19, 2: 20, 3: 145}
OPEN_TOO_LONG = 35                  # AlertCodes DOOR_OPEN_TOO_LONG
ALERT_NAMES = {
    4: "DOOR1OPEN", 5: "DOOR2OPEN", 144: "DOOR3OPEN", 19: "DOOR1CLOSE", 20: "DOOR2CLOSE", 145: "DOOR3CLOSE",
    35: "DOOR_OPEN_TOO_LONG",
}
END_STEP = "end"

# ---------------- days H2 - H6 (door, door_concox) ----------------
# H1 ends parked at the depot (05:40). Then one trip a day H2 - H5 (> 10 km), parked the rest of the day.
# DOOR-04 day = local date of the location packet time (kernel dc0e9eea5).
# door1 stays closed from its last H1 change (only glitches): DOOR-04 from H5 00:00, ends H5 08:30:15.
# door2 stays closed from H3 08:23:15: DOOR-04 from H6 00:00, ends H6 08:30:05.
DAYS_END = m(9, day=5)                                  # H6 09:00
DAYS_OFF = [(m(5, 40), m(8, day=1)), (m(8, 50, day=1), m(8, day=2)), (m(9, day=2), m(8, day=3)),
            (m(8, 45, day=3), m(9, day=4)), (m(9, 45, day=4), DAYS_END)]


def cycles(opens: list[int], open_for: int) -> list[tuple[int, str]]:
    """Open at each second, closed `open_for` seconds later."""
    return [event for sec in opens for event in ((sec, "open"), (sec + open_for, "closed"))]


DAYS_DOOR1 = [                  # door1 digital (door: event record, concox: 0x94 IO detection)
    *cycles([m(8, 5, 21, day=1), m(8, 5, 51, day=1), m(8, 6, 21, day=1), m(8, 6, 51, day=1)], 2),     # glitch, < 10 km
    *cycles([m(14, 0, 21, day=1), m(14, 1, 21, day=1), m(14, 2, 21, day=1), m(14, 3, 21, day=1)], 2),  # glitch, parked
    *cycles([m(8, 4, 21, day=2), m(8, 9, 21, day=2), m(8, 14, 21, day=2), m(8, 24, 21, day=2)], 2),   # glitch, > 10 km
    (m(8, 30, 15, day=4), "open"), (m(8, 40, 15, day=4), "closed"),                                    # ends DOOR-04
]
DAYS_DOOR2 = [                  # door2 (door: event record, concox: analog sample 5 s after a location)
    *cycles([m(8, 10, 5, day=1), m(8, 10, 35, day=1), m(8, 11, 5, day=1), m(8, 11, 35, day=1)], 10),  # < 10 km
    *cycles([m(15, 0, 5, day=1), m(15, 10, 5, day=1), m(15, 20, 5, day=1), m(15, 30, 5, day=1)], 300),  # parked
    *cycles([m(8, 3, 5, day=2), m(8, 8, 5, day=2), m(8, 13, 5, day=2), m(8, 23, 5, day=2)], 10),      # > 10 km
    (m(8, 30, 5, day=5), "open"), (m(8, 35, 5, day=5), "closed"),                                      # ends DOOR-04
]


class Alert(NamedTuple):
    """One raw_alert row (seconds from the timeline start)."""
    code: int                       # alert_code
    door: int                       # 1..3
    time: int                       # created_on
    created: int                    # message that makes the kernel insert it
    data: int                       # alert_data: door value, or alert_min_time (open too long)
    trigger: int | None = None      # alert_trigger: door sensor code (open too long)


class Glitch(NamedTuple):
    """A change back within DOOR_TIMEOUT: no alert."""
    door: int
    value: int                      # value of the ignored change
    start: int
    end: int


def digital_value(value: int) -> int:
    """Digital pin: 1 = open (DoorSensor._determine_door_value)."""
    return OPEN if value == 1 else CLOSE


def analog_value(volt: float) -> int:
    return OPEN if volt >= ANALOG_OPEN_VOLT else CLOSE


def db_requirements(devices: list[tuple], sensors: list[dict], min_time: dict) -> list[str]:
    """Short DB requirements: devices (family, model, imei), sensor speed, door sensors (setup code + pin)."""
    lines = [f"device {family}, {model} -> imei {imei}" for family, model, imei in devices]
    lines.append(f"sensor speed (sensor_code {v2_setup.SENSOR_CODES['speed']})")
    for sensor in sensors:
        code, pin = sensor["code"], sensor["pin"]
        lines.append(f"sensor {code} (sensor_code {v2_setup.SENSOR_CODES[code]}, pin_code {v2_setup.PIN_CODES[pin]} "
                     f"({pin}), alert_min_time {min_time.get(int(code[-1])) or 'kosong'})")
    return lines


def describe_cases(cases: list[tuple], health: list[tuple]) -> str:
    """Case description: per case what happens, then the alert, device health and other DB results.
    cases: (label, what, alert, health note or None[, db note]). health: (case label, code, channel or None, start,
    end, why)."""
    lines = ["Kasus:"]
    for label, what, alert, health_note, *db in cases:
        lines += [f"  {label:<5} {what}", f"        -> alert : {alert}"]
        for case, code, door, start, end, why in health:
            if case == label:
                name = " ".join(filter(None, (code, door)))
                start_at, end_at = (fmt(start)[3:], fmt(end)[3:]) if end < DAY else (fmt(start), fmt(end))
                text = f"{name} {start_at} - {end_at}"
                lines.append(f"        -> health: {text}, {why}" if why else f"        -> health: {text}")
        if health_note:
            lines.append(f"        -> health: {health_note}")
        if db and db[0]:
            lines.append(f"        -> db    : {db[0]}")
    return "\n".join(lines) + "\n"


class DoorAlerts:
    def __init__(self, min_time: dict[int, int | None]):
        """min_time: {door: sensor alert_min_time in minutes, None = no open too long}."""
        self.min_time = min_time
        self.status = dict.fromkeys(min_time)           # vehicle_status doorN_status (NULL after reset)
        self.last_alert = dict.fromkeys(min_time)       # vehicle_status last_doorN_alert
        self.pending = dict.fromkeys(min_time)          # (value, change time)
        self.last_open = dict.fromkeys(min_time)
        self.last_packet = None                         # vehicle_status last_packet (reset sentinel = before all)
        self.alerts: list[Alert] = []
        self.glitches: list[Glitch] = []
        self.late_too_long: list[Alert] = []            # open too long while a close is pending (see below)
        self.message = None
        self.on_confirm = None                          # callback(door, value, change time): valid change

    # ---- one gate message ----
    def record(self, sec: int, doors: dict[int, int], location: bool = True):
        """Door packets {door: value after the pin conversion}, then the location packet of the same time."""
        self.message = sec
        for door, value in doors.items():
            self.door_packet(sec, door, value)
        if location:
            self.location_packet(sec)
        self.end_message()

    def door_packet(self, sec: int, door: int, value: int):
        """DoorSensor.process of one door packet (open too long, then the change debounce)."""
        if self.last_open[door] is not None:
            self._open_too_long(door, sec)
        self._check(door, value, sec)

    def location_packet(self, sec: int):
        if self.last_packet is None or sec > self.last_packet:
            self.last_packet = sec

    def end_message(self):
        """Sensor.sensor_staged_packet, after every packet of the gate message."""
        self._staged()

    def door(self, sec: int, door: int, value: int):
        self.record(sec, {door: value}, location=False)

    def location(self, sec: int):
        self.record(sec, {})

    # ---- kernel DoorSensor ----
    def _staged(self):
        """create_staged_packet: open too long first, then the pending change."""
        if self.last_packet is None:
            return
        for door in self.min_time:
            if door not in STAGED_DOORS:
                continue
            if self.last_open[door] is not None and self.last_open[door] < self.last_packet:
                self._open_too_long(door, self.last_packet)
            pending = self.pending[door]
            if pending is not None and pending[1] < self.last_packet:
                self._check(door, pending[0], self.last_packet)

    def _check(self, door: int, value: int, sec: int):
        """_alert_checker. last_open is read before the alert (kernel keeps the old door state)."""
        pending, last_open = self.pending[door], self.last_open[door]
        if pending and sec - pending[1] <= DOOR_TIMEOUT and value == pending[0]:
            return
        if pending and sec - pending[1] > DOOR_TIMEOUT:
            self._confirm(door, *pending)
        elif pending:
            self.glitches.append(Glitch(door, pending[0], pending[1], sec))
            if self.status[door] == CLOSE and last_open is not None:
                self.last_open[door] = None
        self.pending[door] = None
        if value != self.status[door]:
            if value == OPEN and self.min_time[door] is not None:
                last_open = sec
            self.pending[door] = (value, sec)
            self.last_open[door] = last_open

    def _confirm(self, door: int, value: int, sec: int):
        """_prepare_alert."""
        self.status[door] = value
        code = OPEN_ALERTS[door] if value == OPEN else CLOSE_ALERTS[door]
        self.alerts.append(Alert(code, door, sec, self.message, value))
        self.last_alert[door] = sec
        if value == CLOSE:
            self.last_open[door] = None
        if self.on_confirm:
            self.on_confirm(door, value, sec)

    def _open_too_long(self, door: int, sec: int):
        """_check_open_door_duration."""
        minutes = self.min_time[door]
        if minutes and sec - self.last_open[door] >= minutes * 60:
            alert = Alert(OPEN_TOO_LONG, door, sec, self.message, minutes, SENSOR_CODES[door])
            self.alerts.append(alert)
            self.last_open[door] = None
            # kernel checks open too long before the pending change: a door closed just before the limit
            # still gets the alert when the close is confirmed by the next message
            if self.pending[door] and self.pending[door][0] == CLOSE:
                self.late_too_long.append(alert)


class DoorMessage(NamedTuple):
    """One gate message for the device health replay."""
    sec: int
    doors: list[tuple[int, int, bool]]        # door packets in order: (door, value, on_change)
    location: dict | None = None              # location packet after the doors: lng, lat, speed, acc


def simulate_door_health(messages: list[DoorMessage], doors: int, min_time: dict) -> tuple[list[Summary], dict]:
    """DeviceHealth DOOR-01 / 02 / 03 summaries and the door status after each message {second: (door-1, ...)}.
    Kernel order per gate message: each door packet (data received, then the change debounce: a change confirmed
    by this packet is recorded at once), the location, sensor_staged_packet (changes confirmed by the newer
    location), then the alarm packets the message made. DeviceHealth runs after each packet (alarm: at its time)."""
    alerts, health = DoorAlerts(min_time), DoorSimulator(doors)
    alerts.on_confirm = lambda door, value, change_sec: health.change(door - 1, value, change_sec)
    statuses = {}
    for message in messages:
        alerts.message = message.sec
        first_alert = len(alerts.alerts)
        for door, value, on_change in message.doors:
            health.door(message.sec, door - 1, on_change)
            alerts.door_packet(message.sec, door, value)
            health.check(message.sec)
        if message.location is not None:
            location = message.location
            health.location(message.sec, location["lng"], location["lat"], location["speed"], location["acc"])
            alerts.location_packet(message.sec)
        alerts.end_message()
        for alert in alerts.alerts[first_alert:]:
            health.check(alert.time)
        statuses[message.sec] = tuple(health.h["status"])
    return health.summaries, statuses


def check_simulation(sim: DoorAlerts, counts: dict[int, int], too_long: list, glitches: int):
    """Generator guard: the cases written in the description still match the simulated kernel."""
    actual = {code: sum(1 for alert in sim.alerts if alert.code == code) for code in counts}
    actual_too_long = [(alert.door, alert.time) for alert in sim.alerts if alert.code == OPEN_TOO_LONG]
    if actual != counts or len(sim.alerts) != sum(counts.values()) or actual_too_long != too_long \
            or len(sim.glitches) != glitches:
        alerts = [(ALERT_NAMES[alert.code], fmt(alert.time)) for alert in sim.alerts]
        raise ValueError(f"door cases changed: {alerts}, glitches {sim.glitches}")
    if sim.late_too_long:
        raise ValueError(f"open too long while the close is pending (kernel order), move the close: "
                         f"{[fmt(alert.time) for alert in sim.late_too_long]}")


# ---------------- expected ----------------
def step_name(sec: int, last_sec: int) -> str:
    return END_STEP if sec == last_sec else f"cek_h{1 + sec // DAY}_{fmt(sec)[3:].replace(':', '')}"


def alert_check(alert: Alert) -> dict:
    where = {"vehicle_id": "$vehicle_id", "alert_code": alert.code}
    note = f"{ALERT_NAMES[alert.code]} ({alert.code})"
    if alert.code == OPEN_TOO_LONG:
        where["alert_trigger"] = alert.trigger
        note += f" door{alert.door}, alert_data = alert_min_time {alert.data} menit"
    else:
        note += f", alert_data {alert.data} ({'open' if alert.data == OPEN else 'closed'})"
    where["created_on"] = fmt(alert.time)
    return {"table": "raw_alert", "where": where, "count": 1, "match": {"alert_data": alert.data}, "note": note}


def no_alert_check(door: int, value: int, start: int, end: int, note: str) -> dict:
    code = (OPEN_ALERTS if value == OPEN else CLOSE_ALERTS)[door]
    when = fmt(start) if start == end else {"between": [fmt(start), fmt(end)]}
    where = {"vehicle_id": "$vehicle_id", "alert_code": code, "created_on": when}
    return {"table": "raw_alert", "where": where, "count": 0, "note": f"{note}: tidak ada {ALERT_NAMES[code]}"}


def status_check(doors: set, state: tuple) -> dict:
    status, last_alert = state
    match, notes = {}, []
    for door in sorted(doors):
        match[f"door{door}_status"] = status[door]
        match[f"last_door{door}_alert"] = fmt(last_alert[door])
        notes.append(f"door{door} {'open' if status[door] == OPEN else 'closed'}")
    note = ", ".join(notes) + " (0 = open, 1 = closed), last alert = waktu perubahan"
    return {"table": "vehicle_status", "where": {"vehicle_id": "$vehicle_id"}, "match": match, "note": note}


def build_checks(sim: DoorAlerts, states: dict, locations: list[int], no_alert=()) -> dict[int, list]:
    """{message second: checks}: alerts where the kernel creates them, no alert (glitches, no_alert
    [(second, door, value, reason)]) at the next location message. states: {second: (status, last_alert)}."""
    raw, doors = {}, {}

    def add(sec: int, door: int, check: dict):
        raw.setdefault(sec, []).append(check)
        doors.setdefault(sec, set()).add(door)

    for alert in sim.alerts:
        add(alert.created, alert.door, alert_check(alert))
    quiet = [(glitch.door, glitch.value, glitch.start, glitch.end,
              f"glitch door{glitch.door} {fmt(glitch.start)[3:]} -> {fmt(glitch.end)[3:]} "
              f"({glitch.end - glitch.start} detik <= 3)") for glitch in sim.glitches]
    quiet += [(door, value, sec, sec, reason) for sec, door, value, reason in no_alert]
    for door, value, start, end, note in quiet:
        add(next(sec for sec in locations if sec > end), door, no_alert_check(door, value, start, end, note))
    return {sec: raw[sec] + [status_check(doors[sec], states[sec])] for sec in sorted(raw)}


def add_health_status(checks: dict[int, list], statuses: dict[int, tuple], last_sec: int,
                      sensor: str = "door") -> dict[int, list]:
    """device_health <sensor>_N_error_status (simulated status after each gate message) where it changes, and at
    the last message. statuses: {second: (channel-1 status, channel-2 status, ...)}, None = normal (NULL)."""
    merged = {sec: list(items) for sec, items in checks.items()}
    previous = None
    for sec, status in statuses.items():
        if previous is None:
            previous = (None,) * len(status)
        if status != previous or sec == last_sec:
            changed = [f"{sensor}-{i + 1} {old or 'normal'} -> {new or 'normal'}"
                       for i, (old, new) in enumerate(zip(previous, status)) if old != new]
            match = {f"{sensor}_{i + 1}_error_status": value for i, value in enumerate(status)}
            note = f"status device health {sensor} di akhir pesan (null = normal)"
            merged.setdefault(sec, []).append({
                "table": "device_health", "where": {"imei": "$imei"}, "match": match,
                "timeout": "00:01:00", "note": f"{', '.join(changed)}; {note}" if changed else note,
            })
        previous = status
    return dict(sorted(merged.items()))


def alert_steps(checks: dict[int, list], last_sec: int, counts: dict[int, int], end_checks: list) -> dict:
    """Expected steps: alert checks per step, step `end` = wait for the last packet, alert totals, end_checks,
    then the alert checks of the last message."""
    steps = {step_name(sec, last_sec): {"db": items} for sec, items in checks.items()}
    end = [
        {"table": "vehicle_status", "where": {"vehicle_id": "$vehicle_id"}, "match": {"last_packet": "$step_at"},
         "timeout": "00:10:00", "note": "tunggu kernel selesai memproses data terakhir"},
        *[{"table": "raw_alert", "where": {"vehicle_id": "$vehicle_id", "alert_code": code}, "count": count,
           "note": f"jumlah {ALERT_NAMES[code]} sejak awal timeline"} for code, count in counts.items()],
        *end_checks,
    ]
    steps[END_STEP] = {"db": end + steps.get(END_STEP, {}).get("db", [])}
    return steps
