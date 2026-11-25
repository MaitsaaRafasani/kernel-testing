"""Shared parts of the device health generators (device_health_gps, device_health_fuel).

A Teltonika truck driving a real road loop (routes/health_loop.json): a record every 10 s while acc is on and every
5 min while acc is off, always a record at an acc change. Values like a real vehicle: speed varies and matches the
distance between records, satellites 9-13, accu 13.9-14.3 V engine on / 12.4-12.6 V engine off.
The simulators mirror kernel v2 DeviceHealth (GPS / FUEL, general_timeout = 5) and MotionStatus, like the kernel
scenario generators (healthExpectedResult.md), so the expected summaries follow the generated data.
"""
import math
from copy import deepcopy
from datetime import datetime, timedelta
from typing import NamedTuple

from harness.route import Route, distance
from harness.scenario_import import to_timeline

STEP_ON = 10                    # seconds between records, acc on
STEP_OFF = 5 * 60               # seconds between records, acc off
DAY = 24 * 3600
ALTITUDE = 10                   # metres, Jakarta - Karawang lowland
ROUTE = "health_loop"


def m(hh: int, mm: int = 0, ss: int = 0, day: int = 0) -> int:
    """Seconds from the timeline start (H1 00:00:00 = 0)."""
    return day * DAY + hh * 3600 + mm * 60 + ss


def fmt(sec: int) -> str:
    """H1 hh:mm:ss (expected time variable)."""
    return f"H{1 + sec // DAY} {datetime(2000, 1, 1) + timedelta(seconds=sec % DAY):%H:%M:%S}"


class Summary(NamedTuple):
    """One expected summary_device_error row (seconds from the timeline start)."""
    label: str                          # error_type, ex: GPS-01
    start: int
    end: int
    gap: float | None = None            # GPS: description = metres between the positions
    description: str | None = None      # ex: door-1 (FUEL: always 'fuel')
    created: int | None = None          # moment the kernel inserts it, when not `end` (DOOR-03)
    note: str = ""


def is_in(sec: int, windows) -> bool:
    return any(start <= sec < end for start, end in windows)


def advanced(current, previous) -> bool:
    """DeviceHealth._is_advanced: the time moved forward."""
    return current is not None and previous is not None and current > previous


def seconds_between(current, previous) -> int:
    """DeviceHealth._seconds_between: 0 when one is unknown."""
    return 0 if current is None or previous is None else current - previous


def schedule(total: int, off_windows) -> list[tuple[int, bool]]:
    """[(second, acc_on)]: 10 s apart with acc on, 5 min apart with acc off, a record at every acc change."""
    bounds = sorted({bound for window in off_windows for bound in window})
    rows, sec = [], 0
    while sec < total:
        off = is_in(sec, off_windows)
        rows.append((sec, not off))
        step = sec + (STEP_OFF if off else STEP_ON)
        sec = next((bound for bound in bounds if sec < bound < step), step)
    return rows


def drive_speed(sec: int, slow: bool = False) -> int:
    """km/h. Normal traffic 24-52, traffic jam 8-11 (both change smoothly, never below kernel MIN_SPEED 5)."""
    if slow:
        return round(9.5 + 1.5 * math.sin(2 * math.pi * sec / 420))
    return round(38 + 10 * math.sin(2 * math.pi * sec / 900) + 4 * math.sin(2 * math.pi * sec / 170))


def satellites(sec: int) -> int:
    return 9 + (sec // STEP_ON * 7) % 5


def voltage(sec: int, engine_on: bool) -> float:
    """Accu volt: alternator charging when the engine runs (kernel engine on = above 13.3 V)."""
    step = (sec // STEP_ON * 3) % 5
    return round(13.9 + step / 10, 1) if engine_on else round(12.4 + step % 3 / 10, 1)


class Odometer:
    """Position on the loop from the metres driven: the truck continues where it stopped."""

    def __init__(self):
        self.route = Route(ROUTE)
        self.metres = 0.0
        self.last = None
        self.last_speed = 0

    def advance(self, sec: int, speed: int, driving: bool) -> tuple[float, float]:
        """The truck moved since the previous record only if it was driving then: a stop record still moves (it drove
        until now), a start record after parking does not (it starts now)."""
        speed = speed if driving else 0
        if self.last is not None and self.last_speed and (speed or self.last_speed):
            self.metres += max(speed, self.last_speed) / 3.6 * (sec - self.last)
        self.last, self.last_speed = sec, speed
        lat, lng, _ = self.route.at(self.metres % self.route.length)
        return lat, lng


def to_case_timeline(records: list[dict], altitude: int | None = ALTITUDE) -> list[dict]:
    """records -> timeline rows (constant runs like parking merged, last packet = step `end`).
    altitude None: family without altitude field (concox). volt None: no external voltage sent.
    record packet `sensor`: a sensor-only packet with the `io` fields (ex: concox 0x94 door).
    record `step`: step_name of that packet (expected checks), the row is never merged."""
    rows = []
    for record in records:
        named = {"step_name": record["step"]} if record.get("step") else {}
        if record.get("packet") == "sensor":
            rows.append((record["sec"], {"packet": "sensor", **named, **record["io"]}))
            continue
        raw = {"packet": "location", **named, "lat": record["lat"], "lng": record["lng"]}
        if altitude is not None:
            raw["altitude"] = altitude
        raw.update(satellites=record["sats"], speed=record["speed"])
        if record["acc"] is not None:                    # no acc = no location packet (sensor + heartbeat only)
            raw["acc"] = record["acc"]
        if record.get("volt") is not None:
            raw["external_voltage"] = round(record["volt"] * 1000)
        raw.update(record.get("io") or {})
        rows.append((record["sec"], raw))
    return to_timeline(rows)


# ---------------- GPS health (DeviceHealth._check_gps_error_status) ----------------
GPS0102 = 3 * 5 * 60 + 60       # GPS-01 / GPS-02 -> 960 s
GPS03 = 3600                    # GPS-03 -> 1 hour
STANDBY_VOLT = 13.3


def simulate_gps(records: list[dict]) -> list[Summary]:
    """GPS summaries (gap = description distance) with GNSS_POSITIONING=false (DeviceHealth._check_gps_error_status).
    GPS-03 timer (last_location_change) restarts on a location change, or with the engine off unless GPS-03 is
    already active: then only a location change ends it (one summary for the whole stop)."""
    t0 = records[0]["sec"]
    h = dict(loc=t0, valid=t0, chg=t0, lon=None, lat=None, status=None)
    last_packet = t0
    summaries = []
    for record in records:
        sec = record["sec"]
        pre = dict(h)
        if record["acc"] is not None:                   # location packet
            last_packet = h["loc"] = sec
            if record["sats"] > 5 and not (record["lng"] == 0 and record["lat"] == 0):
                h["lon"], h["lat"], h["valid"] = record["lng"], record["lat"], sec
        changed = pre["lon"] != h["lon"] or pre["lat"] != h["lat"]
        engine_off = not (record.get("volt") or 0) > STANDBY_VOLT          # no accu voltage = engine off
        if changed or (engine_off and h["status"] != "GPS-03"):
            h["chg"] = last_packet
        # (label, prev, curr, interval, ref of the active check, can recover)
        checks = [
            ("GPS-01", pre["loc"], h["loc"], GPS0102, sec, True),
            ("GPS-02", pre["valid"], h["valid"], GPS0102, h["loc"], True),
            ("GPS-03", pre["chg"], h["chg"], GPS03, h["valid"], changed),
        ]
        h["status"] = next((label for label, start, end, interval, ref, _ in checks
                            if not advanced(end, start) and seconds_between(ref, start) >= interval), None)
        recovered = next(((label, start, end) for label, start, end, interval, _, ok in checks
                          if ok and end > start and end - start >= interval), None)
        if recovered:
            gap = None if None in (pre["lon"], h["lon"]) else distance(pre["lon"], pre["lat"], h["lon"], h["lat"])
            summaries.append(Summary(*recovered, gap=gap))
    return summaries


# ---------------- FUEL health (record_fuel_health + DeviceHealth._check_fuel_status + MotionStatus) ----------
FUEL0102 = 2 * 5 * 60 + 60      # FUEL-01 / FUEL-02 -> 660 s
FUEL_JUMP = 7                   # MAX_FUEL_DIGITAL_THRESHOLD (status_float not set)
FUEL03_SECONDS = 7 * DAY
FUEL03_DISTANCE = 10000         # metres driven with the same value
FUEL_ERROR_CODE = -0.4          # UL202 error code, dropped by the parser
MIN_DISTANCE, MIN_SPEED, MIN_DISTANCE_OFF = 20, 5, 300
NO_GPS_TIMEOUT = 15 * 60
STOP_TIMEOUT = 5 * 60


def simulate_fuel(records: list[dict]) -> tuple[list[Summary], dict]:
    """FUEL summaries and {label: first second the status was active}."""
    def exceeded(check, end):
        _, prev, _, _, interval, stuck, max_distance = check
        if end is not None and prev is not None and end - prev >= interval:
            return True
        return max_distance is not None and (stuck or 0) >= max_distance

    h = dict(recv=None, valid=None, chg=None, value=None, previous=None, stuck=None, lon=None, lat=None)
    status = dict(motion="U", lon=None, lat=None, last_position=None, last_stop=None)
    pre = dict(h)
    summaries, first_active = [], {}

    def record_fuel(sec, value):
        h["recv"] = sec
        if value <= 0:
            return
        previous, h["previous"] = h["previous"], value
        if previous is not None and abs(previous - value) > FUEL_JUMP:
            return
        h["valid"] = sec
        if h["value"] is not None and value != h["value"]:
            h["chg"], h["stuck"] = sec, None
        elif h["value"] is not None and h["stuck"] is None:
            h["stuck"] = 0.0
        h["value"] = value

    def location(sec, lon, lat, speed, engine_on):
        h["lon"], h["lat"] = lon, lat
        moved_distance = distance(status["lon"], status["lat"], lon, lat) if status["lon"] is not None else 0
        if status["motion"] != "U" and sec - status["last_position"] > NO_GPS_TIMEOUT:
            status["motion"] = "U"
        status["last_position"] = sec
        if status["motion"] == "U":
            status.update(motion="I" if engine_on else "P", lon=lon, lat=lat, last_stop=sec)
            return
        if status["motion"] in ("M", "A") and sec - status["last_stop"] >= STOP_TIMEOUT:
            status["motion"] = "I" if engine_on else "P"
        moved = (moved_distance >= MIN_DISTANCE and speed >= MIN_SPEED) if engine_on \
            else moved_distance >= MIN_DISTANCE_OFF
        if moved:
            status.update(motion="M" if engine_on else "A", lon=lon, lat=lat, last_stop=sec)

    def check(sec):
        nonlocal pre
        if h["stuck"] is not None and status["motion"] == "M" and pre["lon"] is not None:
            h["stuck"] += distance(pre["lon"], pre["lat"], h["lon"], h["lat"])
        h["recv"] = sec if h["recv"] is None else h["recv"]
        h["valid"] = h["recv"] if h["valid"] is None else h["valid"]
        h["chg"] = h["valid"] if h["chg"] is None else h["chg"]
        stuck = (pre if advanced(h["chg"], pre["chg"]) else h)["stuck"]
        checks = [
            ("FUEL-01", pre["recv"], h["recv"], sec, FUEL0102, None, None),
            ("FUEL-02", pre["valid"], h["valid"], h["recv"], FUEL0102, None, None),
            ("FUEL-03", pre["chg"], h["chg"], h["valid"], FUEL03_SECONDS, stuck, FUEL03_DISTANCE),
        ]
        active = next((c for c in checks if not advanced(c[2], c[1]) and exceeded(c, c[3])), None)
        recovered = next((c for c in checks if advanced(c[2], c[1]) and exceeded(c, c[2])), None)
        if recovered:
            summaries.append(Summary(recovered[0], recovered[1], recovered[2]))
        if active:
            first_active.setdefault((active[0], active[1]), sec)
        pre = dict(h)

    for record in records:
        sec, fuel = record["sec"], record.get("fuel")
        if fuel is not None:
            check(sec)                                   # UL202 status alarm packet
            if fuel != FUEL_ERROR_CODE:
                record_fuel(sec, fuel)
                check(sec)
        location(sec, record["lng"], record["lat"], record["speed"], bool(record["acc"]))
        check(sec)
    return summaries, first_active


# ---------------- DOOR health (record_door_health + DeviceHealth._check_door_status + MotionStatus) ----------
DOOR01 = 2 * 5 * 60 + 60        # DOOR-01 -> 660 s
DOOR_DISTANCE = 10000           # DOOR-02 open distance / DOOR-03 window / DOOR-04 day distance, metres
DOOR04_DAYS = 3                 # DOOR-04: days driven > 10 km without a valid change
DOOR_MIN_SPEED = 10             # MIN_SPEED_DEVICE_HEALTH: distance / toggles count only above this speed
OPEN, CLOSE = 0, 1              # DoorCodes


class MotionStatus:
    """Kernel MotionStatus.process: vehicle_status motion_status (U / M / A / I / P) and speed per location packet."""

    def __init__(self):
        self.vs = dict(motion="U", lon=None, lat=None, last_position=None, last_stop=None, speed=0)

    def update(self, sec: int, lon: float, lat: float, speed: int, engine_on: bool) -> None:
        vs = self.vs
        packet_speed = speed if engine_on else 0
        if vs["motion"] != "U" and sec - vs["last_position"] > NO_GPS_TIMEOUT:
            vs["motion"] = "U"
        vs["last_position"] = sec
        if vs["motion"] == "U":
            vs.update(motion="I" if engine_on else "P", lon=lon, lat=lat, last_stop=sec, speed=0)
            return
        moved_distance = distance(vs["lon"], vs["lat"], lon, lat)
        if vs["motion"] == "M" and engine_on and moved_distance >= 10 and packet_speed >= 3:
            vs["last_stop"] = sec
        if vs["motion"] in ("M", "A") and sec - vs["last_stop"] >= STOP_TIMEOUT:
            vs.update(motion="I" if engine_on else "P", speed=0)
        moved = moved_distance >= MIN_DISTANCE and packet_speed >= MIN_SPEED if engine_on \
            else moved_distance >= MIN_DISTANCE_OFF
        if moved:
            if vs["motion"] != "M" and engine_on:
                vs["motion"] = "M"
                packet_speed = min(packet_speed, 10)      # first motion record: device speed not trusted
                vs["speed"] = packet_speed
            elif vs["motion"] != "A" and not engine_on:
                vs["motion"] = "A"
            else:
                vs["speed"] = packet_speed
            vs.update(lon=lon, lat=lat, last_stop=sec)
        elif engine_on and vs["motion"] == "P":
            vs["motion"] = "I"
        elif not engine_on and vs["motion"] == "I":
            vs["motion"] = "P"
        if vs["motion"] in ("M", "A"):
            vs["speed"] = packet_speed


class DoorSimulator:
    """Kernel DOOR-01 / 02 / 03 / 04 for `doors` door channels, DOOR-05 (BLE battery) is not simulated.
    DOOR-04 day = local date of the location packet time (kernel dc0e9eea5: last_location_packet).
    Feed packets in kernel order: door() / location() (DeviceHealth runs after each one), change() when DoorSensor
    confirms a change (generators/door_common.py simulate_door_health)."""

    def __init__(self, doors: int):
        self.n = doors
        lists = ("last", "value", "chg", "open_d", "t_start", "t_count", "t_d", "status", "on_change", "days",
                 "day_d")
        self.h = {"lon": None, "lat": None, "loc": None, **{key: [None] * doors for key in lists}}
        self.pre = deepcopy(self.h)
        self.motion = MotionStatus()
        self.vs = self.motion.vs
        self.summaries: list[Summary] = []

    def door(self, sec: int, index: int, on_change: bool) -> None:
        """Door data received (helper.device_health.record_door_health): DOOR-01 only."""
        self.h["last"][index] = sec
        self.h["on_change"][index] = on_change

    def change(self, index: int, value: int, change_sec: int) -> None:
        """Valid door change (record_door_change from DoorSensor._prepare_alert): kept > 3 s or bypass, at the time
        it changed, speed = vehicle status speed when it is confirmed. A glitch never gets here."""
        h = self.h
        previous, h["value"][index] = h["value"][index], value
        if value == previous:
            return
        if previous is not None:
            h["chg"][index] = change_sec
            if self.vs["speed"] > DOOR_MIN_SPEED:
                if h["t_start"][index] is None:
                    h["t_start"][index], h["t_d"][index] = change_sec, 0.0
                h["t_count"][index] = (h["t_count"][index] or 0) + 1
        h["open_d"][index] = 0.0 if value == OPEN else None
        h["days"][index], h["day_d"][index] = 0, 0.0

    def location(self, sec: int, lon: float, lat: float, speed: int, engine_on: bool) -> None:
        """Location.process (health position, valid fix) + MotionStatus.process (motion status and speed)."""
        self.h["lon"], self.h["lat"], self.h["loc"] = lon, lat, sec
        self.motion.update(sec, lon, lat, speed, engine_on)
        self.check(sec)

    def check(self, sec: int) -> None:
        """DeviceHealth.process door part (_increase_distance, _check_door_status), then the next snapshot."""
        h, pre = self.h, self.pre
        if self.vs["motion"] == "M":
            gap = distance(pre["lon"], pre["lat"], h["lon"], h["lat"]) if pre["lon"] is not None else 0
            moving = self.vs["speed"] > DOOR_MIN_SPEED
            for i in range(self.n):
                if h["day_d"][i] is not None:              # DOOR-04 counts every trip, any speed
                    h["day_d"][i] += gap
                if moving and h["open_d"][i] is not None:
                    h["open_d"][i] += gap
                if moving and h["t_d"][i] is not None:
                    h["t_d"][i] += gap
        new_day = None not in (pre["loc"], h["loc"]) and pre["loc"] // DAY != h["loc"] // DAY
        for i in range(self.n):
            if h["last"][i] is None:
                h["last"][i] = sec
            if h["chg"][i] is None:
                h["chg"][i] = h["last"][i]
            if new_day:                 # a day driven > 10 km without a valid change counts
                if (h["day_d"][i] or 0) > DOOR_DISTANCE:
                    h["days"][i] = (h["days"][i] or 0) + 1
                h["day_d"][i] = 0.0
            src = pre if advanced(h["chg"][i], pre["chg"][i]) else h
            open_too_far = (src["open_d"][i] or 0) >= DOOR_DISTANCE
            stuck = (src["days"][i] or 0) >= DOOR04_DAYS
            # (label, prev, curr, active, can recover, exceeded at recovery)
            silent = seconds_between(h["last"][i], pre["last"][i]) >= DOOR01
            if h["on_change"][i]:       # on change source: silence = door not changed, never DOOR-01
                door01 = ("DOOR-01", pre["last"][i], h["last"][i], False, h["status"][i] == "DOOR-01", silent)
            else:
                door01 = ("DOOR-01", pre["last"][i], h["last"][i], seconds_between(sec, pre["last"][i]) >= DOOR01,
                          True, silent)
            checks = [door01, ("DOOR-02", pre["chg"][i], h["chg"][i], open_too_far, True, open_too_far),
                      ("DOOR-04", pre["chg"][i], h["chg"][i], stuck, True, stuck)]
            active = next((c[0] for c in checks if not advanced(c[2], c[1]) and c[3]), None)
            recovered = next((c for c in checks if c[4] and advanced(c[2], c[1]) and c[5]), None)
            if recovered:
                notes = {"DOOR-02": f"jarak terbuka {(src['open_d'][i] or 0) / 1000:.1f} km",
                         "DOOR-04": f"{src['days'][i]} hari jalan > 10 km tanpa perubahan"}
                self.summaries.append(Summary(
                    recovered[0], recovered[1], recovered[2], description=f"door-{i + 1}",
                    created=sec if recovered[0] == "DOOR-04" else None, note=notes.get(recovered[0], "")))
            door03 = None
            if h["t_start"][i] is not None:
                flapping = h["t_count"][i] > 1
                if (h["t_d"][i] or 0) >= DOOR_DISTANCE:
                    if flapping:
                        self.summaries.append(Summary(
                            "DOOR-03", h["t_start"][i], h["chg"][i], description=f"door-{i + 1}", created=sec,
                            note=f"jarak {h['t_d'][i] / 1000:.1f} km, {h['t_count'][i]} perubahan saat jalan"))
                    h["t_start"][i] = h["t_count"][i] = h["t_d"][i] = None
                elif flapping:
                    door03 = "DOOR-03"
            h["status"][i] = active or door03
        self.pre = deepcopy(h)


# ---------------- expected ----------------
def expected_checks(usecase: str, summaries: list[Summary]) -> dict:
    """Step `end`: last packet processed, then the summaries (health_checks)."""
    checks = [
        {"table": "vehicle_status", "where": {"vehicle_id": "$vehicle_id"}, "match": {"last_packet": "$step_at"},
         "timeout": "00:10:00", "note": "tunggu kernel selesai memproses data terakhir"},
        *health_checks(summaries),
    ]
    return {usecase: {"default": {"steps": {"end": {"db": checks}}}}}


def health_checks(summaries: list[Summary]) -> list[dict]:
    """summary_device_error: total, then each summary (description: fuel / door-N)."""
    checks = [
        {"table": "summary_device_error", "where": {"vehicle_id": "$vehicle_id"}, "count": len(summaries),
         "note": "jumlah semua summary device health sejak awal timeline"},
    ]
    for item in sorted(summaries, key=lambda summary: (summary.end, summary.start)):
        where = {"vehicle_id": "$vehicle_id", "error_type": item.label}
        check = {"table": "summary_device_error", "where": where, "count": 1}
        description = "fuel" if item.label.startswith("FUEL") else item.description
        if description:
            where["description"] = description
        where.update({"start_time": fmt(item.start), "end_time": fmt(item.end)})
        notes = [f"dibuat {fmt(item.created if item.created is not None else item.end)}"]
        if item.label.startswith("GPS") and item.gap is not None:
            check["match"] = {"description": {"approx": item.gap, "tolerance": 1}}
            notes.append("jarak m")
        if item.note:
            notes.append(item.note)
        check["note"] = ", ".join(notes)
        checks.append(check)
    return checks
