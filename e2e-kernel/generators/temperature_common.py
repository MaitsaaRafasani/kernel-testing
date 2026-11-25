"""Dallas temperature wire 1-3 (kernel v2), shared by temperature_teltonika / _ruptela / _queclink.

python cli.py generate --usecase temperature_<family>
A reefer truck (wire 1 freezer, wire 2 chiller, wire 3 cabin) drives routes/health_loop.json, parks overnight and
drives again on day 2 (H2 06:30 - 15:30) with the device health TEMP cases of the kernel scenario
device_health_TEMP.txt (replaces the usecase device_health_temp). The family module turns each record's
temperatures into its own encoding and error values, and says what the kernel parser makes of them (Reading).
One record = one gate message, simulated in kernel order:
1. sensor status alarm per wire (teltonika / ruptela): raw_alert 76 only when the status changes, status True =
   temperature data received but invalid (DeviceHealth last_temperature_vehicle)
2. TemperatureSensor per valid wire: vehicle_status.temperature, raw_sensor_info values, DeviceHealth valid / changes,
   alert TEMPERATURE_1/2/3 (alert_hi / alert_lo when motion_status M, else alert_idle_hi / alert_idle_lo), at most
   once per 300 s per alert type, the first packet only starts the timers
3. location (MotionStatus), then Device._store_raw_sensor: a wire without data for more than 10 min (packet time,
   other wires still sending) is reset (Device.__reset_sensor: vehicle_status.temperature[i] and raw_sensor_info
   temperatureN = NULL); raw_sensor_info insert = first, changed and >= 60 s, >= 300 s, or an alert
DeviceHealth TEMP-01/02/03 (general_timeout 5 -> 660 s) runs after every packet.
Expected = the correct behaviour. Two kernel bugs are simulated too; where the kernel differs the check has
`known_bug` (it fails until the kernel is fixed):
- alert threshold 0 is ignored (`if alert_lo and ...`)
- alert interval uses timedelta.seconds: a gap of 1 day + x counts as x
"""
from copy import deepcopy
from typing import NamedTuple

from generators.door_common import add_health_status, describe_cases
from generators.health_common import (
    DAY, MotionStatus, Odometer, Summary, advanced, drive_speed, fmt, health_checks, is_in, m, satellites,
    schedule, seconds_between, simulate_gps, to_case_timeline,
)
from versions.v2 import setup as v2_setup

WIRES = (1, 2, 3)
SENSORS = [{"code": f"temperature_{wire}", "pin": f"dallas1_wire{wire}"} for wire in WIRES]
PINS = {wire: v2_setup.PIN_CODES[f"dallas1_wire{wire}"] for wire in WIRES}          # alert_trigger of raw_alert 76
ALERT_CODES = {1: 18, 2: 17, 3: 26}           # AlertCodes TEMPERATURE_1 / 2 / 3
STATUS_ALERT = 76                             # AlertCodes DALLAS_TEMPERATURE_STATUS
ALERT_INTERVAL = 300                          # Constants.ALERT_TEMPERATURE_INTERVAL (alert when gap > 300 s)
RESET_AFTER = 10 * 60                         # Constants.TIMEOUT_SENSOR (reset when gap > 600 s)
TEMP_INTERVAL = 2 * 5 * 60 + 60               # TEMP-01/02/03: 2 x general_timeout 5 min + 60 s
TOLERANCE = 0.05                              # DB temperature columns (float / numeric)

# sensor setting per wire (DB). wire 2 alert_lo 0: a chiller must stay above freezing (kernel bug: 0 = no alert)
THRESHOLDS = {
    1: {"alert_hi": -15, "alert_lo": -25, "alert_idle_hi": -12, "alert_idle_lo": -25},
    2: {"alert_hi": 8, "alert_lo": 0, "alert_idle_hi": 10, "alert_idle_lo": 1},
    3: {},
}
ALERT_TYPES = {         # type -> (threshold key, compare, alert_flag)
    "hi": ("alert_hi", 1, True), "lo": ("alert_lo", -1, True),
    "idle_hi": ("alert_idle_hi", 1, False), "idle_lo": ("alert_idle_lo", -1, False),
}


def h2(hh: int, mm: int = 0, ss: int = 0) -> int:
    return m(hh, mm, ss, day=1)


TOTAL = h2(15, 30, 1)                         # last record H2 15:30:00
OFF = [(m(0), m(0, 30)), (m(5), h2(6, 30))]   # depot loading, overnight parking (record every 5 min)
IDLE = [(m(2), m(2, 20))]                     # customer stop with the engine running (record every 10 s)
# device health cases moved from the kernel scenario device_health_TEMP.txt (H2 = day 2)
SAMPLES_12_MIN = {h2(11, 12), h2(11, 24), h2(11, 36), h2(11, 48)}               # U  wire 3 only these records
MISSING = {                                   # wire not sent at all (sensor unplugged / no IO / no 1-wire id)
    1: [(m(23, 30), h2(0, 30)), (h2(7), h2(7, 30)), (h2(7, 45), h2(7, 55)), (h2(9), h2(9, 30)),
        (h2(14, 30), h2(15))],                # N, P, Q, S, Y
    2: [],
    3: [(h2(11, 0, 10), h2(12))],             # U  except SAMPLES_12_MIN
}
OFFLINE = [(m(4), m(4, 13))]                  # J  no signal while driving: no record at all
SENSOR_LOST = (m(2, 30), m(2, 50))            # E  wire 3 sensor lost while driving (family: error / no data)
FAULTS = [m(3, s * 5) for s in range(4)]      # F  wire 2: one bad record each (family error codes / unknown id)
BOUNDS = {m(3, 50): 125.0, m(3, 50, 10): 125.1, m(3, 50, 20): -55.0, m(3, 50, 30): -55.1}     # H  wire 3
SENSOR_85 = m(3, 55)                          # I  wire 3 reads 85.0 C
RAW_WINDOWS = [                               # M  raw_sensor_info rows counted per window
    (m(0, 30), m(0, 59, 59), "jalan 00:30-01:00, suhu berubah tiap paket: 1 baris tiap >= 60 detik"),
    (m(5), m(5, 59, 59), "parkir 05:00-06:00, paket tiap 5 menit: tiap paket 1 baris (>= 300 detik)"),
]


ORDER = ["Awal", *"ABCDEFGHIJKLMNOPQRSTUVWXY"]
BUG_ZERO = "BUG kernel known_bug: alert_lo 0 diabaikan (if alert_lo and ...)"
BUG_DAY = "BUG kernel known_bug: jeda 1 hari dihitung 0 detik (timedelta.seconds)"

# cases every family has: (label, what, alert, health note, db note). Family cases: E, F, (G), H, I + Awal db note.
COMMON_CASES = [
    ("A", "jalan: freezer -14 C 01:00:00-01:06:00 (di atas alert_hi -15)",
     "TEMPERATURE_1 01:00:00 + 01:05:10 (> 300 detik sejak alert sebelumnya), flag true, trigger -15", None,
     "raw_sensor_info baris saat alert"),
    ("B", "jalan: freezer -26 C 01:30:00-01:32:00 (di bawah alert_lo -25)",
     "TEMPERATURE_1 01:30:00 saja (< 300 detik), trigger -25", None),
    ("C", "jalan: chiller -1 C 01:45:00-01:48:00 (di bawah alert_lo 0)",
     f"TEMPERATURE_2 01:45:00, trigger 0 [{BUG_ZERO}]", None),
    ("D", "berhenti mesin hidup 02:00-02:20 (motion I 5 menit setelah berhenti): chiller 11 C 02:08:00-02:12:00",
     "TEMPERATURE_2 idle 02:08:00 (di atas alert_idle_hi 10), flag false", None),
    ("J", "jalan: sinyal hilang 04:00:00-04:13:00 (tanpa record)", "tidak ada",
     "13 menit < 16: belum GPS-01", "suhu tidak di-reset: paket berikutnya membawa semua wire"),
    ("K", "parkir: kabin 27.0 C tetap 05:00:00-05:30:00 (wire lain tetap berubah)", "tidak ada", None),
    ("L", "parkir semalam: freezer mati -11 C H1 06:00:00 dan H2 06:00:00-06:15:00 (di atas alert_idle_hi -12)",
     f"TEMPERATURE_1 idle H1 06:00:00, H2 06:00:00 [{BUG_DAY}], H2 06:10:00 (H2 06:05:00 baru 300 detik)", None),
    ("N", "parkir: wire 1 tidak dikirim H1 23:30:00-H2 00:30:00 (sensor dicabut)", "tidak ada", None,
     "wire 1 NULL setelah > 10 menit, terisi lagi H2 00:30:00"),
    ("O", "parkir semalam: kabin 24.5 C tetap H2 00:00:00-06:00:00", "tidak ada", None),
    ("P", "jalan: wire 1 tidak dikirim H2 07:00:00-07:30:00", "tidak ada", None, "wire 1 NULL setelah > 10 menit"),
    ("Q", "jalan: wire 1 tidak dikirim dan chiller 3.7 C tetap, sama-sama 10 menit H2 07:45:00-07:54:50",
     "tidak ada", "tidak ada (< 11 menit)", "wire 1 tidak di-reset (tepat 600 detik, harus > 600)"),
    ("U", "jalan: kabin hanya terkirim tiap 12 menit (H2 11:12, 11:24, 11:36, 11:48), sisanya tidak dikirim "
     "H2 11:00:10-11:59:50", "tidak ada", None, "kabin NULL di antara sampel (> 10 menit), terisi di tiap sampel"),
    ("Y", "jalan: wire 1 tidak dikirim H2 14:30:00-14:59:50, kembali dengan nilai sama -18.0 C", "tidak ada",
     "bukan TEMP-03: wire 1 sudah di-reset NULL, jadi -18.0 dihitung berubah (expected device_health_TEMP kernel "
     "masih TEMP-03)", "wire 1 NULL setelah > 10 menit"),
]
COMMON_HEALTH = [
    ("J", "TEMP-01", "temperature-1", m(3, 59, 50), m(4, 13), "tidak ada data >= 11 menit"),
    ("J", "TEMP-01", "temperature-2", m(3, 59, 50), m(4, 13), None),
    ("J", "TEMP-01", "temperature-3", m(3, 59, 50), m(4, 13), None),
    ("K", "TEMP-03", "temperature-3", m(5), m(5, 30), "nilai sama >= 11 menit (aktif 05:15:00)"),
    ("N", "TEMP-01", "temperature-1", m(23, 25), h2(0, 30), "data terakhir = paket parkir 23:25:00"),
    ("O", "TEMP-03", "temperature-3", h2(0), h2(6), "nilai sama 6 jam"),
    ("P", "TEMP-01", "temperature-1", h2(6, 59, 50), h2(7, 30), None),
    ("S", "TEMP-01", "temperature-1", h2(8, 59, 50), h2(9, 30), None),
    ("S", "TEMP-03", "temperature-2", h2(9), h2(9, 30), None),
    *[("U", "TEMP-01", "temperature-3", start, start + 720,
       "jeda 12 menit >= 11, 1 summary per sampel" if start == h2(11) else None)
      for start in (h2(11), *sorted(SAMPLES_12_MIN))],
    ("Y", "TEMP-01", "temperature-1", h2(14, 29, 50), h2(15), None),
]


def cases(family, raw_counts: tuple[int, int]) -> list[tuple]:
    first = ("Awal", "parkir depot, freezer pre-cooling: wire 1 -10.2 C 00:00:00, -11.3 C 00:05:00 (di atas "
             "alert_idle_hi -12)", "tidak ada: paket pertama hanya mulai timer, 00:05:00 baru 300 detik (harus > 300)",
             None, family.FIRST_DB)
    rows = ("M", "raw_sensor_info: jalan 00:30-01:00 (suhu berubah tiap paket), parkir 05:00-06:00 (paket 5 menit)",
            "tidak ada", None, f"jalan: 1 baris tiap >= 60 detik ({raw_counts[0]} baris), parkir: tiap paket "
                               f"({raw_counts[1]} baris)")
    items = [first, *COMMON_CASES, *family.CASES, rows]
    return sorted(items, key=lambda item: ORDER.index(item[0]))


class Reading(NamedTuple):
    """What the kernel parser makes of one wire in one record."""
    value: float | None = None                # sensor packet value (None = no sensor packet)
    status: tuple | None = None               # sensor status alarm (flag, alert_data), None = not sent


def jitter(index: int) -> float:
    """-0.2 .. +0.2, never the same twice in a row: a real sensor never reads the same value long (no TEMP-03)."""
    return 0.1 * ((index * 7) % 5 - 2)


def celsius(sec: int, index: int) -> list[float | None]:
    """Temperature per wire (C) of the record, cases of the description included. None = wire not sent."""
    j = jitter(index)
    all_85 = h2(13, 45) <= sec < h2(14, 15)                             # X  every wire reads 85.0
    freezer = -18 + j
    if sec < m(0, 30):
        freezer = -10 - 8 * sec / m(0, 30) + j                          # Awal  pre-cooling at the depot
    elif m(1) <= sec < m(1, 6) or m(1, 30) <= sec < m(1, 32):
        freezer = (-14 if sec < m(1, 6) else -26) + j                   # A / B  while driving
    elif m(6) <= sec < m(6, 5) or h2(6) <= sec < h2(6, 15):
        freezer = -11 + j                                               # L  unit off while parked
    elif sec in (h2(14, 29, 50), h2(15)):
        freezer = -18.0                                                 # Y  same value before and after the gap
    chiller = 4 + j
    if m(1, 45) <= sec < m(1, 48):
        chiller = -1 + j                                                # C  below alert_lo 0
    elif m(2, 8) <= sec < m(2, 12):
        chiller = 11 + j                                                # D  door open at the stop
    elif h2(7, 45) <= sec < h2(7, 55):
        chiller = 3.7                                                   # Q  the same value 10 min
    elif h2(8) <= sec < h2(8, 30):
        chiller = 85.0                                                  # R
    elif h2(9) <= sec < h2(9, 30):
        chiller = 3.5                                                   # S  the same value 30 min
    cabin = 29 + j if sec < m(5) else 26 + j if sec < h2(6) else 28 + j
    if sec in BOUNDS:
        cabin = BOUNDS[sec]
    elif sec == SENSOR_85:
        cabin = 85.0
    elif m(5) <= sec < m(5, 30):
        cabin = 27.0                                                    # K  the same value 30 min
    elif h2(0) <= sec < h2(6):
        cabin = 24.5                                                    # O  the same value 6 hours
    elif h2(9) <= sec < h2(9, 30):
        cabin = 85.0 if sec // 10 % 2 == 0 else 150.0                   # S  85.0 / 150.0 every other record
    elif h2(10) <= sec < h2(10, 30) or h2(12, 30) <= sec < h2(12, 45):
        cabin = 150.0                                                   # T / V
    elif h2(12, 15) <= sec < h2(12, 30):
        cabin = 85.0                                                    # V
    values = [freezer, chiller, cabin]
    if all_85:
        values = [85.0] * 3
    for wire in WIRES:
        if is_in(sec, MISSING[wire]) and sec not in SAMPLES_12_MIN:
            values[wire - 1] = None
    return [None if value is None else round(value, 2) for value in values]


def records() -> list[dict]:
    """Location records, 10 s apart driving / idling, 5 min parked, with the temperature per wire."""
    odometer, rows = Odometer(), []
    for sec, acc_on in schedule(TOTAL, OFF):
        speed = drive_speed(sec) if acc_on and not is_in(sec, IDLE) else 0
        lat, lng = odometer.advance(sec, speed, acc_on)
        if is_in(sec, OFFLINE):
            continue
        rows.append({"sec": sec, "lat": lat, "lng": lng, "speed": speed, "sats": satellites(sec), "acc": acc_on,
                     "celsius": celsius(sec, len(rows))})
    return rows


class Alert(NamedTuple):
    sec: int
    wire: int
    kind: str                                 # hi / lo / idle_hi / idle_lo
    flag: bool                                # alert_flag: True moving, False idle
    data: str                                 # alert_data = str(float(temperature))
    trigger: float                            # alert_trigger = threshold


class Kernel:
    """Kernel v2 temperature processing per gate message. bug=True: the kernel as it is (known bugs)."""

    def __init__(self, bug: bool):
        self.bug = bug
        self.motion = MotionStatus()
        self.status = [None] * 3                                         # vehicle_status.temperature
        self.timers = {wire: dict.fromkeys(ALERT_TYPES) for wire in WIRES}
        self.alerts: list[Alert] = []
        self.last_status = {}
        self.status_alerts = []                                          # (sec, wire, flag, data)
        self.raw = {"created": None, "times": [None] * 3, "values": [None] * 3, "by_pass": False}
        self.raw_pre = None
        self.raw_rows = []                                               # (created_on, (t1, t2, t3))
        self.h = {key: [None] * 3 for key in ("last", "valid", "chg", "error")}
        self.pre = deepcopy(self.h)
        self.summaries: list[Summary] = []
        self.vehicle_status, self.health_status = {}, {}                 # second -> state after the message

    def message(self, record: dict, readings: dict[int, Reading]) -> None:
        sec = record["sec"]
        for wire, reading in readings.items():
            if reading.status is not None:
                self.status_alarm(sec, wire, *reading.status)
                self.check_health(sec)
        for wire, reading in readings.items():
            if reading.value is not None:
                self.sensor(sec, wire, reading.value)
                self.check_health(sec)
        self.motion.update(sec, record["lng"], record["lat"], record["speed"], record["acc"])
        self.check_health(sec)
        self.store_raw_sensor()
        self.vehicle_status[sec] = list(self.status)
        self.health_status[sec] = tuple(self.h["error"])

    def status_alarm(self, sec: int, wire: int, flag: bool, data: str) -> None:
        """Alarm._set_temperature_status_health + is_sensor_status_changed."""
        if flag:
            self.h["last"][wire - 1] = sec
        if self.last_status.get(wire) == flag:
            return
        self.last_status[wire] = flag
        self.status_alerts.append((sec, wire, flag, data))

    def sensor(self, sec: int, wire: int, value: float) -> None:
        """TemperatureSensor.process."""
        i, raw = wire - 1, self.raw
        self.h["last"][i] = sec
        raw["created"], raw["times"][i] = sec, sec
        changed = value != self.status[i]
        self.status[i] = value
        self.h["valid"][i] = sec
        if changed:
            self.h["chg"][i] = sec
        if self.alert_check(sec, wire, value):
            raw["by_pass"] = True
        raw["values"] = list(self.status)

    def alert_check(self, sec: int, wire: int, value: float) -> bool:
        """alert_check_based_sensor + last_alert_check."""
        moving = self.motion.vs["motion"] == "M"
        kind = None
        for name in (("hi", "lo") if moving else ("idle_hi", "idle_lo")):
            key, sign, _ = ALERT_TYPES[name]
            threshold = THRESHOLDS[wire].get(key)
            is_set = bool(threshold) if self.bug else threshold is not None
            if is_set and (value - threshold) * sign > 0:
                kind = name
                break
        timers = self.timers[wire]
        if any(timer is None for timer in timers.values()):
            for name, timer in timers.items():
                timers[name] = sec if timer is None else timer
            return False
        if kind is None:
            return False
        gap = sec - timers[kind]
        if self.bug:
            gap %= DAY                                                   # timedelta.seconds
        if gap <= ALERT_INTERVAL:
            return False
        timers[kind] = sec
        key, _, flag = ALERT_TYPES[kind]
        self.alerts.append(Alert(sec, wire, kind, flag, str(float(value)), THRESHOLDS[wire][key]))
        return True

    def store_raw_sensor(self) -> None:
        """Device._store_raw_sensor: detach a silent wire, then insert raw_sensor_info when due."""
        raw = self.raw
        if raw["created"] is None:
            return
        for i in range(3):
            if raw["times"][i] is not None and raw["created"] - raw["times"][i] > RESET_AFTER:
                raw["values"][i] = raw["times"][i] = self.status[i] = None
            elif raw["times"][i] is None and raw["values"][i] is not None:
                raw["times"][i] = raw["created"]
        gap = raw["created"] - self.raw_pre["created"] if self.raw_pre else 0
        changed = self.raw_pre is not None and self.raw_pre["values"] != raw["values"]
        if self.raw_pre is None or (changed and gap >= 60) or gap >= 300 or raw["by_pass"]:
            raw["by_pass"] = False
            self.raw_rows.append((raw["created"], tuple(raw["values"])))
            self.raw_pre = {"created": raw["created"], "values": list(raw["values"])}

    def check_health(self, sec: int) -> None:
        """DeviceHealth._check_temperature_error_status (ref_time = packet time, data sent after the fact)."""
        h, pre = self.h, self.pre
        for i in range(3):
            h["last"][i] = sec if h["last"][i] is None else h["last"][i]
            h["valid"][i] = h["last"][i] if h["valid"][i] is None else h["valid"][i]
            h["chg"][i] = h["valid"][i] if h["chg"][i] is None else h["chg"][i]
            checks = [("TEMP-01", "last", sec), ("TEMP-02", "valid", h["last"][i]),
                      ("TEMP-03", "chg", h["valid"][i])]
            active = next((label for label, key, ref in checks if not advanced(h[key][i], pre[key][i])
                           and seconds_between(ref, pre[key][i]) >= TEMP_INTERVAL), None)
            recovered = next(((label, pre[key][i], h[key][i]) for label, key, _ in checks
                              if advanced(h[key][i], pre[key][i]) and h[key][i] - pre[key][i] >= TEMP_INTERVAL), None)
            if recovered:
                self.summaries.append(Summary(*recovered, description=f"temperature-{i + 1}"))
            h["error"][i] = active
        self.pre = deepcopy(h)


def simulated(family) -> tuple[list[dict], Kernel, Kernel]:
    """Records with the family encoding (io) and parser result (readings), simulated correct and as the kernel is."""
    rows = records()
    for row in rows:
        row["io"], row["readings"] = family.read(row)
    sims = []
    for bug in (False, True):
        kernel = Kernel(bug)
        for row in rows:
            kernel.message(row, row["readings"])
        sims.append(kernel)
    return rows, *sims


# ---------------- expected ----------------
def step_name(sec: int, last_sec: int) -> str:
    return "end" if sec == last_sec else f"cek_h{1 + sec // DAY}_{fmt(sec)[3:].replace(':', '')}"


def approx(values) -> list:
    return [None if value is None else {"approx": value, "tolerance": TOLERANCE} for value in values]


def bug_note(correct, kernel, reason: str) -> dict:
    return {} if correct == kernel else {"known_bug": reason}


def alert_where(alert: Alert) -> dict:
    return {"vehicle_id": "$vehicle_id", "alert_code": ALERT_CODES[alert.wire], "created_on": fmt(alert.sec)}


def alert_checks(correct: Kernel, kernel: Kernel) -> dict[int, list]:
    """raw_alert TEMPERATURE_N per alert (correct), and count 0 where only the kernel makes one."""
    checks: dict[int, list] = {}
    kernel_alerts = set(kernel.alerts)
    for alert in correct.alerts:
        checks.setdefault(alert.sec, []).append({
            "table": "raw_alert", "where": alert_where(alert), "count": 1,
            "match": {"alert_flag": alert.flag, "alert_data": alert.data, "alert_trigger": alert.trigger},
            **({} if alert in kernel_alerts else {"known_bug": kernel_bug(alert)}),
            "note": f"TEMPERATURE_{alert.wire} ({ALERT_CODES[alert.wire]}) {alert.kind.replace('_', ' ')} "
                    f"{'jalan' if alert.flag else 'tidak jalan'}, alert_trigger = batas",
        })
    for alert in sorted(kernel_alerts - set(correct.alerts)):
        checks.setdefault(alert.sec, []).append({
            "table": "raw_alert", "where": alert_where(alert), "count": 0,
            "known_bug": "jeda alert pakai timedelta.seconds",
            "note": f"TEMPERATURE_{alert.wire} ({ALERT_CODES[alert.wire]}): seharusnya tidak ada",
        })
    return checks


def kernel_bug(alert: Alert) -> str:
    key = ALERT_TYPES[alert.kind][0]
    if THRESHOLDS[alert.wire][key] == 0:
        return f"{key} 0 diabaikan (if {key} and ...)"
    return "jeda alert pakai timedelta.seconds: jeda 1 hari + x dihitung x detik"


def status_alert_checks(kernel: Kernel) -> dict[int, list]:
    """raw_alert 76 per status change. A run of changes every record (flag true / false in turn) = 1 count check."""
    runs: list[list] = []
    for alert in kernel.status_alerts:
        same_run = runs and runs[-1][-1][1] == alert[1] and alert[0] - runs[-1][-1][0] <= 10
        if same_run:
            runs[-1].append(alert)
        else:
            runs.append([alert])
    checks: dict[int, list] = {}
    for run in runs:
        if len(run) > 4:
            (start, wire, _, _), end = run[0], run[-1][0]
            checks.setdefault(end, []).append({
                "table": "raw_alert",
                "where": {"vehicle_id": "$vehicle_id", "alert_code": STATUS_ALERT, "alert_trigger": PINS[wire],
                          "created_on": {"between": [fmt(start), fmt(end)]}},
                "count": len(run), "note": f"DALLAS_TEMPERATURE_STATUS wire {wire} flag true / false bergantian tiap "
                                           "paket (error / bukan error)",
            })
            continue
        for sec, wire, flag, data in run:
            checks.setdefault(sec, []).append({
                "table": "raw_alert",
                "where": {"vehicle_id": "$vehicle_id", "alert_code": STATUS_ALERT, "alert_trigger": PINS[wire],
                          "created_on": fmt(sec)},
                "count": 1, "match": {"alert_flag": flag, "alert_data": data},
                "note": f"DALLAS_TEMPERATURE_STATUS ({STATUS_ALERT}) wire {wire} {'error' if flag else 'normal'}, "
                        "disimpan hanya saat status berubah",
            })
    return checks


def vehicle_status_checks(kernel: Kernel, extra: set[int]) -> dict[int, list]:
    """vehicle_status.temperature where a wire becomes NULL / comes back, at the first message and at `extra`."""
    checks: dict[int, list] = {}
    previous = None
    for sec, values in kernel.vehicle_status.items():
        pattern = [value is None for value in values]
        if previous is None or pattern != previous or sec in extra:
            reset = [f"wire {i + 1} NULL" for i, (old, new) in enumerate(zip(previous or [False] * 3, pattern))
                     if new and not old]
            note = "temperature wire 1-3 setelah pesan"
            if reset:
                note += f": {', '.join(reset)} (reset > 10 menit tanpa data)"
            checks.setdefault(sec, []).append({
                "table": "vehicle_status", "where": {"vehicle_id": "$vehicle_id"},
                "match": {"temperature": approx(values)}, "timeout": "00:01:00", "note": note,
            })
        previous = pattern
    return checks


def raw_sensor_rows(correct: Kernel, kernel: Kernel) -> dict[int, list]:
    """raw_sensor_info: first row, first row with a reset wire, first row after it comes back, row of each alert."""
    alert_secs = {alert.sec for alert in correct.alerts}
    picked, previous = [], None
    for created, values in correct.raw_rows:
        pattern = [value is None for value in values]
        if previous is None or pattern != previous or created in alert_secs:
            picked.append((created, values))
        previous = pattern
    kernel_rows = dict(kernel.raw_rows)
    checks: dict[int, list] = {}
    for created, values in picked:
        checks.setdefault(created, []).append({
            "table": "raw_sensor_info", "where": {"vehicle_id": "$vehicle_id", "created_on": fmt(created)},
            "count": 1, "match": {"temperature": approx(values[:1])[0], "temperature2": approx(values[1:2])[0],
                                  "temperature3": approx(values[2:])[0]},
            **bug_note(values, kernel_rows.get(created), "alert hilang karena bug kernel, baris alert tidak ada"),
            "note": "baris raw_sensor_info" + (" saat alert (insert_by_pass)" if created in alert_secs
                                               else " (NULL = wire di-reset)"),
        })
    return checks


def raw_sensor_counts(correct: Kernel, kernel: Kernel, windows: list[tuple[int, int, str]]) -> list[dict]:
    def count(sim, start=None, end=None):
        return sum(1 for created, _ in sim.raw_rows if start is None or start <= created <= end)

    checks = [{"table": "raw_sensor_info", "where": {"vehicle_id": "$vehicle_id"}, "count": count(correct),
               **bug_note(count(correct), count(kernel), "alert hilang karena bug kernel, baris alert tidak ada"),
               "note": "jumlah baris: pertama, berubah >= 60 detik, >= 300 detik, atau ada alert"}]
    for start, end, note in windows:
        checks.append({
            "table": "raw_sensor_info", "where": {"vehicle_id": "$vehicle_id",
                                                  "created_on": {"between": [fmt(start), fmt(end)]}},
            "count": count(correct, start, end),
            **bug_note(count(correct, start, end), count(kernel, start, end), "alert hilang karena bug kernel"),
            "note": note,
        })
    return checks


def alert_totals(correct: Kernel, kernel: Kernel, has_status: bool) -> list[dict]:
    checks = []
    for wire, code in ALERT_CODES.items():
        total = sum(1 for alert in correct.alerts if alert.wire == wire)
        kernel_total = sum(1 for alert in kernel.alerts if alert.wire == wire)
        checks.append({"table": "raw_alert", "where": {"vehicle_id": "$vehicle_id", "alert_code": code},
                       "count": total, **bug_note(total, kernel_total, "alert temperature, lihat cek per alert"),
                       "note": f"jumlah TEMPERATURE_{wire} ({code})"})
    if has_status:
        checks.append({"table": "raw_alert", "where": {"vehicle_id": "$vehicle_id", "alert_code": STATUS_ALERT},
                       "count": len(kernel.status_alerts), "note": "jumlah DALLAS_TEMPERATURE_STATUS (76)"})
    return checks


def merge(*parts: dict[int, list]) -> dict[int, list]:
    merged: dict[int, list] = {}
    for part in parts:
        for sec, items in part.items():
            merged.setdefault(sec, []).extend(items)
    return dict(sorted(merged.items()))


def verify(summaries: list[Summary], health: list) -> None:
    """Generator guard: the health summaries written in the description still match the simulation."""
    actual = sorted((item.label, item.description, item.start, item.end) for item in summaries)
    if actual != sorted(item[1:5] for item in health):
        found = [(label, description, fmt(start), fmt(end)) for label, description, start, end in actual]
        raise ValueError(f"device health changed: {found}")


def db_requirements(family: str, model: str, imei: str) -> list[str]:
    lines = [f"device {family}, {model} -> imei {imei}", f"sensor speed (sensor_code {v2_setup.SENSOR_CODES['speed']})"]
    for sensor in SENSORS:
        wire = int(sensor["code"][-1])
        settings = ", ".join(f"{key} {value}" for key, value in THRESHOLDS[wire].items()) or "tanpa batas alert"
        lines.append(f"sensor {sensor['code']} (sensor_code {v2_setup.SENSOR_CODES[sensor['code']]}, pin_code "
                     f"{v2_setup.PIN_CODES[sensor['pin']]} ({sensor['pin']}), {settings})")
    return lines


def build(family) -> tuple[dict, dict]:
    """family module: USECASE, FAMILY, MODEL, IMEI, HAS_STATUS, UNIQUE, CASES, HEALTH, read(record),
    extra_checks(rows, last_sec) -> {second: [check]}."""
    rows, correct, kernel = simulated(family)
    if (correct.vehicle_status, correct.status_alerts, correct.summaries) != \
            (kernel.vehicle_status, kernel.status_alerts, kernel.summaries):
        raise ValueError("kernel bug simulation changed more than the temperature alerts")
    summaries = correct.summaries + simulate_gps(rows)
    health = COMMON_HEALTH + family.HEALTH
    verify(summaries, health)

    last_sec = rows[-1]["sec"]
    checks = merge(
        family.extra_checks(rows, last_sec), alert_checks(correct, kernel), status_alert_checks(kernel),
        vehicle_status_checks(kernel, {*BOUNDS, SENSOR_85, rows[0]["sec"]}), raw_sensor_rows(correct, kernel),
    )
    wait_last = {"table": "vehicle_status", "where": {"vehicle_id": "$vehicle_id"},
                 "match": {"last_packet": "$step_at"}, "timeout": "00:10:00",
                 "note": "tunggu kernel selesai memproses data terakhir"}
    checks[last_sec] = [
        wait_last, *checks.get(last_sec, []), *alert_totals(correct, kernel, family.HAS_STATUS),
        *raw_sensor_counts(correct, kernel, RAW_WINDOWS), *health_checks(summaries),
    ]
    checks = add_health_status(checks, correct.health_status, last_sec, sensor="temperature")

    for row in rows:
        if row["sec"] in checks or row["sec"] == last_sec:
            row["step"] = step_name(row["sec"], last_sec)
    steps = {step_name(sec, last_sec): {"db": items} for sec, items in sorted(checks.items())}

    raw_counts = tuple(sum(1 for created, _ in correct.raw_rows if start <= created <= end)
                       for start, end, _ in RAW_WINDOWS)
    case = {
        "usecase": family.USECASE,
        "description": f"Unik {family.FAMILY}:\n" + "".join(f"  {line}\n" for line in family.UNIQUE)
                       + describe_cases(cases(family, raw_counts), health),
        "db_requirements": db_requirements(family.FAMILY, family.MODEL, family.IMEI),
        "families": [family.FAMILY],
        "start": "today-2d",
        "interval": "00:00:10",
        "tags": ["temperature"],
        "imei": {family.FAMILY: family.IMEI},
        "models": {family.FAMILY: family.MODEL},
        "carry_over": False,
        "setup": {"sensors": SENSORS},
        "timeline": to_case_timeline(rows),
    }
    return case, {family.USECASE: {"default": {"steps": steps}}}
