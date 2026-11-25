"""device_health_fuel: FUEL-01 / FUEL-02 / FUEL-03 cases of healthExpectedResult.md on a real road loop.

python cli.py generate --usecase device_health_fuel
Same cases as the kernel scenario (device_health_FUEL.txt), the truck drives routes/health_loop.json with realistic
values. FUEL-03 counts metres driven, so the distance cases follow real speeds: M is a traffic jam (< 10 km in
55 min), N drives 10 min before the 1 hour stop. Expected = simulated kernel DeviceHealth (FUEL + GPS).
"""
from generators.health_common import (
    DAY, FUEL_ERROR_CODE, Odometer, drive_speed, expected_checks, fmt, is_in, m, satellites, schedule,
    simulate_fuel, simulate_gps, to_case_timeline, voltage,
)

USECASE = "device_health_fuel"
TOTAL = 2 * DAY
IMEI = "350000000000015"

# acc off (parked): a record every 5 min
OFF = [
    (m(9, 45), m(10)),                 # J: stop at a fuel station
    (m(17), m(18)),                    # N: stop in the middle of a stuck window
    (m(1, day=1), m(6, day=1)),        # night 1 (Q, R)
    (m(19, day=1), TOTAL),             # night 2 (V, W)
]
IDLE = [(m(19), m(21))]                # O: acc on but not moving
JAM = [(m(15), m(15, 55))]             # M: traffic jam 8-11 km/h

# (start, end, mode, label). The first matching block wins.
# mode: empty | sparse12 | errcode | zero | negative | jump (every other record +15) | stuck (value frozen)
BLOCKS = [
    (m(1), m(1, 30), "empty", "A  FUEL-01: data fuel hilang 30 menit"),
    (m(2), m(2, 10), "empty", "B  data fuel hilang 10 menit (< 11 menit) -> tidak error"),
    (m(3), m(4), "sparse12", "C  data fuel jarang: 1 data tiap 12 menit (5 x FUEL-01)"),
    (m(4, 30), m(5), "errcode", "D  FUEL-01: sensor mengirim error code -0.4 selama 30 menit"),
    (m(6, 30), m(7), "zero", "F  FUEL-02: nilai 0 selama 30 menit"),
    (m(7, 30), m(7, 40), "zero", "G  nilai 0 selama 10 menit (< 11 menit) -> tidak error"),
    (m(8), m(8, 30), "negative", "H  FUEL-02: nilai negatif -1.00 selama 30 menit"),
    (m(9), m(9, 30), "jump", "I  FUEL-02: nilai loncat 15 tiap data selama 30 menit"),
    (m(11), m(11, 20), "zero", "K1 FUEL-02 20 menit"),
    (m(11, 20), m(12), "empty", "K2 lalu tanpa data 40 menit (FUEL-02 -> FUEL-01)"),
    (m(13), m(14, 10), "stuck", "L  FUEL-03: nilai tetap sambil jalan 70 menit (> 10 km)"),
    (m(15), m(15, 55), "stuck", "M  nilai tetap 55 menit di kemacetan (< 10 km) -> tidak error"),
    (m(16, 50), m(18, 40), "stuck", "N  FUEL-03: nilai tetap, jalan 10 menit, parkir 1 jam, jalan lagi"),
    (m(19), m(21), "stuck", "O  nilai tetap 2 jam, acc on tapi diam -> tidak error"),
    (m(23, 10), m(0, 30, day=1), "stuck", "P  FUEL-03 melewati tengah malam"),
    (m(1, 30, day=1), m(2, 10, day=1), "empty", "Q  acc off: data fuel hilang 40 menit"),
    (m(3, day=1), m(6, day=1), "stuck", "R  acc off: nilai tetap 3 jam (parkir) -> tidak error"),
    (m(7, day=1), m(8, 10, day=1), "stuck", "S1 FUEL-03: nilai tetap sambil jalan 70 menit"),
    (m(8, 10, day=1), m(8, 40, day=1), "empty", "S2 lalu tanpa data 30 menit (FUEL-03 -> FUEL-01)"),
    (m(10, day=1), m(10, 5, day=1), "zero", "T2 nilai 0 selama 5 menit di tengah nilai tetap"),
    (m(9, 30, day=1), m(10, 40, day=1), "stuck", "T1 FUEL-03: nilai tetap sambil jalan 70 menit"),
    (m(13, day=1), m(16, day=1), "stuck", "U  FUEL-03 panjang: nilai tetap sambil jalan 3 jam"),
    (m(20, day=1), m(20, 30, day=1), "zero", "V  acc off: nilai 0 selama 30 menit (FUEL-02)"),
    (m(21, 30, day=1), m(23, 30, day=1), "empty", "W  acc off: data fuel hilang 2 jam"),
]
OFFLINE = [(m(5, 30), m(6))]           # E: device sends nothing (FUEL-01 + GPS-01)

# ultrasonic level (cm) of a 400 L truck tank (~6.7 L per cm): 0.03 cm per km driven (~0.2 L/km), one refuel
START_LEVEL = 40.0
CM_PER_KM = 0.03
REFUEL = (m(10), 18.0)                 # J: +18 cm at the fuel station, one jump > 7 = one invalid record
JUMP = 15

DB_REQUIREMENTS = [
    f"IMEI {IMEI} (model fmb125) terdaftar di device, vehicle, vehicle_status. Vehicle sendiri, tidak dipakai "
    "usecase lain (pin sensor bisa bentrok).",
    "vehicle_status sudah di-reset (sentinel 2024-04-03 07:00:00 +0700) sebelum run.",
    "Sensor speed terdaftar. Sensor ACCUMULATOR (pin VCC) dibuat otomatis oleh kernel.",
    "Sensor fuel di pin RS232, status_float kosong (batas loncatan default 7; jika >= 15 hasil kasus I berubah).",
]

# summaries per type the cases must produce (healthExpectedResult.md FUEL)
EXPECTED_FUEL = {"FUEL-01": 12, "FUEL-02": 4, "FUEL-03": 5}


def level(km: float, sec: int, previous: float | None) -> float:
    """Level dropping with the distance, sensor noise ±0.15 cm that changes on every record."""
    value = START_LEVEL - CM_PER_KM * km + (REFUEL[1] if sec >= REFUEL[0] else 0)
    value = round(value + ((sec // 10 * 7) % 7 - 3) / 20, 2)
    return round(value + 0.01, 2) if value == previous else value


def records() -> list[dict]:
    odometer, rows = Odometer(), []
    frozen, last_normal = {}, None
    for sec, acc_on in schedule(TOTAL, OFF):
        stopped = not acc_on or is_in(sec, IDLE)
        speed = 0 if stopped else drive_speed(sec, slow=is_in(sec, JAM))
        lat, lng = odometer.advance(sec, speed, not stopped)
        if is_in(sec, OFFLINE):
            continue
        normal = level(odometer.metres / 1000, sec, last_normal)
        fuel = normal
        stuck = next((index for index, (start, end, mode, _) in enumerate(BLOCKS)
                      if mode == "stuck" and start <= sec < end), None)
        if stuck is not None:
            fuel = frozen.setdefault(stuck, last_normal)
        for start, end, mode, _ in BLOCKS:
            if not start <= sec < end or mode == "stuck":
                continue
            if mode == "empty" or (mode == "sparse12" and (sec - start) % 720):
                fuel = None
            elif mode == "errcode":
                fuel = FUEL_ERROR_CODE
            elif mode == "zero":
                fuel = 0.0
            elif mode == "negative":
                fuel = -1.0
            elif mode == "jump":
                fuel = round(normal + (JUMP if (sec - start) // 10 % 2 == 0 else 0), 2)
            break
        if stuck is None:
            last_normal = normal
        rows.append({
            "sec": sec, "lat": lat, "lng": lng, "speed": speed, "sats": satellites(sec), "acc": acc_on,
            "volt": voltage(sec, acc_on), "fuel": fuel,
            "io": {} if fuel is None else {"ultrasonic_fuel": round(fuel * 100)},
        })
    return rows


def build() -> tuple[dict, dict]:
    data = records()
    fuel_summaries, _ = simulate_fuel(data)
    counts = {label: sum(1 for item in fuel_summaries if item.label == label) for label in EXPECTED_FUEL}
    if counts != EXPECTED_FUEL:
        raise ValueError(f"fuel cases changed: {counts}, expected {EXPECTED_FUEL}")
    summaries = fuel_summaries + simulate_gps(data)
    lowest = min(row["fuel"] for row in data if row["fuel"] and row["fuel"] > 1)
    cases = "\n".join(f"  {label}" for _, _, _, label in BLOCKS)
    case = {
        "usecase": USECASE,
        "description": f"""\
Dibuat oleh generators/device_health_fuel.py (python cli.py generate --usecase {USECASE}): ubah generator, bukan Excel.
Device health FUEL, 2 hari, truk di rute jalan asli (routes/health_loop.json, Cakung - Cikarang - Karawang, loop).
Nilai seperti kendaraan asli: kecepatan 24-52 km/jam (macet 8-11) sesuai jarak antar data, satelit 9-13,
aki 13,9-14,3 V mesin hidup / 12,4-12,6 V mesin mati, data tiap 10 detik (acc on) / 5 menit (acc off).
Level BBM ultrasonic (cm, tangki 400 L): mulai {START_LEVEL} cm, turun {CM_PER_KM} cm per km, noise ±0,15 cm,
isi BBM +{REFUEL[1]} cm di SPBU H1 09:45-10:00 (level terendah {lowest} cm).
Kasus jarak (FUEL-03 = 10 km) disesuaikan dengan kecepatan asli: M = macet, N = jalan 10 menit sebelum parkir.
Expected = simulasi DeviceHealth (FUEL + GPS: offline E juga GPS-01, idle O dengan mesin hidup juga GPS-03).
Mode cepat (waktu lampau): butuh kernel dengan heartbeat balasan Teltonika memakai waktu record terakhir
(commit 24cff0d1e).
Kasus (E: device offline {fmt(OFFLINE[0][0])} - {fmt(OFFLINE[0][1])}):
{cases}
""",
        "db_requirements": DB_REQUIREMENTS,
        "families": ["teltonika"],
        "start": "today-2d",
        "tags": ["device_health"],
        "imei": {"teltonika": IMEI},
        "models": {"teltonika": "fmb125"},
        "setup": {"sensors": [{"code": "fuel", "pin": "rs232"}]},
        "interval": "00:00:10",
        "carry_over": False,
        "timeline": to_case_timeline(data),
    }
    return case, expected_checks(USECASE, summaries)
