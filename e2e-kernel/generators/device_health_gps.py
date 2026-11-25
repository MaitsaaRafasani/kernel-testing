"""device_health_gps: GPS-01 / GPS-02 / GPS-03 cases of healthExpectedResult.md on a real road loop.

python cli.py generate --usecase device_health_gps
Same case schedule as the kernel scenario (device_health_GPS.txt), but the truck drives routes/health_loop.json
with realistic values. Expected = simulated kernel DeviceHealth (GNSS_POSITIONING=false): GPS-03 active is kept
while the engine is off and ends only when the location changes (kernel commit 0acb5aada, case J).
"""
from generators.health_common import (
    DAY, Odometer, drive_speed, expected_checks, m, satellites, schedule, simulate_gps, to_case_timeline,
    voltage,
)

USECASE = "device_health_gps"
TOTAL = 2 * DAY
IMEI = "350000000000012"

# (start, end, mode, label). The first matching block wins, so nested blocks come first.
BLOCKS = [
    (m(1), m(1, 30), "noloc", "A  GPS-01: tidak ada data lokasi 30 menit (mis. terowongan / gangguan modul GPS)"),
    (m(2), m(2, 15), "noloc", "B  tidak ada lokasi 15 menit (< 16 menit) -> tidak error"),
    (m(3), m(3, 30), "lowsat", "C  GPS-02: satelit 3 selama 30 menit"),
    (m(4), m(4, 30), "zero", "D  GPS-02: koordinat 0,0 selama 30 menit"),
    (m(5), m(5, 15), "lowsat", "E  satelit rendah 15 menit (< 16 menit) -> tidak error"),
    (m(6), m(6, 20), "lowsat", "F1 GPS-02 20 menit"),
    (m(6, 20), m(7), "noloc", "F2 lalu tanpa lokasi 40 menit (GPS-02 -> GPS-01)"),
    (m(8), m(9, 30), "park_on", "G  GPS-03: parkir mesin hidup 1,5 jam lalu jalan"),
    (m(10), m(10, 50), "park_on", "H  parkir mesin hidup 50 menit (< 1 jam) -> tidak error"),
    (m(11), m(13), "park_off", "I  parkir mesin mati 2 jam -> tidak error"),
    (m(14), m(15, 30), "park_on", "J1 parkir mesin hidup 1,5 jam"),
    (m(15, 30), m(16), "park_off", "J2 lalu mesin mati 30 menit di tempat yang sama, lalu jalan -> GPS-03 aktif sejak "
     "15:00 tidak di-reset mesin mati, 1 summary 14:00:00 - 16:00:10 (lokasi pertama berubah)"),
    (m(17), m(18, 30), "park_jitter", "K  parkir mesin hidup 1,5 jam dengan GPS drift ±1 m"),
    (m(19), m(20, 10), "park_on", "L1 parkir mesin hidup (GPS-03 di 20:00)"),
    (m(20, 10), m(20, 40), "park_lowsat", "L2 masih parkir, satelit rendah 30 menit (GPS-02)"),
    (m(20, 40), m(21), "park_on", "L3 GPS valid lagi di tempat yang sama, jalan 21:00"),
    (m(22), m(22, 40), "off_noloc", "V  acc off: tanpa lokasi 40 menit (GPS-01)"),
    (m(22, 50), m(23, 20), "off_lowsat", "W  acc off: satelit rendah 30 menit (GPS-02)"),
    (m(21, 30), m(23, 30), "park_off", "Z1 parkir malam mesin mati 2 jam"),
    (m(23, 50), m(0, 20, day=1), "noloc", "N  GPS-01 melewati tengah malam"),
    (m(2, day=1), m(5, day=1), "noloc", "O  GPS-01 panjang 3 jam"),
    (m(6, day=1), m(6, 20, day=1), "lowsat", "P1 GPS-02 20 menit"),
    (m(6, 20, day=1) + 10, m(6, 40, day=1), "lowsat", "P2 1 data valid di 06:20, lalu GPS-02 lagi 20 menit"),
    (m(8, day=1), m(12, day=1), "park_on", "Q  GPS-03 panjang: parkir mesin hidup 4 jam"),
    (m(13, day=1), m(13, 20, day=1), "sat5", "R1 satelit tepat 5 (batas tidak valid) 20 menit"),
    (m(14, day=1), m(14, 20, day=1), "sat6", "R2 satelit tepat 6 (batas valid) 20 menit -> tidak error"),
    (m(15, day=1), m(16, 10, day=1), "park_on", "T1 parkir mesin hidup (GPS-03 di 16:00)"),
    (m(16, 10, day=1), m(16, 40, day=1), "park_noloc", "T2 masih parkir, tanpa lokasi 30 menit (GPS-01)"),
    (m(16, 40, day=1), m(17, day=1), "park_on", "T3 lokasi kembali di tempat yang sama, jalan 17:00"),
    (m(18, day=1), m(18, 30, day=1), "gnss_off", "U1 GNSS mati (gnssStatus 0) 30 menit"),
    (m(19, day=1), m(19, 30, day=1), "gnss_nofix", "U2 GNSS hidup tanpa fix (gnssStatus 2) 30 menit"),
    (m(20, day=1), m(20, 10, day=1), "gnss_nofix", "U3 GNSS tanpa fix 10 menit (< 16 menit) -> tidak error"),
    (m(21, day=1), m(21, 30, day=1), "gnss_sleep", "U4 GNSS sleep (gnssStatus 3) 30 menit"),
    (m(22, day=1), TOTAL, "park_off", "Z2 parkir malam mesin mati sampai akhir"),
]
OFF = [(start, end) for start, end, mode, _ in BLOCKS if mode == "park_off"]
PARKED = ("park_", "off_")
GNSS = {"gnss_off": 0, "gnss_nofix": 2, "gnss_sleep": 3}
LOW_SATELLITES = {"lowsat": 3, "park_lowsat": 3, "off_lowsat": 3, "sat5": 5, "sat6": 6}
DRIFT = 0.00001                  # degrees, ±1 m while parked

DB_REQUIREMENTS = [
    f"IMEI {IMEI} (model fmb125) terdaftar di device, vehicle, vehicle_status. Vehicle sendiri, tidak dipakai "
    "usecase lain (pin sensor bisa bentrok).",
    "vehicle_status sudah di-reset (sentinel 2024-04-03 07:00:00 +0700) sebelum run.",
    "Sensor speed terdaftar. Sensor ACCUMULATOR (pin VCC) dibuat otomatis oleh kernel: dipakai untuk mesin hidup "
    "(GPS-03).",
]


def block_at(sec: int) -> str:
    return next((mode for start, end, mode, _ in BLOCKS if start <= sec < end), "drive")


def records() -> list[dict]:
    odometer, rows = Odometer(), []
    for sec, acc_on in schedule(TOTAL, OFF):
        mode = block_at(sec)
        driving = acc_on and not mode.startswith(PARKED)
        speed = drive_speed(sec) if driving else 0
        lat, lng = odometer.advance(sec, speed, driving)
        if mode == "park_jitter":
            lng = round(lng + (DRIFT if (sec // 10) % 2 else -DRIFT), 6)
        if mode == "zero":
            lat, lng = 0.0, 0.0
        rows.append({
            "sec": sec, "lat": lat, "lng": lng, "speed": speed,
            "sats": LOW_SATELLITES.get(mode, satellites(sec)),
            "acc": None if mode.endswith("noloc") else acc_on,
            "volt": voltage(sec, acc_on),
            "io": {"gnss": GNSS.get(mode, 1)},
        })
    return rows


def build() -> tuple[dict, dict]:
    data = records()
    summaries = simulate_gps(data)
    cases = "\n".join(f"  {label}" for _, _, _, label in BLOCKS)
    case = {
        "usecase": USECASE,
        "description": f"""\
Dibuat oleh generators/device_health_gps.py (python cli.py generate --usecase {USECASE}): ubah generator, bukan Excel.
Device health GPS, 2 hari, truk di rute jalan asli (routes/health_loop.json, Cakung - Cikarang - Karawang, loop).
Nilai seperti kendaraan asli: kecepatan 24-52 km/jam sesuai jarak antar data, satelit 9-13, aki 13,9-14,3 V
mesin hidup / 12,4-12,6 V mesin mati, data tiap 10 detik (acc on) / 5 menit (acc off).
Jadwal kasus sama dengan healthExpectedResult.md, expected = simulasi DeviceHealth (jarak description dari rute ini).
Expected untuk env kernel GNSS_POSITIONING=false.
Mode cepat (waktu lampau): butuh kernel dengan heartbeat balasan Teltonika memakai waktu record terakhir
(commit 24cff0d1e).
Kasus:
{cases}
""",
        "db_requirements": DB_REQUIREMENTS,
        "families": ["teltonika"],
        "start": "today-2d",
        "tags": ["device_health"],
        "imei": {"teltonika": IMEI},
        "models": {"teltonika": "fmb125"},
        "interval": "00:00:10",
        "carry_over": False,
        "timeline": to_case_timeline(data),
    }
    return case, expected_checks(USECASE, summaries)
