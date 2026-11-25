"""temperature_teltonika: dallas temperature wire 1-3 on a teltonika (IO 72 / 73 / 74, signed 0.1 C).

python cli.py generate --usecase temperature_teltonika
Timeline, common cases and kernel simulation: generators/temperature_common.py.
Kernel family/teltonika/teltonika.py filter_dallas_temperature:
- 850 / 2000 / 3000 / 4000 / 5000 = error: status alarm True (alert_data = code), no sensor packet
- outside -55 .. 125 C: no sensor packet, status alarm False (alert_data = value)
- dallas id IO 76 = 0 with temperature 0: error -127 (generate_avl_parse8_byte)
"""
import sys

from generators import temperature_common as tc
from generators.health_common import is_in, m

USECASE = "temperature_teltonika"
FAMILY = "teltonika"
MODEL = "fmb125"
IMEI = "350000000000011"
HAS_STATUS = True
ERRORS = (850, 2000, 3000, 4000, 5000)       # 850 not ready, 2000 read error, 3000 not connected, 4000 id, 5000 same id
LOST = 3000
FAULTS = dict(zip(tc.FAULTS, (850, 2000, 4000, 5000)))
ID_ZERO = [(m(3, 40), m(3, 40, 10)), (tc.h2(13), tc.h2(13, 30))]       # G 1 record, W 30 min: wire 1

UNIQUE = [
    "nilai dallas mentah 0.1 C. Kode error 850 / 2000 / 3000 / 4000 / 5000 -> raw_alert 76 flag true,",
    "alert_data = kode, suhu tidak dipakai. ID dallas 0 + suhu 0 -> error -127. Di luar -55..125 C dibuang",
    "(flag false, tidak ada alert). 85.0 C = kode 850 (sensor not ready), bukan suhu.",
]

FIRST_DB = "raw_alert 76 flag false per wire (status pertama), vehicle_status.temperature, raw_sensor_info baris 1"
CASES = [
    ("E", "jalan: sensor wire 3 lepas 02:30:00-02:50:00, kirim 3000 (not connected)",
     "raw_alert 76 wire 3 flag true '3000' 02:30:00, flag false 02:50:00", None,
     "02:40:00 (> 10 menit tanpa suhu wire 3): vehicle_status.temperature[3] + raw_sensor_info.temperature3 NULL, "
     "terisi lagi 02:50:00"),
    ("F", "jalan: wire 2 satu paket error 03:00:00 850, 03:05:00 2000, 03:10:00 4000, 03:15:00 5000",
     "raw_alert 76 wire 2 flag true (alert_data = kode), flag false di paket berikutnya: 8 baris",
     "tidak ada (< 11 menit)", "suhu wire 2 tetap nilai terakhir"),
    ("G", "jalan: wire 1 ID dallas 0 + suhu 0 03:40:00",
     "raw_alert 76 wire 1 flag true '-127', flag false 03:40:10", "tidak ada (1 paket)", "suhu 0 tidak dipakai"),
    ("H", "jalan: wire 3 125.0 / 125.1 / -55.0 / -55.1 C 03:50:00-03:50:30",
     "tidak ada (wire 3 tanpa batas alert, 76 tetap flag false)", None,
     "125.0 dan -55.0 disimpan, 125.1 dan -55.1 dibuang (vehicle_status tetap nilai sebelumnya)"),
    ("I", "jalan: wire 3 85.0 C 03:55:00", "raw_alert 76 wire 3 flag true '850', flag false 03:55:10", None,
     "85.0 tidak disimpan"),
    ("R", "jalan: chiller 85.0 C H2 08:00:00-08:29:50", "raw_alert 76 wire 2 flag true '850', flag false 08:30:00",
     None, "chiller tetap nilai terakhir lalu NULL setelah > 10 menit"),
    ("S", "jalan bersamaan H2 09:00:00-09:30:00: wire 1 tidak dikirim, chiller 3.5 C tetap, kabin 85.0 / 150.0 "
     "bergantian", "raw_alert 76 wire 3 flag true (850) / false (150.0 di luar batas) tiap paket",
     "3 error berbeda bersamaan"),
    ("T", "jalan: kabin 150.0 C H2 10:00:00-10:29:50", "tidak ada", "150.0 dibuang = tidak ada data: TEMP-01",
     "kabin NULL setelah > 10 menit"),
    ("V", "jalan: kabin 85.0 C H2 12:15:00-12:29:50 lalu 150.0 C 12:30:00-12:44:50",
     "raw_alert 76 wire 3 flag true 12:15:00, flag false 12:30:00", "TEMP-02 (85) tidak jadi summary: saat pulih "
     "TEMP-01 (150 = tanpa data) yang ditulis, 1 summary per pemulihan"),
    ("W", "jalan: wire 1 ID dallas 0 + suhu 0 H2 13:00:00-13:29:50", "raw_alert 76 wire 1 flag true '-127', "
     "flag false 13:30:00", None, "wire 1 NULL setelah > 10 menit"),
    ("X", "jalan: semua wire 85.0 C H2 13:45:00-14:14:50", "raw_alert 76 wire 1-3 flag true '850', flag false "
     "14:15:00", None, "semua wire NULL setelah > 10 menit"),
]
HEALTH = [
    ("E", "TEMP-02", "temperature-3", m(2, 29, 50), m(2, 50), "data error >= 11 menit (aktif 02:40:50)"),
    ("R", "TEMP-02", "temperature-2", tc.h2(7, 59, 50), tc.h2(8, 30), None),
    ("S", "TEMP-02", "temperature-3", tc.h2(8, 59, 50), tc.h2(9, 30), None),
    ("T", "TEMP-01", "temperature-3", tc.h2(9, 59, 50), tc.h2(10, 30), None),
    ("V", "TEMP-01", "temperature-3", tc.h2(12, 29, 50), tc.h2(12, 45), None),
    ("W", "TEMP-02", "temperature-1", tc.h2(12, 59, 50), tc.h2(13, 30), None),
    *[("X", "TEMP-02", f"temperature-{wire}", tc.h2(13, 44, 50), tc.h2(14, 15), None) for wire in tc.WIRES],
]


def read(row: dict) -> tuple[dict, dict]:
    """Record -> (timeline io fields, kernel reading per wire)."""
    sec, io, readings = row["sec"], {}, {}
    for wire, value in zip(tc.WIRES, row["celsius"]):
        if value is None:
            continue                                          # IO not sent
        raw = round(value * 10)
        if wire == 3 and is_in(sec, [tc.SENSOR_LOST]):
            raw = LOST
        if wire == 2 and sec in FAULTS:
            raw = FAULTS[sec]
        id_zero = wire == 1 and is_in(sec, ID_ZERO)
        if id_zero:
            raw, io["dallas_temperature_1_id"] = 0, 0
        io[f"temperature_{wire}"] = raw
        readings[wire] = decode(raw, id_zero)
    return io, readings


def decode(raw: int, id_zero: bool) -> tc.Reading:
    if raw in ERRORS:
        return tc.Reading(None, (True, str(raw)))
    value = raw / 10
    if id_zero and value == 0:
        return tc.Reading(None, (True, "-127"))
    if -55 <= value <= 125:
        return tc.Reading(value, (False, str(value)))
    return tc.Reading(None, (False, str(value)))


def extra_checks(rows: list[dict], last_sec: int) -> dict:
    return {}


def build() -> tuple[dict, dict]:
    return tc.build(sys.modules[__name__])
