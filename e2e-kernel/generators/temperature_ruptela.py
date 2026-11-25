"""temperature_ruptela: dallas temperature wire 1-3 on a ruptela (IO 78 / 79 / 80, signed 0.1 C).

python cli.py generate --usecase temperature_ruptela
Timeline, common cases and kernel simulation: generators/temperature_common.py.
Kernel family/ruptela/ruptela.py decode_io (enums/ruptela_enum.py _TEMPERATURE_ERROR_VALUES, checked on the
unsigned value): 2000-2004 / 850 = error, status alarm True with the error text as alert_data, no sensor packet.
min_value / max_value (-550 .. 1250) are not checked: every other value is a temperature.
"""
import sys

from generators import temperature_common as tc
from generators.health_common import is_in, m

USECASE = "temperature_ruptela"
FAMILY = "ruptela"
MODEL = "fm_pro4"
IMEI = "352000000000011"
HAS_STATUS = True
ERRORS = {2000: "No device", 2001: "Faulty bus", 2002: "CRC error", 2003: "No data", 2004: "Out of range",
          850: "Power supply too low"}
LOST = 2000
FAULTS = dict(zip(tc.FAULTS, (2001, 2002, 2003, 2004)))

UNIQUE = [
    "nilai dallas mentah 0.1 C. Kode error 2000-2004 / 850 -> raw_alert 76 flag true, alert_data = teks error",
    "(No device, Faulty bus, CRC error, No data, Out of range, Power supply too low), suhu tidak dipakai.",
    "Tidak ada batas -55..125 C: 125.1 / -55.1 tetap disimpan. 85.0 C = kode 850, bukan suhu.",
]

FIRST_DB = "raw_alert 76 flag false per wire (status pertama), vehicle_status.temperature, raw_sensor_info baris 1"
CASES = [
    ("E", "jalan: sensor wire 3 lepas 02:30:00-02:50:00, kirim 2000 (No device)",
     "raw_alert 76 wire 3 flag true 'No device' 02:30:00, flag false 02:50:00", None,
     "02:40:00 (> 10 menit tanpa suhu wire 3): vehicle_status.temperature[3] + raw_sensor_info.temperature3 NULL, "
     "terisi lagi 02:50:00"),
    ("F", "jalan: wire 2 satu paket error 03:00:00 2001, 03:05:00 2002, 03:10:00 2003, 03:15:00 2004",
     "raw_alert 76 wire 2 flag true (Faulty bus, CRC error, No data, Out of range), flag false di paket "
     "berikutnya: 8 baris", "tidak ada (< 11 menit)", "suhu wire 2 tetap nilai terakhir"),
    ("H", "jalan: wire 3 125.0 / 125.1 / -55.0 / -55.1 C 03:50:00-03:50:30",
     "tidak ada (wire 3 tanpa batas alert)", None, "semua disimpan, tidak ada batas -55..125"),
    ("I", "jalan: wire 3 85.0 C 03:55:00", "raw_alert 76 wire 3 flag true 'Power supply too low', flag false 03:55:10",
     None, "85.0 tidak disimpan"),
    ("R", "jalan: chiller 85.0 C H2 08:00:00-08:29:50", "raw_alert 76 wire 2 flag true 'Power supply too low', "
     "flag false 08:30:00", None, "chiller tetap nilai terakhir lalu NULL setelah > 10 menit"),
    ("S", "jalan bersamaan H2 09:00:00-09:30:00: wire 1 tidak dikirim, chiller 3.5 C tetap, kabin 85.0 / 150.0 "
     "bergantian", "raw_alert 76 wire 3 flag true (85.0) / false (150.0 suhu valid) tiap paket",
     "kabin: 150.0 selalu sama -> TEMP-03 (bukan TEMP-02)"),
    ("T", "jalan: kabin 150.0 C H2 10:00:00-10:29:50", "tidak ada (kabin tanpa batas alert)",
     "150.0 disimpan dan tidak berubah: TEMP-03"),
    ("V", "jalan: kabin 85.0 C H2 12:15:00-12:29:50 lalu 150.0 C 12:30:00-12:44:50",
     "raw_alert 76 wire 3 flag true 12:15:00, flag false 12:30:00", None),
    ("X", "jalan: semua wire 85.0 C H2 13:45:00-14:14:50", "raw_alert 76 wire 1-3 flag true 'Power supply too low', "
     "flag false 14:15:00", None, "semua wire NULL setelah > 10 menit"),
]
HEALTH = [
    ("E", "TEMP-02", "temperature-3", m(2, 29, 50), m(2, 50), "data error >= 11 menit (aktif 02:40:50)"),
    ("R", "TEMP-02", "temperature-2", tc.h2(7, 59, 50), tc.h2(8, 30), None),
    ("S", "TEMP-03", "temperature-3", tc.h2(9, 0, 10), tc.h2(9, 30), None),
    ("T", "TEMP-03", "temperature-3", tc.h2(10), tc.h2(10, 30), None),
    ("V", "TEMP-02", "temperature-3", tc.h2(12, 14, 50), tc.h2(12, 30), "85.0 = error"),
    ("V", "TEMP-03", "temperature-3", tc.h2(12, 30), tc.h2(12, 45), "150.0 tetap"),
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
        io[f"temperature_{wire}"] = raw
        readings[wire] = decode(raw)
    return io, readings


def decode(raw: int) -> tc.Reading:
    error = ERRORS.get(raw & 0xFFFF)
    if error:
        return tc.Reading(None, (True, error))
    value = round(raw * 0.1, 6)
    return tc.Reading(value, (False, str(value)))


def extra_checks(rows: list[dict], last_sec: int) -> dict:
    return {}


def build() -> tuple[dict, dict]:
    return tc.build(sys.modules[__name__])
