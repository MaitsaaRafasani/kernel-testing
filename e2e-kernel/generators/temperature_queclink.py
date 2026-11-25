"""temperature_queclink: dallas temperature on a queclink (+RSP ERI 1-wire block, gv58cg).

python cli.py generate --usecase temperature_queclink
Timeline, common cases and kernel simulation: generators/temperature_common.py.
Kernel family/queclink/queclink.py: each 1-wire DS18B20 = sensor packet on pin DALLAS_TEMPERATURE_GENERAL with the
sensor id, value = signed raw x 0.0625 C, no error values and no status alarm. usecase/sensor/component: a known id
uses its sensor (sensor.mac_address), a new id takes the first dallas pin without mac (wire 1, 2, 3) and the kernel
writes sensor.mac_address; a new id when every pin has a mac is ignored.
"""
import sys

from generators import temperature_common as tc
from generators.health_common import is_in, m
from versions.v2 import setup as v2_setup

USECASE = "temperature_queclink"
FAMILY = "queclink"
MODEL = "gv58cg"
IMEI = "353000000000011"
HAS_STATUS = False
IDS = {1: "28FF4A1B6416034E", 2: "28FF7C2D6416032B", 3: "28FFB13E6416035A"}     # 1-wire id per wire
UNKNOWN_ID = "28FF0D9A6416037C"               # F  sensor not registered on the vehicle
UNKNOWN_CELSIUS = 22.5
SCALE = 16                                    # raw = C x 16 (0.0625 C)

UNIQUE = [
    "sensor dikenali dari ID 1-wire, bukan pin: ID baru otomatis ke wire 1/2/3 yang belum punya mac, kernel mengisi",
    "sensor.mac_address. ID asing saat semua wire sudah punya mac diabaikan. Nilai x 0.0625 C, tanpa kode error dan",
    "tanpa raw_alert 76: sensor lepas = ID tidak dikirim (TEMP-01, bukan TEMP-02). 85.0 C dan di luar -55..125 C",
    "tetap disimpan.",
]

FIRST_DB = "sensor.mac_address wire 1/2/3 = ID 1-wire (urutan di paket), vehicle_status.temperature, " \
           "raw_sensor_info baris 1"
CASES = [
    ("E", "jalan: sensor wire 3 lepas 02:30:00-02:50:00, ID-nya tidak dikirim", "tidak ada (queclink tanpa 76)", None,
     "02:40:00 (> 10 menit tanpa suhu wire 3): vehicle_status.temperature[3] + raw_sensor_info.temperature3 NULL, "
     "terisi lagi 02:50:00"),
    ("F", f"jalan: ID asing {UNKNOWN_ID} ({UNKNOWN_CELSIUS} C) ikut dikirim 03:00:00, 03:05:00, 03:10:00, 03:15:00",
     "tidak ada", None, "diabaikan: tidak ada sensor dengan mac ID itu, suhu wire 1-3 normal"),
    ("H", "jalan: wire 3 125.0 / 125.1 / -55.0 / -55.1 C 03:50:00-03:50:30",
     "tidak ada (wire 3 tanpa batas alert)", None, "semua disimpan (125.125 / -55.125: resolusi 0.0625 C)"),
    ("I", "jalan: wire 3 85.0 C 03:55:00", "tidak ada", None, "85.0 disimpan (tidak ada kode error)"),
    ("R", "jalan: chiller 85.0 C H2 08:00:00-08:29:50", "TEMPERATURE_2 hi 08:00:00, 08:05:10, 08:10:20, 08:15:30, "
     "08:20:40, 08:25:50 (85.0 = suhu valid, > alert_hi 8, alert palsu)", "85.0 tetap: TEMP-03 (bukan TEMP-02)"),
    ("S", "jalan bersamaan H2 09:00:00-09:30:00: wire 1 tidak dikirim, chiller 3.5 C tetap, kabin 85.0 / 150.0 "
     "bergantian", "tidak ada", "kabin berubah tiap paket: tidak ada error"),
    ("T", "jalan: kabin 150.0 C H2 10:00:00-10:29:50", "tidak ada (kabin tanpa batas alert)",
     "150.0 disimpan dan tidak berubah: TEMP-03"),
    ("V", "jalan: kabin 85.0 C H2 12:15:00-12:29:50 lalu 150.0 C 12:30:00-12:44:50", "tidak ada",
     "2 kali TEMP-03 (85.0 tetap, lalu 150.0 tetap)"),
    ("X", "jalan: semua wire 85.0 C H2 13:45:00-14:14:50", "TEMPERATURE_1 dan TEMPERATURE_2 hi tiap > 300 detik "
     "13:45:00-14:10:50 (6 + 6, alert palsu)", "85.0 tetap: TEMP-03 semua wire"),
]
HEALTH = [
    ("E", "TEMP-01", "temperature-3", m(2, 29, 50), m(2, 50), "tidak ada data >= 11 menit (aktif 02:40:50)"),
    ("R", "TEMP-03", "temperature-2", tc.h2(8), tc.h2(8, 30), None),
    ("T", "TEMP-03", "temperature-3", tc.h2(10), tc.h2(10, 30), None),
    ("V", "TEMP-03", "temperature-3", tc.h2(12, 15), tc.h2(12, 30), None),
    ("V", "TEMP-03", "temperature-3", tc.h2(12, 30), tc.h2(12, 45), None),
    *[("X", "TEMP-03", f"temperature-{wire}", tc.h2(13, 45), tc.h2(14, 15), None) for wire in tc.WIRES],
]


def read(row: dict) -> tuple[dict, dict]:
    """Record -> (timeline io fields, kernel reading per wire)."""
    sec, one_wire, readings = row["sec"], {}, {}
    for wire, value in zip(tc.WIRES, row["celsius"]):
        if value is None or (wire == 3 and is_in(sec, [tc.SENSOR_LOST])):
            continue                                          # sensor lost: not in the 1-wire list
        raw = round(value * SCALE)
        one_wire[IDS[wire]] = raw
        readings[wire] = tc.Reading(raw / SCALE)
    if sec in tc.FAULTS:
        one_wire[UNKNOWN_ID] = round(UNKNOWN_CELSIUS * SCALE)
    return {"one_wire": one_wire}, readings


def extra_checks(rows: list[dict], last_sec: int) -> dict:
    """sensor.mac_address written by the kernel on the first record, never for the unknown id."""
    checks = {rows[0]["sec"]: [
        {"table": "sensor",
         "where": {"vehicle_id": "$vehicle_id", "sensor_code": v2_setup.SENSOR_CODES[f"temperature_{wire}"]},
         "match": {"mac_address": sensor_id}, "timeout": "00:01:00",
         "note": f"ID 1-wire ke-{wire} di paket otomatis ke dallas1_wire{wire} (pin pertama tanpa mac)"}
        for wire, sensor_id in IDS.items()
    ]}
    checks[tc.FAULTS[0]] = [{
        "table": "sensor", "where": {"vehicle_id": "$vehicle_id", "mac_address": UNKNOWN_ID}, "count": 0,
        "note": "ID asing diabaikan: semua pin dallas sudah punya mac",
    }]
    return checks


def build() -> tuple[dict, dict]:
    return tc.build(sys.modules[__name__])
