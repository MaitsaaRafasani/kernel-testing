"""Preflight for kernel v2 (current Postgres schema): is the test device ready? Check only, never writes.

1. Registered: device (by IMEI) -> vehicle -> vehicle_status row, and sensor `speed` + sensors of the usecase.
   Login NEEDS the vehicle_status row: without it login raises VehicleInvalidException and bans the IMEI 5 minutes.
   A sensor sent on a pin (setup `pin`, else creator.sensor_pins) must have that pin_code, and no other sensor
   of the vehicle may use the same pin (kernel SensorRepository.get_all maps pin -> sensor, last row wins).
2. Reset: vehicle_status still has the reset sentinel times (reset is done via kernel API, not here).

Kernel reset (API reset_vehicle_scenario -> VehicleRepository.vehicle_id_reset_all_component) deletes
vehicle_status and re-inserts one row with every time column = RESET_SENTINEL_TIME.
"""
from dataclasses import dataclass

# same literal as kernel insert, so Postgres casts it like the kernel did (timestamp or timestamptz)
RESET_SENTINEL_TIME = "2024-04-03 07:00:00.000 +0700"

# sensor name (case `setup` sheet) -> sensor_code (kernel enums/sensor_enum.py SensorCodes)
SENSOR_CODES = {
    "battery": 2, "gsm_signal": 3, "door1": 4, "door2": 5, "speed": 6, "vibration": 7, "sos": 8,
    "engine": 9, "fuel": 10, "temperature_2": 11, "temperature_1": 12, "rfid": 13, "refrige_active": 15,
    "pto_active": 24, "fuel_digital": 33, "temperature_3": 36, "accumulator": 38, "door3": 66, "door4": 68,
}
BASE_SENSORS = ("speed",)           # every test device needs these

# pin name (case `setup` sheet, column `pin`) -> pin_code (kernel enums/sensor_enum.py PinCodes)
PIN_CODES = {
    "digital_1": 1, "digital_2": 2, "digital_3": 3, "digital_4": 4,
    "analog_1": 5, "analog_2": 6, "analog_3": 7, "analog_4": 8, "rs232": 9, "bluetooth": 10,
    "dallas1_wire1": 11, "dallas1_wire2": 12, "dallas1_wire3": 13, "rs485": 14, "vcc": 18,
}

# one round trip: device + vehicle + vehicle_status + sensors [[sensor_code, pin_code], ...]
DEVICE_STATE_SQL = """
    SELECT d.vehicle_id, d.company_id,
        v.vehicle_id IS NOT NULL AS has_vehicle,
        vs.vehicle_id IS NOT NULL AS has_status,
        COALESCE(vs.last_packet = %(sentinel)s AND vs.last_receive = %(sentinel)s, false) AS is_reset,
        ARRAY(SELECT ARRAY[s.sensor_code, s.pin_code] FROM sensor s WHERE s.vehicle_id = d.vehicle_id) AS sensors
    FROM device d
    LEFT JOIN vehicle v ON v.vehicle_id = d.vehicle_id
    LEFT JOIN vehicle_status vs ON vs.vehicle_id = d.vehicle_id
    WHERE d.imei = %(imei)s
    LIMIT 1
"""


@dataclass
class DeviceState:
    variables: dict          # imei, vehicle_id, company_id (expected $variables)
    problems: list[str]      # empty = registered and all sensors exist
    is_reset: bool


def required_sensors(setup: dict | None) -> dict[str, int]:
    """{name: sensor_code}: BASE_SENSORS + `setup.sensors` of the case (row `sensors | code`)."""
    names = list(BASE_SENSORS) + [str(item["code"]) for item in (setup or {}).get("sensors") or []]
    unknown = [name for name in names if name not in SENSOR_CODES]
    if unknown:
        raise ValueError(f"sensor tidak dikenal di setup: {unknown} (pilihan: {', '.join(SENSOR_CODES)})")
    return {name: SENSOR_CODES[name] for name in names}


def setup_pins(setup: dict | None) -> dict[str, int]:
    """{name: pin_code} of setup sensors with `pin` (ex: sensors | refrige_active | digital_2)."""
    pins = {}
    for item in (setup or {}).get("sensors") or []:
        if "pin" not in item:
            continue
        pin = str(item["pin"]).strip().lower()
        if pin not in PIN_CODES:
            choices = ", ".join(PIN_CODES)
            raise ValueError(f"pin '{item['pin']}' sensor {item['code']} tidak dikenal, pilihan: {choices}")
        pins[str(item["code"])] = PIN_CODES[pin]
    return pins


async def check_device(db, imei: str, sensors: dict[str, int], pins: dict[str, int]) -> DeviceState:
    """sensors: required {name: sensor_code}. pins: {name: pin_code the packet is sent on}."""
    row = await db.get_one(DEVICE_STATE_SQL, {"imei": imei, "sentinel": RESET_SENTINEL_TIME})
    if row is None:
        return DeviceState({"imei": imei}, [f"IMEI {imei} tidak ada di tabel device"], False)
    variables = {"imei": imei, "vehicle_id": row["vehicle_id"], "company_id": row["company_id"]}
    if row["vehicle_id"] is None:
        return DeviceState(variables, [f"device {imei}: vehicle_id kosong"], False)

    problems = []
    if not row["has_vehicle"]:
        problems.append(f"vehicle_id {row['vehicle_id']} tidak ada di tabel vehicle")
    if not row["has_status"]:
        problems.append(f"vehicle_id {row['vehicle_id']} tidak ada di vehicle_status (reset dulu lewat API kernel)")
    problems += sensor_problems(row["vehicle_id"], row["sensors"] or [], sensors, pins)
    return DeviceState(variables, problems, row["has_status"] and row["is_reset"])


def sensor_problems(vehicle_id: int, rows: list, sensors: dict[str, int], pins: dict[str, int]) -> list[str]:
    """rows: [[sensor_code, pin_code], ...] from DB."""
    pin_of = {code: pin for code, pin in rows}
    missing, problems = [], []
    for name, code in sensors.items():
        if code not in pin_of:
            missing.append(f"{name} ({code})")
            continue
        if name not in pins:            # not sent on a pin (ex: speed from GPS)
            continue
        if pin_of[code] != pins[name]:
            problems.append(f"vehicle_id {vehicle_id}: sensor {name} ({code}) pin_code {pin_of[code]}, "
                            f"creator kirim di pin {pins[name]}")
        others = sorted(other for other, pin in rows if pin == pins[name] and other != code)
        if others:
            problems.append(
                f"vehicle_id {vehicle_id}: pin {pins[name]} sensor {name} juga dipakai sensor_code {others}"
            )
    if missing:
        problems.insert(0, f"vehicle_id {vehicle_id}: sensor belum ada di tabel sensor: {', '.join(missing)}")
    return problems
