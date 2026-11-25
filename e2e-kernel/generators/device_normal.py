"""device_normal: 3 trips on real roads (routes/trip_*.json) for all families.

python cli.py generate --usecase device_normal  -> cases/device_normal.yaml + versions/v2/expected/device_normal.yaml
Expected values come from the kernel v2 rules (harness/route.py): motion >= 20 m (acc on) / >= 50 m (towed),
assisted start >= 300 m from the park spot, trip_mileage = kernel haversine after the first motion packet.
"""
import math

from harness.route import Route, check_moves, clock, distance, drive, location_row, mileage

USECASE = "device_normal"
FAMILIES = ["concox", "teltonika", "topflytech", "ruptela", "queclink"]
INTERVAL = 10                          # seconds between realtime packets
BUFFER_EVERY = 30                      # seconds between buffered records
BUFFER_RECORDS = 120                   # 1 hour offline
REALTIME_BEFORE_OFFLINE = 30           # packets (5 minutes)
SPEED = 60                             # km/h
OVERSPEED = (120, 1800, 1980)          # km/h, from second, until second (00:30:00 - 00:33:00)
TOW_SPEED = 30
PARK_EVERY = 300                       # park packet every 5 minutes
PARK = 1200                            # trip -> park 20 minutes (summary written after park_timeout <= 10 min)
PARK_ASSISTED = 1500                   # assisted needs park_timeout + 5 minutes
MOTION_MIN = 20                        # metres, acc on
TOW_MOTION_MIN = 50                    # metres, assisted (acc off)
ASSISTED_START_MIN = 300               # metres from the park spot
GATE = {"ip_gate": "10.10.10.10", "port_gate": "random"}
REPLY = "STATUS OK GPRS:Link Up GSM Signal Level:Strong ACC:ON"
KEY_ORDER = ("from", "to", "every", "step_name", "first_step_name", "packet", "families", "batch", "sent_at")
DB_REQUIREMENTS = [
    "IMEI tiap family (env.yaml devices) terdaftar di device, vehicle, vehicle_status dengan model yang sama: "
    "concox 358000000000001, teltonika 350000000000001, topflytech 351000000000001, ruptela 352000000000001, "
    "queclink 353000000000001.",
    "vehicle_status tiap vehicle sudah di-reset (sentinel 2024-04-03 07:00:00 +0700) sebelum run.",
    f"Sensor speed terdaftar dengan alert_hi < {OVERSPEED[0]} km/jam (alert overspeed).",
    "Company vehicle test: park_timeout <= 10 menit (trip ditutup saat parkir 20 menit).",
    "Company dan vehicle khusus test: tidak ada notifikasi / forwarder ke customer asli.",
]


def park(at: int, length: int, name: str, **fields) -> dict:
    return {"from": clock(at), "to": clock(at + length), "every": clock(PARK_EVERY), "step_name": name,
            "packet": "location", "speed": 0, **fields}


def name_rows(rows: list[dict], first: str | None = None, last: str | None = None) -> list[dict]:
    if first:
        rows[0]["step_name"] = first
    if last:
        rows[-1]["step_name"] = last
    return rows


def trip_check(start: str, kind: str, stop: str, packets: list[dict], max_speed: int, note: str) -> dict:
    last = packets[-1]
    return {
        "table": "summary_trip", "where": {"vehicle_id": "$vehicle_id", "start_time": f"$at.{start}"}, "count": 1,
        "match": {"type": kind, "stop_time": f"$at.{stop}",
                  "trip_mileage": {"approx": round(mileage(packets)), "tolerance": 300}, "max_speed": max_speed,
                  "stop_lat": {"approx": last["lat"]}, "stop_long": {"approx": last["lng"]}},
        "note": note,
    }


def status_check(packet: dict, note: str) -> dict:
    return {"table": "vehicle_status", "where": {"vehicle_id": "$vehicle_id"},
            "match": {"latitude": {"approx": packet["lat"]}, "longitude": {"approx": packet["lng"]},
                      "last_packet": "$step_at"}, "note": note}


def build() -> tuple[dict, dict]:
    normal, assisted, buffer = Route("trip_normal"), Route("trip_assisted"), Route("trip_buffer")
    start_lat, start_lng, start_direction = normal.at(0)
    timeline = [
        {"from": "00:00:00", "step_name": "login", "packet": "login"},
        {"from": "00:00:05", "step_name": "connected", "packet": "connect", **GATE},
        {"from": "00:00:10", "to": "00:01:00", "first_step_name": "location", "packet": "location",
         "lat": start_lat, "lng": start_lng, "speed": 0, "direction": start_direction, "acc": True},
    ]

    # trip 1: normal, acc on, overspeed
    over_speed, over_from, over_until = OVERSPEED
    trip1 = drive(normal, 60, INTERVAL, lambda t: over_speed if over_from <= t < over_until else SPEED)
    check_moves(trip1, MOTION_MIN, "trip normal")
    rows = [(packet["t"], location_row(packet)) for packet in trip1]
    for t, row in rows:
        if t == over_from:
            row["step_name"] = "overspeed_start"
        elif t == over_until - INTERVAL:
            row["step_name"] = "overspeed_end"
    rows[0][1]["step_name"], rows[-1][1]["step_name"] = "trip_start", "trip_end"
    rows.append((60, {"from": "00:01:00", "to": clock(trip1[-1]["t"]), "every": "00:10:00",
                      "first_step_name": "heartbeat", "packet": "heartbeat", "families": ["concox"],
                      "battery": 5, "signal_strength": 4}))
    rows.append((150, {"from": "00:02:30", "step_name": "command_reply", "packet": "command_reply",
                       "families": ["concox", "teltonika", "topflytech", "ruptela"], "reply_text": REPLY}))
    timeline += [row for _, row in sorted(rows, key=lambda pair: pair[0])]
    park1 = trip1[-1]["t"] + INTERVAL
    timeline.append(park(park1, PARK, "trip_park_end", acc=False))

    # trip 2: assisted (towed, acc off)
    tow = drive(assisted, park1 + PARK, INTERVAL, lambda t: TOW_SPEED)
    spot = trip1[-1]
    first = next(i for i, packet in enumerate(tow)
                 if distance(spot["lng"], spot["lat"], packet["lng"], packet["lat"]) >= ASSISTED_START_MIN)
    tow_trip = tow[first:]
    check_moves(tow_trip, TOW_MOTION_MIN, "trip assisted")
    tow_rows = name_rows([location_row(packet) for packet in tow], "tow_start", "tow_end")
    tow_rows[first]["step_name"] = "assisted_start"
    timeline += tow_rows
    park2 = tow[-1]["t"] + INTERVAL
    timeline.append(park(park2, PARK_ASSISTED, "assisted_park_end"))

    # trip 3: realtime -> offline (buffer) -> realtime to the destination
    idle = park2 + PARK_ASSISTED
    timeline.append({"from": clock(idle), "to": clock(idle + 60), "packet": "location", "acc": True})
    realtime = drive(buffer, idle + 60, INTERVAL, lambda t: SPEED, count=REALTIME_BEFORE_OFFLINE)
    offline = drive(buffer, realtime[-1]["t"] + BUFFER_EVERY, BUFFER_EVERY, lambda t: SPEED, realtime[-1]["s"],
                    count=BUFFER_RECORDS)
    back = offline[-1]["t"] + BUFFER_EVERY
    realtime2 = drive(buffer, back, INTERVAL, lambda t: SPEED,
                      offline[-1]["s"] + SPEED / 3.6 * (BUFFER_EVERY - INTERVAL))
    trip3 = realtime + offline + realtime2
    check_moves(trip3, MOTION_MIN, "trip buffer")
    timeline += name_rows([location_row(packet) for packet in realtime], "buffer_trip_start", "realtime_end")
    timeline += [
        {"from": clock(realtime[-1]["t"] + INTERVAL), "step_name": "disconnected", "packet": "disconnect"},
        {"from": clock(back), "step_name": "relogin", "packet": "login"},       # before the buffer (same send time)
        {"from": clock(back), "step_name": "reconnected", "packet": "connect", **GATE},
    ]
    timeline += name_rows([location_row(packet, batch="offline", sent_at=clock(back)) for packet in offline],
                          "buffer_start", "buffer_end")
    timeline += name_rows([location_row(packet) for packet in realtime2], last="drive_end")
    park3 = realtime2[-1]["t"] + INTERVAL
    timeline.append(park(park3, PARK, "buffer_park_end", acc=False))

    end = park3 + PARK
    timeline = [{**{key: row[key] for key in KEY_ORDER if key in row},
                 **{key: value for key, value in row.items() if key not in KEY_ORDER}} for row in timeline]
    case = {
        "usecase": USECASE,
        "description": description(normal, assisted, buffer, trip1, tow, first, realtime, realtime2, back),
        "db_requirements": DB_REQUIREMENTS,
        "families": FAMILIES,
        "start": f"now-{clock(math.ceil((end + 600) / 600) * 600)}",          # last packet >= 10 min ago
        "interval": clock(INTERVAL),
        "timeline": timeline,
    }

    steps = {
        "connected": [{"table": "device", "where": {"imei": "$imei"},
                       "match": {"ip": "$ip_gate", "port": "$port_gate"},
                       "note": "connect -> update_ip_port_gate_in_device"}],
        "location": [{"table": "vehicle_status", "where": {"vehicle_id": "$vehicle_id"},
                      "match": {"latitude": {"approx": start_lat}, "longitude": {"approx": start_lng},
                                "last_packet": "$step_at"},
                      "note": "lokasi pertama -> vehicle_status (flush consumer_vehicle_status)"}],
        "heartbeat": [{"table": "vehicle_status", "where": {"vehicle_id": "$vehicle_id"},
                       "match": {"battery": 85, "signal_strength": 4},
                       "note": "concox heartbeat 0x13 level 5 -> 85 persen"}],
        "command_reply": [{"table": "device_log", "where": {"vehicle_id": "$vehicle_id", "imei": "$imei"},
                           "match": {"message": "$reply_text", "type": "R"},
                           "note": "online command reply -> device_log type R"}],
        "overspeed_end": [{"table": "raw_alert",
                           "where": {"vehicle_id": "$vehicle_id", "alert_code": 8,
                                     "created_on": f"H1 {clock(over_from + 60)}"},
                           "match": {"alert_flag": True},
                           "note": f"overspeed lokasi {over_speed} km/jam > 60 detik -> raw_alert (OVERSPEEDS = 8), "
                                   "waktu awal + 60 detik"}],
        "trip_park_end": [trip_check("trip_start", "M", "trip_end", trip1, over_speed,
                                     f"trip normal: {normal.label}")],
        "assisted_park_end": [trip_check("assisted_start", "A", "tow_end", tow_trip, TOW_SPEED,
                                         f"trip assisted (derek, acc off): {assisted.label}, "
                                         f"mulai >= {ASSISTED_START_MIN} m dari titik parkir")],
        "buffer_end": [
            {"table": "raw_location", "where": {"vehicle_id": "$vehicle_id",
                                                "sent_on": {"between": ["$at.buffer_start", "$at.buffer_end"]}},
             "count": BUFFER_RECORDS, "note": f"{BUFFER_RECORDS} record buffer tersimpan dengan waktu aslinya"},
            status_check(offline[-1], "posisi record buffer terakhir"),
        ],
        "drive_end": [status_check(realtime2[-1], "realtime lanjut setelah buffer")],
        "buffer_park_end": [trip_check("buffer_trip_start", "M", "drive_end", trip3, SPEED,
                                       f"1 trip utuh realtime + buffer + realtime: {buffer.label}")],
    }
    expected = {USECASE: {"default": {"steps": {name: {"db": checks} for name, checks in steps.items()}}}}
    return case, expected


def description(normal, assisted, buffer, trip1, tow, first, realtime, realtime2, back) -> str:
    over_speed, over_from, over_until = OVERSPEED
    return f"""\
Dibuat oleh generators/device_normal.py (python cli.py generate --usecase device_normal): ubah generator, bukan Excel.
Device normal kernel v2, satu timeline untuk semua family di creators, rute jalan asli (routes/*.json, OSRM).
1. Trip normal {clock(trip1[0]['t'])} - {clock(trip1[-1]['t'])}: {normal.label}, {normal.length / 1000:.1f} km, acc on,
   {SPEED} km/jam, overspeed {over_speed} km/jam {clock(over_from)} - {clock(over_until)}.
2. Trip assisted {clock(tow[first]['t'])} - {clock(tow[-1]['t'])}: {assisted.label}, {assisted.length / 1000:.1f} km,
   diderek (acc off), {TOW_SPEED} km/jam.
3. Trip buffer {clock(realtime[0]['t'])} - {clock(realtime2[-1]['t'])}: {buffer.label}, {buffer.length / 1000:.1f} km,
   {SPEED} km/jam. 5 menit realtime, offline 1 jam ({BUFFER_RECORDS} record tiap {BUFFER_EVERY} detik, dikirim saat
   login lagi {clock(back)}), lalu realtime sampai tujuan.
Tiap trip ditutup parkir acc off 20 - 25 menit (paket tiap 5 menit) sampai summary_trip tertulis.
Syarat DB: lihat db_requirements.
Catatan kernel v2 (usecase/location/motion_status/motion_status.py):
- trip M: acc on, jarak >= 20 m dan speed >= 5 -> motion, tripstart_on = waktu paket itu.
  Selesai: diam >= park_timeout -> parkir, stop_time = paket gerak terakhir.
- trip A (assisted): acc off, jarak >= 300 m dari titik parkir -> assisted (step assisted_start).
  Lanjut tiap paket >= 50 m. Selesai: diam >= park_timeout + 5 menit.
- overspeed lokasi (usecase/sensor/overspeed): speed > alert_hi lebih dari 60 detik -> raw_alert code 8,
  created_on = awal overspeed + 60 detik. Waktu paket > 30 menit lalu: tetap insert, tanpa notifikasi.
- trip_mileage = jarak haversine antar paket gerak (paket gerak pertama tidak dihitung).
- buffer: teltonika 1 paket AVL {BUFFER_RECORDS} record, ruptela 1 paket, concox / topflytech frame disambung
  1 pesan, queclink 1 frame +BSP per pesan. Jeda paket lokasi < 15 menit (NO_GPS_TIMEOUT).
- heartbeat hanya concox (paket 0x13). command reply tanpa queclink (+RESP berisi waktu kirim device).
- queclink tidak punya paket login: step login tidak mengirim apa pun.
"""
