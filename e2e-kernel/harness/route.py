"""Real road routes for test case timelines (README "Rute jalan").

- fetch_route(): road geometry from OSRM (OpenStreetMap), saved as GeoJSON in routes/<name>.json (source, in git)
- Route: position + road direction at s metres along the route
- drive(): moving packets along a route, distance per packet = speed x gap
- distance() / mileage(): same haversine as kernel v2 (helper/calculate_distance.py), for expected values
- check_moves(): distances between packets the kernel counts as movement
"""
import json
import math
import urllib.request
from pathlib import Path

from harness.config import ROOT

ROUTES_DIR = ROOT / "routes"
OSRM_URL = "https://router.project-osrm.org/route/v1/driving/{points}?overview=full&geometries=geojson"
EARTH_RADIUS = 6372800                  # metres, kernel helper/calculate_distance.py
DECIMALS = 6
FETCH_TIMEOUT = 30                      # seconds


def distance(lng1: float, lat1: float, lng2: float, lat2: float) -> float:
    """Metres, 2 decimals, same as kernel calculate_distance."""
    if (lng1, lat1) == (lng2, lat2):
        return 0
    lng1, lat1, lng2, lat2 = map(math.radians, (lng1, lat1, lng2, lat2))
    a = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lng2 - lng1) / 2) ** 2
    return round(EARTH_RADIUS * 2 * math.asin(math.sqrt(a)), 2)


def bearing(lng1: float, lat1: float, lng2: float, lat2: float) -> int:
    """Direction 0-359 degrees (0 = north)."""
    lng1, lat1, lng2, lat2 = map(math.radians, (lng1, lat1, lng2, lat2))
    y = math.sin(lng2 - lng1) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(lng2 - lng1)
    return round(math.degrees(math.atan2(y, x))) % 360


def fetch_route(name: str, points: list[tuple[float, float]], label: str) -> Path:
    """OSRM driving route through `points` [(lng, lat), ...] -> routes/<name>.json. Only coordinates are sent."""
    if len(points) < 2:
        raise ValueError("route needs at least 2 points (start, destination)")
    url = OSRM_URL.format(points=";".join(f"{lng},{lat}" for lng, lat in points))
    request = urllib.request.Request(url, headers={"User-Agent": "e2e-kernel-route"})
    with urllib.request.urlopen(request, timeout=FETCH_TIMEOUT) as response:
        result = json.load(response)
    if result.get("code") != "Ok" or not result.get("routes"):
        raise ValueError(f"OSRM: no route ({result.get('code')}: {result.get('message')})")
    route = result["routes"][0]
    feature = {
        "type": "Feature",
        "properties": {"name": name, "route": label, "source": "OSRM / OpenStreetMap",
                       "distance_m": round(route["distance"])},
        "geometry": {"type": "LineString", "coordinates": [
            [round(lng, DECIMALS), round(lat, DECIMALS)] for lng, lat in route["geometry"]["coordinates"]]},
    }
    path = ROUTES_DIR / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(feature, separators=(",", ":")), encoding="utf-8")
    return path


class Route:
    """GeoJSON LineString from routes/<name>.json, measured with the kernel distance."""

    def __init__(self, name: str):
        feature = json.loads((ROUTES_DIR / f"{name}.json").read_text(encoding="utf-8"))
        self.name = name
        self.label = feature["properties"].get("route", name)
        self.points = feature["geometry"]["coordinates"]
        self.cumulative = [0.0]
        for (lng1, lat1), (lng2, lat2) in zip(self.points, self.points[1:]):
            self.cumulative.append(self.cumulative[-1] + distance(lng1, lat1, lng2, lat2))
        self.length = self.cumulative[-1]

    def at(self, s: float) -> tuple[float, float, int]:
        """(lat, lng, direction) at s metres from the start (clamped to the route)."""
        s = min(max(s, 0.0), self.length)
        low, high = 1, len(self.cumulative) - 1
        while low < high:                                   # first segment end >= s
            middle = (low + high) // 2
            if self.cumulative[middle] >= s:
                high = middle
            else:
                low = middle + 1
        start, end = self.cumulative[low - 1], self.cumulative[low]
        ratio = 0.0 if end == start else (s - start) / (end - start)
        (lng1, lat1), (lng2, lat2) = self.points[low - 1], self.points[low]
        return (round(lat1 + (lat2 - lat1) * ratio, DECIMALS), round(lng1 + (lng2 - lng1) * ratio, DECIMALS),
                bearing(lng1, lat1, lng2, lat2))


def drive(route: Route, start: int, every: int, speed_at, s: float = 0.0, count: int | None = None) -> list[dict]:
    """Moving packets from second `start`, every `every` s, until the route end (or `count` packets).
    speed_at(second) -> km/h. s = metres already done. Packet: {t, s, lat, lng, speed, direction}."""
    packets, t = [], start
    while s < route.length and (count is None or len(packets) < count):
        speed = speed_at(t)
        s = min(s + speed / 3.6 * every, route.length)
        lat, lng, direction = route.at(s)
        packets.append({"t": t, "s": s, "lat": lat, "lng": lng, "speed": speed, "direction": direction})
        t += every
    return packets


def moves(packets: list[dict]) -> list[float]:
    """Kernel distance between consecutive packets."""
    return [distance(a["lng"], a["lat"], b["lng"], b["lat"]) for a, b in zip(packets, packets[1:])]


def mileage(packets: list[dict]) -> float:
    """Kernel trip_distance of a trip: sum of moves (the first motion packet adds nothing)."""
    return sum(moves(packets))


def check_moves(packets: list[dict], minimum: float, name: str) -> None:
    """Raise when a packet moves less than the kernel motion threshold (20 m acc on, 50 m towed)."""
    shortest = min(moves(packets), default=minimum)
    if shortest < minimum:
        raise ValueError(f"{name}: packet moves {shortest} m < {minimum} m (kernel would not count it as motion)")


def clock(seconds: int) -> str:
    """Seconds from timeline start -> HH:MM:SS."""
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def location_row(packet: dict, **options) -> dict:
    """One timeline row for one moving packet. options: step_name, batch, sent_at, acc, ..."""
    name = {"step_name": options.pop("step_name")} if "step_name" in options else {}
    batch = {key: options.pop(key) for key in ("batch", "sent_at") if key in options}
    return {"from": clock(packet["t"]), **name, "packet": "location", **batch, "lat": packet["lat"],
            "lng": packet["lng"], "speed": packet["speed"], "direction": packet["direction"], **options}
