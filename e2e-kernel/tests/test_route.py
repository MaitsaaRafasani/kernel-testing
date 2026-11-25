"""Real road route helpers (harness/route.py) and the device_normal generator. No network."""
import json

import pytest

from generators import device_normal
from harness import route as route_module
from harness.expected import steps_for
from harness.route import Route, bearing, check_moves, clock, distance, drive, location_row, mileage


@pytest.fixture
def square_route(tmp_path, monkeypatch):
    """Two straight legs: east then north (about 1.1 km each)."""
    monkeypatch.setattr(route_module, "ROUTES_DIR", tmp_path)
    feature = {"type": "Feature", "properties": {"route": "A -> B"},
               "geometry": {"type": "LineString", "coordinates": [[106.80, -6.20], [106.81, -6.20], [106.81, -6.19]]}}
    (tmp_path / "square.json").write_text(json.dumps(feature), encoding="utf-8")
    return Route("square")


def test_distance_matches_kernel_formula():
    assert distance(106.8167, -6.2001, 106.8167, -6.2001) == 0
    assert distance(106.80, -6.20, 106.81, -6.20) == 1105.76          # R = 6372800, 2 decimals
    assert bearing(106.80, -6.20, 106.81, -6.20) == 90 and bearing(106.81, -6.20, 106.81, -6.19) == 0


def test_position_follows_the_road(square_route):
    assert square_route.label == "A -> B" and square_route.length == pytest.approx(2218.1, abs=1)
    assert square_route.at(0) == (-6.2, 106.8, 90)
    lat, lng, direction = square_route.at(1500)                          # on the second leg
    assert lng == 106.81 and lat > -6.2 and direction == 0
    assert square_route.at(99999)[:2] == (-6.19, 106.81)                 # clamped to the end


def test_drive_spacing_speed_and_end(square_route):
    packets = drive(square_route, 60, 10, lambda t: 60)
    assert [packet["t"] for packet in packets[:3]] == [60, 70, 80]
    assert packets[0]["s"] == pytest.approx(166.67, abs=0.01)
    assert packets[-1]["s"] == square_route.length and len(packets) == 14
    assert mileage(packets) <= square_route.length                      # chords on the corner
    check_moves(packets[:5], 20, "trip")
    with pytest.raises(ValueError, match="kernel would not count"):
        check_moves(packets, 200, "trip")


def test_location_row_order():
    packet = {"t": 3725, "lat": -6.2, "lng": 106.8, "speed": 60, "direction": 90}
    row = location_row(packet, step_name="x", batch="offline", sent_at="02:00:00", acc=True)
    assert clock(3725) == "01:02:05"
    assert list(row) == ["from", "step_name", "packet", "batch", "sent_at", "lat", "lng", "speed", "direction", "acc"]


def test_device_normal_generator_names_every_expected_step():
    case, expected = device_normal.build()
    rows = case["timeline"]
    names = {row.get("step_name") for row in rows} | {row.get("first_step_name") for row in rows}
    assert set(steps_for(expected, "device_normal", "concox")) <= names
    assert all("to" in row for row in case["timeline"] if "first_step_name" in row)
    moving = [row for row in case["timeline"] if row.get("packet") == "location" and "to" not in row]
    assert len({(row["lat"], row["lng"]) for row in moving}) == len(moving)   # every packet at a new place
