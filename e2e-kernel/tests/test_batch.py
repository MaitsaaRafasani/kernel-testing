"""Buffered data: batch per family + sent_at ordering."""
from datetime import datetime, timedelta, timezone

from creators.concox.creator import ConcoxCreator, crc_itu
from creators.teltonika.creator import TeltonikaCreator, crc16_arc
from harness.runner import group_batches
from harness.timeline import expand_steps

NOW = datetime(2026, 11, 18, 3, 0, 0, tzinfo=timezone.utc)
IMEI = "358000000000001"


def records(count: int) -> list[tuple]:
    return [
        ("location", {"latitude": -6.2, "longitude": 106.8 + i * 0.001, "gps_satellites": 10, "speed": 30,
                      "engine_on": True}, NOW + timedelta(seconds=30 * i))
        for i in range(count)
    ]


def test_teltonika_batch_is_one_avl_packet_with_all_records_oldest_first():
    items = records(3)
    packet = TeltonikaCreator().build_batch(items, IMEI)
    data = packet[16:-8]
    assert int(packet[8:16], 16) * 2 == len(data) and int(packet[-8:], 16) == crc16_arc(data)
    assert data[2:4] == "03" and data[-2:] == "03"                       # record count at start and end
    first_record_time = int(data[4:20], 16)                              # oldest record first
    assert first_record_time == int(items[0][2].timestamp() * 1000)


def split_concox(message: str) -> list[str]:
    frames, index = [], 0
    while index < len(message):
        length = int(message[index + 4:index + 6], 16)
        end = index + 6 + length * 2 + 4
        frames.append(message[index:end])
        index = end
    return frames


def test_concox_batch_is_concatenated_frames_with_reupload_flag():
    message = ConcoxCreator().build_batch(records(3), IMEI)
    frames = split_concox(message)
    assert len(frames) == 3
    for frame in frames:
        assert frame[6:8] == "22" and frame.endswith("0d0a")
        assert format(crc_itu(frame[4:-8]), "04x") == frame[-8:-4]
        assert frame[64:66] == "01"                                     # GPS re-upload = 1 (buffered)


def test_sent_at_and_batch_grouping():
    scenario = {"start": "now", "steps": [
        {"offset": "0s", "packet": "location"},
        {"offset": "10m", "packet": "connect"},
    ], "segments": [
        {"from": "1m", "to": "4m", "every": "1m", "packet": "location", "batch": "off", "sent_at": "10m",
         "first_step_name": "b1", "step_name": "b3"},
        {"from": "10m", "to": "12m", "every": "1m", "packet": "location"},
    ]}
    steps = expand_steps(scenario, {}, NOW)
    order = [(s.packet, s.id, s.batch) for s in steps]
    assert order == [
        ("location", None, None), ("connect", None, None),
        ("location", "b1", "off"), ("location", None, "off"), ("location", "b3", "off"),
        ("location", None, None), ("location", None, None),
    ]
    assert [s.at - NOW for s in steps][2:5] == [timedelta(minutes=1), timedelta(minutes=2), timedelta(minutes=3)]
    assert [len(group) for group in group_batches(steps)] == [1, 1, 3, 1, 1]
