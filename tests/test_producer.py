"""Event construction in the replay producer, without Kafka."""

import json
from datetime import datetime, timezone
from decimal import Decimal

import pyarrow.parquet as pq

import replay_producer as producer
from conftest import SAMPLE_DIR


def row(**changes):
    r = {
        "Samplingpoint": "SPO.DE_DEBE010_PM10_dataGroup1",
        "Pollutant": 5,
        "Start": datetime(2024, 1, 1, 1, 0),
        "Value": 12.5,
        "Unit": "ug.m-3",
        "Validity": 1,
    }
    r.update(changes)
    return r


def test_event_layout():
    key, event_time, payload = producer.to_event(row())
    assert key == b"DEBE010"
    assert event_time == datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert json.loads(payload) == {
        "station_id": "DEBE010",
        "pollutant": "PM10",
        "event_time": "2024-01-01T00:00:00Z",
        "value": 12.5,
        "unit": "ug.m-3",
    }


def test_source_time_is_utc_plus_one():
    # midnight in the EEA files is 23:00 UTC of the day before
    _, event_time, payload = producer.to_event(row(Start=datetime(2024, 1, 1, 0, 0)))
    assert event_time.isoformat() == "2023-12-31T23:00:00+00:00"
    assert json.loads(payload)["event_time"] == "2023-12-31T23:00:00Z"


def test_pollutant_codes():
    codes = {8: "NO2", 7: "O3", 5: "PM10", 6001: "PM25", 9: "9"}
    for code, name in codes.items():
        _, _, payload = producer.to_event(row(Pollutant=code))
        assert json.loads(payload)["pollutant"] == name


def test_only_validated_values_pass():
    assert producer.to_event(row(Validity=1)) is not None
    for validity in (-1, 2, -99, 0):
        assert producer.to_event(row(Validity=validity)) is None


def test_decimal_value_becomes_json_number():
    _, _, payload = producer.to_event(row(Value=Decimal("12.5000")))
    assert json.loads(payload)["value"] == 12.5


def test_sample_file_converts_end_to_end():
    table = pq.read_table(SAMPLE_DIR / "PM10" / "DEBE010.parquet", columns=producer.COLUMNS)
    rows = table.to_pylist()
    events = [producer.to_event(r) for r in rows]
    sent = [e for e in events if e is not None]
    valid_rows = [r for r in rows if r["Validity"] == 1]

    assert len(sent) == len(valid_rows)
    assert 0 < len(sent) < len(rows)
    assert {key for key, _, _ in sent} == {b"DEBE010"}
    times = [t for _, t, _ in sent]
    assert times == sorted(times)
    for (_, t, payload), r in zip(sent, valid_rows):
        assert t == (r["Start"] - producer.SOURCE_OFFSET).replace(tzinfo=timezone.utc)
        assert json.loads(payload)["value"] == float(r["Value"])
