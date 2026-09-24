"""Validation and window logic of the Spark job, on a local SparkSession."""

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
import pytest

import replay_producer as producer
import streaming_job as job
from conftest import SAMPLE_DIR

GOOD = {
    "station_id": "DEBE010",
    "pollutant": "PM10",
    "event_time": "2024-01-01T00:00:00Z",
    "value": 12.5,
    "unit": "ug.m-3",
}


def event(**changes):
    e = dict(GOOD)
    e.update(changes)
    return json.dumps(e)


def raw(spark, *messages):
    return spark.createDataFrame([(m.encode(),) for m in messages], "value binary")


def test_valid_event_passes(spark):
    rows = job.valid_events(raw(spark, event())).collect()
    assert len(rows) == 1
    r = rows[0]
    assert (r.station_id, r.pollutant, r.value, r.unit) == ("DEBE010", "PM10", 12.5, "ug.m-3")
    assert r.event_time == datetime(2024, 1, 1, 0, 0)


@pytest.mark.parametrize(
    "message",
    [
        "not json",
        "{",
        json.dumps({"station_id": "DEBE010"}),
        event(station_id=None),
        event(pollutant=None),
        event(event_time=None),
        event(event_time="yesterday"),
        event(value=None),
        event(value="12.5 ug"),
        event(event_time="2031-01-01T00:00:00Z"),
    ],
    ids=["text", "truncated", "fields-missing", "no-station", "no-pollutant", "no-time",
         "bad-time", "no-value", "text-value", "future"],
)
def test_invalid_event_is_dropped(spark, message):
    assert job.valid_events(raw(spark, message)).count() == 0


def test_unit_is_optional_and_extra_fields_are_ignored(spark):
    rows = job.valid_events(raw(spark, event(unit=None), event(extra="x"))).collect()
    assert len(rows) == 2
    assert set(rows[0].asDict()) == {"station_id", "pollutant", "event_time", "value", "unit"}


def test_mixed_batch_keeps_only_the_good_rows(spark):
    df = raw(spark, event(), "garbage", event(value=None), event(event_time="2024-06-01T12:00:00Z"))
    assert job.valid_events(df).count() == 2


@pytest.fixture(scope="module")
def january(spark):
    """Sample events of one station for three pollutants, January 2024, as
    the producer would send them."""
    rows = []
    for pollutant in ("PM10", "PM25", "NO2"):
        table = pq.read_table(SAMPLE_DIR / pollutant / "DEBE010.parquet", columns=producer.COLUMNS)
        for r in table.to_pylist():
            e = producer.to_event(r)
            if e is None:
                continue
            _, t, payload = e
            if t >= datetime(2024, 2, 1, tzinfo=timezone.utc):
                continue
            rows.append(json.loads(payload) | {"event_time": t})
    df = spark.createDataFrame(rows, job.EVENT_SCHEMA)
    return rows, df


def floor(t, hours):
    return t.replace(hour=t.hour - t.hour % hours, minute=0, second=0, microsecond=0, tzinfo=None)


def test_tumbling_6h_matches_plain_python(january):
    rows, df = january
    got = {(r.window_start, r.station_id, r.pollutant): r for r in job.tumbling_6h(df).collect()}

    groups = defaultdict(list)
    for e in rows:
        groups[(floor(e["event_time"], 6), e["station_id"], e["pollutant"])].append(e["value"])

    assert set(got) == set(groups)
    for key, values in groups.items():
        r = got[key]
        assert r.window_end == key[0] + timedelta(hours=6)
        assert r.n_values == len(values)
        assert r.min_value == min(values) and r.max_value == max(values)
        assert r.avg_value == pytest.approx(sum(values) / len(values))


def test_sliding_24h_matches_plain_python(january):
    rows, df = january
    got = job.sliding_24h(df).collect()

    assert {r.pollutant for r in got} == {"PM10", "PM25"}
    assert all(r.window_end - r.window_start == timedelta(hours=24) for r in got)
    assert all(r.window_start.minute == 0 and r.window_start.second == 0 for r in got)

    pm = [e for e in rows if e["pollutant"] in ("PM10", "PM25")]
    assert sum(r.n_values for r in got) == 24 * len(pm)

    by_pollutant = defaultdict(list)
    for e in pm:
        by_pollutant[e["pollutant"]].append((e["event_time"].replace(tzinfo=None), e["value"]))
    for r in got:
        values = [v for t, v in by_pollutant[r.pollutant] if r.window_start <= t < r.window_end]
        assert r.n_values == len(values)
        assert r.avg_value == pytest.approx(sum(values) / len(values))


def test_json_to_aggregate_path(spark):
    messages = [event(event_time=f"2024-03-01T{h:02d}:00:00Z", value=float(h)) for h in range(6)]
    got = job.tumbling_6h(job.valid_events(raw(spark, *messages, "broken"))).collect()
    assert len(got) == 1
    r = got[0]
    assert (r.n_values, r.min_value, r.max_value, r.avg_value) == (6, 0.0, 5.0, 2.5)
    assert r.window_start == datetime(2024, 3, 1, 0, 0)
