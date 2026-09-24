"""Smoke test against the running stack: every validated event of the replay
file must show up in Kafka and in both aggregate tables, and Grafana must
serve the dashboard. Skipped when Kafka is not reachable.

    DATA_DIR=./data/sample REPLAY_SPEED=1000000 docker compose up -d
    docker compose run --rm tests
"""

import json
import os
import re
import socket
import time
import urllib.request
from collections import defaultdict
from datetime import timedelta, timezone

import psycopg
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "sensor-events")
REPLAY_FILE = os.environ.get("REPLAY_FILE", "/app/data/replay/events.parquet")
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://grafana:3000")
TIMEOUT = int(os.environ.get("E2E_TIMEOUT", "300"))
SOURCE_OFFSET = timedelta(hours=1)
PM_CODES = (5, 6001)


def reachable(hostport):
    host, port = hostport.split(":")
    try:
        socket.create_connection((host, int(port)), timeout=2).close()
        return True
    except OSError:
        return False


pytestmark = [pytest.mark.e2e, pytest.mark.skipif(not reachable(BOOTSTRAP), reason=f"no Kafka at {BOOTSTRAP}")]


def wait_for(read, target, what):
    """Poll until the stack has caught up with the replay, or fail after TIMEOUT."""
    deadline = time.monotonic() + TIMEOUT
    while True:
        value = read()
        if value == target or time.monotonic() > deadline:
            assert value == target, f"{what}: {value:,} after {TIMEOUT}s, expected {target:,}"
            return
        time.sleep(5)


def db():
    return psycopg.connect(
        host=os.environ.get("DB_HOST", "timescaledb"),
        dbname=os.environ.get("DB_NAME", "airquality"),
        user=os.environ.get("DB_USER", "grafana"),
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
    )


def scalar(sql):
    with db() as conn:
        return conn.execute(sql).fetchone()[0] or 0


@pytest.fixture(scope="module")
def replay():
    """What the producer sends: the validated rows of the replay file."""
    table = pq.read_table(REPLAY_FILE, columns=["Samplingpoint", "Pollutant", "Start", "Value", "Validity"])
    return table.filter(pc.equal(table["Validity"], 1))


def test_topic_holds_every_validated_event(replay):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

    def count():
        partitions = admin.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
        futures = admin.list_offsets({TopicPartition(TOPIC, p): OffsetSpec.latest() for p in partitions})
        return sum(f.result().offset for f in futures.values())

    wait_for(count, replay.num_rows, "messages in topic")


def test_tumbling_aggregates_cover_every_event(replay):
    wait_for(lambda: scalar("select sum(n_values) from agg_tumbling_6h"), replay.num_rows, "agg_tumbling_6h")


def test_sliding_aggregates_count_each_pm_event_24_times(replay):
    pm_events = sum(1 for p in replay["Pollutant"].to_pylist() if p in PM_CODES)
    wait_for(lambda: scalar("select sum(n_values) from agg_sliding_24h"), 24 * pm_events, "agg_sliding_24h")


def test_first_full_window_matches_the_raw_values(replay):
    # first 6 h window of the first PM10 series that has all six hourly values
    pm10 = replay.filter(pc.equal(replay["Pollutant"], 5))
    rows = pm10.sort_by([("Samplingpoint", "ascending"), ("Start", "ascending")]).to_pylist()
    groups = defaultdict(list)
    for r in rows:
        t = (r["Start"] - SOURCE_OFFSET).replace(tzinfo=timezone.utc)
        start = t.replace(hour=t.hour - t.hour % 6)
        station = re.search(r"SPO\.DE_([A-Z0-9]+)_", r["Samplingpoint"]).group(1)
        groups[(station, start)].append(float(r["Value"]))
    (station, start), values = next((k, v) for k, v in groups.items() if len(v) == 6)

    with db() as conn:
        got = conn.execute(
            "select avg_value, min_value, max_value, n_values from agg_tumbling_6h "
            "where station_id = %s and pollutant = 'PM10' and window_start = %s",
            (station, start),
        ).fetchone()
    assert got is not None, f"no row for {station} {start}"
    assert got[3] == 6 and got[1] == min(values) and got[2] == max(values)
    assert got[0] == pytest.approx(sum(values) / 6)


def test_grafana_serves_the_dashboard():
    with urllib.request.urlopen(f"{GRAFANA_URL}/api/health", timeout=10) as r:
        assert json.load(r)["database"] == "ok"
    with urllib.request.urlopen(f"{GRAFANA_URL}/api/dashboards/uid/air-quality", timeout=10) as r:
        dashboard = json.load(r)["dashboard"]
    assert dashboard["uid"] == "air-quality"
    assert len(dashboard["panels"]) >= 4
