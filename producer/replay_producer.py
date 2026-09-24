"""Replay producer: streams the sorted EEA measurements into Kafka as JSON events.

Event time runs REPLAY_SPEED times faster than wall-clock time (1440 = one day
per minute). Only measurements with Validity == 1 are sent. Optionally a share
of events is held back by 1-4 hours of event time to exercise late-data handling.

Runs once: if the topic already holds messages the producer exits right away.
"""

import heapq
import json
import logging
import os
import random
import re
import sys
import time
from datetime import timedelta, timezone

import pyarrow.parquet as pq
from confluent_kafka import Producer, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "sensor-events")
REPLAY_FILE = os.environ.get("REPLAY_FILE", "/data/events.parquet")
SPEED = float(os.environ.get("REPLAY_SPEED", "1440"))
LATE_RATE = float(os.environ.get("LATE_EVENT_RATE", "0"))
LATE_DELAY_HOURS = (1, 4)
LOG_EVERY_S = 10

SOURCE_OFFSET = timedelta(hours=1)  # EEA timestamps are UTC+1 without DST
POLLUTANTS = {8: "NO2", 7: "O3", 5: "PM10", 6001: "PM25"}
STATION_RE = re.compile(r"SPO\.DE_([A-Z0-9]+)_")
COLUMNS = ["Samplingpoint", "Pollutant", "Start", "Value", "Unit", "Validity"]

log = logging.getLogger("producer")


class Stats:
    produced = 0
    delivered = 0
    dropped = 0
    failed = 0


def delivery(err, msg):
    if err is None:
        Stats.delivered += 1
        return
    Stats.failed += 1
    if Stats.failed <= 5:
        log.error("delivery failed: %s", err)


def send(producer, key, payload):
    while True:
        try:
            producer.produce(TOPIC, key=key, value=payload, on_delivery=delivery)
            Stats.produced += 1
            return
        except BufferError:
            producer.poll(0.1)


def to_event(row):
    """One measurement -> (key, event time, JSON payload); None for values that
    are not validated (Validity != 1)."""
    if row["Validity"] != 1:
        return None
    event_time = (row["Start"] - SOURCE_OFFSET).replace(tzinfo=timezone.utc)
    station = STATION_RE.search(row["Samplingpoint"]).group(1)
    payload = json.dumps({
        "station_id": station,
        "pollutant": POLLUTANTS.get(row["Pollutant"], str(row["Pollutant"])),
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "value": float(row["Value"]),
        "unit": row["Unit"],
    }).encode()
    return station.encode(), event_time, payload


def topic_message_count(admin):
    partitions = admin.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
    futures = admin.list_offsets({TopicPartition(TOPIC, p): OffsetSpec.latest() for p in partitions})
    return sum(f.result().offset for f in futures.values())


def wait_until(producer, target):
    while (remaining := target - time.monotonic()) > 0:
        producer.poll(min(remaining, 0.5))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # one-shot guard: a second start must not replay the year on top
    n = topic_message_count(AdminClient({"bootstrap.servers": BOOTSTRAP}))
    if n:
        log.info("topic %s already holds %s messages, nothing to do (docker compose down -v to replay)", TOPIC, f"{n:,}")
        return 0
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        # keep retrying through a broker outage instead of failing the run
        "message.timeout.ms": 30 * 60 * 1000,
    })
    rng = random.Random(0)
    late = []  # (release_time, seq, key, payload)
    seq = 0
    tick = wall_start = first_event = None
    last_log = time.monotonic()
    delivered_at_log = 0

    pf = pq.ParquetFile(REPLAY_FILE)
    log.info("replaying %s (%s rows) at %sx, late event rate %s", REPLAY_FILE, f"{pf.metadata.num_rows:,}", SPEED, LATE_RATE)

    for batch in pf.iter_batches(batch_size=50_000, columns=COLUMNS):
        for row in batch.to_pylist():
            event = to_event(row)
            if event is None:
                Stats.dropped += 1
                continue
            key, event_time, payload = event

            if event_time != tick:
                tick = event_time
                if first_event is None:
                    first_event, wall_start = tick, time.monotonic()
                wait_until(producer, wall_start + (tick - first_event).total_seconds() / SPEED)
                while late and late[0][0] <= tick:
                    _, _, late_key, late_payload = heapq.heappop(late)
                    send(producer, late_key, late_payload)

            if LATE_RATE and rng.random() < LATE_RATE:
                release = tick + timedelta(hours=rng.uniform(*LATE_DELAY_HOURS))
                seq += 1
                heapq.heappush(late, (release, seq, key, payload))
            else:
                send(producer, key, payload)

            now = time.monotonic()
            if now - last_log >= LOG_EVERY_S:
                rate = (Stats.delivered - delivered_at_log) / (now - last_log)
                log.info("delivered=%s queued=%s dropped=%s late_pending=%s failed=%s rate=%.0f/s event_time=%s",
                         f"{Stats.delivered:,}", Stats.produced - Stats.delivered - Stats.failed, f"{Stats.dropped:,}",
                         len(late), Stats.failed, rate, tick.strftime("%Y-%m-%d %H:%M"))
                last_log, delivered_at_log = now, Stats.delivered

    while late:
        _, _, key, payload = heapq.heappop(late)
        send(producer, key, payload)
    producer.flush()
    log.info("done: delivered=%s dropped=%s failed=%s", f"{Stats.delivered:,}", f"{Stats.dropped:,}", Stats.failed)
    return 1 if Stats.failed else 0


if __name__ == "__main__":
    sys.exit(main())
