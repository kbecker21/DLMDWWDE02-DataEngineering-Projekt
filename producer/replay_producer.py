"""Replay producer: streams the sorted EEA measurements into Kafka as JSON events.

Event time runs REPLAY_SPEED times faster than wall-clock time (1440 = one day
per minute). Only measurements with Validity == 1 are sent. Optionally a share
of events is held back by 1-4 hours of event time to exercise late-data handling.
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
from confluent_kafka import Producer

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


def wait_until(producer, target):
    while (remaining := target - time.monotonic()) > 0:
        producer.poll(min(remaining, 0.5))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
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
            if row["Validity"] != 1:
                Stats.dropped += 1
                continue
            event_time = (row["Start"] - SOURCE_OFFSET).replace(tzinfo=timezone.utc)

            if event_time != tick:
                tick = event_time
                if first_event is None:
                    first_event, wall_start = tick, time.monotonic()
                wait_until(producer, wall_start + (tick - first_event).total_seconds() / SPEED)
                while late and late[0][0] <= tick:
                    _, _, key, payload = heapq.heappop(late)
                    send(producer, key, payload)

            station = STATION_RE.search(row["Samplingpoint"]).group(1)
            payload = json.dumps({
                "station_id": station,
                "pollutant": POLLUTANTS.get(row["Pollutant"], str(row["Pollutant"])),
                "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "value": float(row["Value"]),
                "unit": row["Unit"],
            }).encode()
            key = station.encode()

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
