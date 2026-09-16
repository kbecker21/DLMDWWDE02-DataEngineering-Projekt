# Kill tests

Run on 2026-09-13 against the full dataset at the default replay speed
(one day per minute, 10 s trigger), stack started from empty volumes.
Every test was done while the producer was sending and the processor was
writing; the replay covered 2024-01-01 to 2024-01-24.

How the containers were killed:

    # SIGTERM to PID 1, the restart policy kicks in
    docker compose exec kafka sh -c 'kill 1'

    # SIGKILL from the host namespace, same effect as a crash
    docker run --rm --pid=host --privileged alpine kill -9 $(docker inspect -f '{{.State.Pid}}' <container>)

    # outage longer than one micro-batch
    docker compose stop timescaledb; sleep 45; docker compose start timescaledb

`docker kill` is not suitable: Docker treats it as a manual stop and does
not restart the container.

| # | target | signal | back after | what the others did |
|---|--------|--------|-----------:|---------------------|
| 1 | kafka | SIGTERM | 8 s | Controlled shutdown. Producer: 3 "connection refused" lines from librdkafka, 0 failed deliveries. Processor: batch 8 took 7.8 s instead of 3.6 s, no restart. |
| 2 | kafka | SIGKILL | 16 s | Broker recovers its unflushed segment. Producer queues 4,908 events (rate 0 for one log interval), then delivers them at double rate, 0 failed. Processor: batch 39 took 15.5 s, no restart. |
| 3 | timescaledb | SIGTERM | 6 s | Smart shutdown with checkpoint. Processor did not notice: a batch writes for 50-400 ms every 10 s. |
| 4 | timescaledb | SIGKILL | 7 s | "database system was not properly shut down; automatic recovery in progress". Processor did not notice. |
| 5 | timescaledb | stopped 47 s | 10 s after start | Sink of batch 75 fails with `psycopg.OperationalError`, the job exits, the container restarts three times until the database answers. Batch 75 is re-run for both queries from the checkpoint, batch 76 then carries the backlog (30,648 events). |
| 6 | kafka | stopped 6 min | 11 s after start | Producer: delivery queue fills to librdkafka's limit of 100,000, then `produce()` blocks; 0 failed deliveries (with the default 5 min `message.timeout.ms` this ended the run). After the restart 192,000 queued events go out in 7 s. Processor: 25 restarts ("No resolvable bootstrap urls", the stopped container has no DNS entry), then batch 82 re-run and batch 83 with 197,245 events; the watermark jumps six days without dropping anything because it only moves after the batch. |
| 7 | processor | SIGKILL | 9 s | Batch 95 was complete, batch 96 in flight. After the restart both queries continue at batch 96. |

## Result check

After the tests the producer was stopped and the processor left running
until its input was empty.

| check | expected | got |
|-------|----------|-----|
| messages in the topic | events sent + 1 poison pill | 702,463 |
| `sum(n_values)` in `agg_tumbling_6h` | 702,462 | 702,462 |
| `sum(n_values)` in `agg_sliding_24h` | 24 x PM10/PM2.5 events (24 x 343,497) | 8,243,928 |
| `sum(n_values)` for windows in 2024-01-01 to 2024-01-21 | pyarrow count on the parquet file: 620,000 | 620,000 |
| 12 tumbling windows of DEBB021 (during tests 5, 6, 7) | pyarrow avg/min/max/count | identical |
| 4 sliding windows of DEBB021 (CET days 14.01. and 18.01.) | pyarrow mean/count | identical |

Batches that were run twice (75, 82, 96) show up nowhere in the counts:
the upsert overwrites its own rows.

## Poison pill

A message with `event_time` in 2030 sent to the topic by hand:

    docker compose exec kafka sh -c 'echo "{\"station_id\":\"DEXX000\",\"pollutant\":\"NO2\",\"event_time\":\"2030-01-01T00:00:00Z\",\"value\":1,\"unit\":\"ug/m3\"}" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sensor-events'

Before the plausibility filter this moved the watermark to 2029-12-31
and every following event was dropped as late until `down -v`. Now batch
91 logs `invalid=1` and the watermark keeps moving in 2024.

## Known limitation

The processor healthcheck only asks the Spark UI. While the job is in a
restart loop because Kafka or the database is away, `docker compose ps`
shows the container as healthy for a few seconds per attempt. The batch
log tells the truth.
