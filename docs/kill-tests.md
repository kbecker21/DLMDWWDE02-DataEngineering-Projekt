# Kill tests

2026-09-13, full dataset, default replay speed, 10 s trigger, empty
volumes. Each kill during active replay (event time 2024-01-01 to 01-24).

    # SIGTERM to PID 1
    docker compose exec kafka sh -c 'kill 1'

    # SIGKILL (crash)
    docker run --rm --pid=host --privileged alpine kill -9 $(docker inspect -f '{{.State.Pid}}' <container>)

    # longer outage
    docker compose stop timescaledb; sleep 45; docker compose start timescaledb

`docker kill` counts as manual stop and does not trigger the restart policy.

| # | target | kill | back after | effect |
|---|--------|------|-----------:|--------|
| 1 | kafka | SIGTERM | 8 s | producer: 0 failed; processor: one batch 7.8 s instead of 3.6 s |
| 2 | kafka | SIGKILL | 16 s | producer queued 4,908 events, 0 failed; processor: one batch 15.5 s |
| 3 | timescaledb | SIGTERM | 6 s | not noticed by processor |
| 4 | timescaledb | SIGKILL | 7 s | crash recovery; not noticed by processor |
| 5 | timescaledb | stop 47 s | 10 s | sink error, processor restarted 3x, batch 75 re-run from checkpoint |
| 6 | kafka | stop 6 min | 11 s | producer queue full (100k), 0 failed; processor restarted 25x, batch 82 re-run, next batch 197,245 events, no late drops |
| 7 | processor | SIGKILL | 9 s | both queries resumed at the in-flight batch 96 |

## Result check

Producer stopped, processor drained its input:

| check | expected | got |
|-------|----------|-----|
| messages in topic | sent + 1 poison pill | 702,463 |
| `sum(n_values)` tumbling | 702,462 | 702,462 |
| `sum(n_values)` sliding | 24 x 343,497 PM events | 8,243,928 |
| windows 01-01 to 01-21 | pyarrow count: 620,000 | 620,000 |
| 12 tumbling / 4 sliding windows of DEBB021 | pyarrow values | identical |

Re-run batches (75, 82, 96) caused no duplicates.

## Poison pill

    docker compose exec kafka sh -c 'echo "{\"station_id\":\"DEXX000\",\"pollutant\":\"NO2\",\"event_time\":\"2030-01-01T00:00:00Z\",\"value\":1,\"unit\":\"ug/m3\"}" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sensor-events'

Without the future-timestamp filter the watermark jumped to 2029 and all
later events were dropped. With it: `invalid=1`, watermark stays in 2024.

## Limitation

The processor healthcheck only queries the Spark UI; during restarts
`docker compose ps` can briefly show healthy. The batch log is reliable.
