# DLMDWWDE02: Streaming backend for air quality reporting

IU portfolio project (module "Projekt: Data Engineering", task 2): a
containerized streaming pipeline that ingests air quality measurements as a
replayed stream and serves windowed aggregates for reporting.

Pipeline: replay producer -> Kafka -> Spark Structured Streaming ->
TimescaleDB -> Grafana. Runs locally with Docker Compose.

## Data

EEA air quality data (dataset E1a, validated hourly values), Germany 2024,
pollutants NO2, O3, PM10, PM2.5: 1,380 station time series, ~12.1M rows.

Download (one-off setup step, ~380 MB transfer):

    docker compose run --rm downloader

The downloader tries the EEA download API first, then the public blob
container, then a mirror of the 2024 cut attached to a GitHub release.
Output goes to `data/eea_e1a_2024/` (gitignored). Re-running resumes.

There is also a small sample in `data/sample/` (4 Berlin stations, ~3 MB),
so the stack can be tried without downloading anything: set
`DATA_DIR=./data/sample` in `.env`.

Data © [European Environment Agency](https://www.eea.europa.eu/), licensed
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). The release mirror
is an unmodified subset (year 2024, Germany) of that dataset.

## Setup

    cp .env.example .env
    docker compose run --rm downloader     # once, skip when using the sample
    docker compose run --rm preprocess     # merge + sort -> data/replay/events.parquet
    docker compose up -d

The preprocess step merges the per-station files into one file sorted by
timestamp. The producer replays it into Kafka as one JSON event per
measurement, keyed by station, with event time running `REPLAY_SPEED` times
faster than wall clock (default: one day per minute, so 2024 takes about six
hours). Only validated values (`Validity = 1`) are sent; timestamps are
converted from the EEA's fixed UTC+1 to UTC. `LATE_EVENT_RATE` holds back a
share of events by 1-4 hours of event time to exercise late-data handling.

The producer is a one-shot: it exits when the replay is through and is not
restarted. If the topic already holds messages it exits right away, so a
second `docker compose up` does not replay the year on top; `docker compose
down -v` starts over. While the broker is down it keeps its events queued
for up to 30 minutes before it gives up and exits with an error.

Kafka-UI is at http://localhost:8080 (topic `sensor-events`, 12 partitions),
Grafana at http://localhost:3000. TimescaleDB listens on `localhost:5432`
(database `airquality`, roles and passwords from `.env`).

## Processing

The Spark job (`processor/`) reads the topic, parses each event against an
explicit schema and drops what does not fit (malformed JSON, missing or
non-numeric fields, an event time in the future). The last one matters: a
single message dated years ahead would move the watermark there and every
following event would be late. Valid events feed two streaming queries,
each with its own checkpoint and target table:

| query         | window                          | pollutants  | table             |
|---------------|---------------------------------|-------------|-------------------|
| `tumbling_6h` | 6 h tumbling, avg/min/max/count | all         | `agg_tumbling_6h` |
| `sliding_24h` | 24 h mean sliding by 1 h        | PM10, PM2.5 | `agg_sliding_24h` |

The sliding query mirrors the daily mean the PM limit values are defined on:
with a 1 h slide there is a window for every full hour, so the CET calendar
day (23:00-23:00 UTC) is always one of them and the other 23 positions make
it an hourly early indicator.

Both queries use a 3-hour watermark, so a window stays open for three hours
after it ends and events arriving later than that are discarded. The
`dropped_late` figure in the log counts what Spark drops at the state store,
which is pre-aggregated groups per batch (station, pollutant, window), not
single events; for the sliding query one event touches 24 windows. Every
micro-batch logs its numbers:

    processor-1  | ... sink=agg_tumbling_6h batch=41 rows=2580 compute_ms=3400 db_ms=60
    processor-1  | ... query=tumbling_6h batch=41 input=5036 invalid=0 dropped_late=0 state_rows=2618 watermark=2024-01-12T19:00:00.000Z rate=1296/s ms=3776

Each query reads the topic with its own consumer; Structured Streaming does
not share a source between queries. At the default replay speed (one day
per minute) and a 10 s trigger a batch holds about four hours of
measurements and both queries keep up with room to spare
(`docker compose logs -f processor`). The batch time is Spark's stateful
processing; the database write takes well under a second per batch
(`db_ms` in the sink log).

Replay speed, trigger interval and watermark interact: the watermark only
advances between batches, so an event is dropped when it is later than the
watermark plus the event time one batch covers. At the default settings
that is about seven hours, and the producer's 1-4 h late simulation passes
through untouched even with a 3 s trigger (measured: 0 drops in 76k events
at 2 % late). An event 11 h old is dropped, as expected.

## Storage

TimescaleDB holds the aggregates in two hypertables (`db/01_schema.sql`),
chunked by month, with a retention policy. Retention is measured against the
window time and the replayed data is from 2024, so the policy is set to
three years; a live feed would use weeks.

The Spark sink is a `foreachBatch` function: each micro-batch is copied
into a temp table and merged with `INSERT ... ON CONFLICT DO UPDATE` on the
key (window start, station, pollutant). `foreachBatch` is at-least-once:
after a crash Spark re-runs the last unfinished batch from the checkpoint,
and because the rows overwrite themselves that produces no duplicates. So
this is at-least-once delivery plus an idempotent sink, not exactly-once
processing; the kill tests rely on it.

Roles are least-privilege (`db/02_roles.sh`, run once when the data volume
is created): `spark` may SELECT, INSERT and UPDATE the two aggregate tables
and nothing else (the upsert needs SELECT to check for conflicts), `grafana`
is read-only. The superuser password is only used by the init scripts.

    docker compose exec timescaledb psql -U grafana -d airquality
    select * from agg_sliding_24h where station_id = 'DEBB021' order by window_start desc limit 5;

`docker compose down -v` resets Kafka data, Spark checkpoints and the
database together. Do this after changing a query: a checkpoint written by
the old query cannot be resumed by the new one, and the aggregates would
be stale.

The processor image runs Spark 4.1.3 rather than the 4.1.2 named in the
concept: 4.1.2 fails with a NullPointerException in the Kafka source
metrics whenever a batch is replayed after a crash (SPARK-55271), which
turns every restart into a restart loop.

## Dashboard

Grafana is at http://localhost:3000, no login needed to view (anonymous
Viewer; admin password in `.env` for editing). Datasource and dashboard
are provisioned from `grafana/`, the datasource connects with the
read-only `grafana` role. The dashboard opens on the year 2024, the event
time of the data; a relative range like "last 6 hours" would be empty.
It refreshes every 10 s and shows, per selected station, the 6 h means
of all pollutants and the 24 h PM10/PM2.5 means with the EU daily limit
for PM10 as reference line. Sliding windows with fewer than 18 of 24
hourly values are hidden there (the 75 % data-capture rule for a valid
daily mean). The stat row on top shows how far the replay has got and
when the sink last wrote.

## Operations

Every service has a healthcheck, a memory limit and `restart: unless-stopped`
(the one-shot producer excepted); the processor waits for Kafka and the
database to be healthy. Killing a container while the replay runs is
covered in `docs/kill-tests.md`.

## Layout

    docker-compose.yml    full stack (infrastructure as code)
    .env.example          config template, copy to .env
    downloader/           one-off data downloader
    preprocess/           one-off merge + sort of the raw files
    producer/             replay producer
    processor/            Spark Structured Streaming job
    db/                   TimescaleDB schema, hypertables, roles
    grafana/              provisioned datasource + dashboard
    tests/                pytest
    docs/                 architecture and ops notes

## Requirements

Docker Desktop (developed on Windows 11 / WSL2). No local Python needed.
