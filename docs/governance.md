# Data governance and security

What the stack does about provenance, data quality, access and privacy, and
what it deliberately leaves out.

## Provenance

Source: European Environment Agency, dataset E1a (validated, reported
hourly values), Germany, 2024, pollutants NO2, O3, PM10 and PM2.5. The
downloader (`downloader/`) fetches the per-station parquet files from the
EEA download API or the public blob container; the release mirror attached
to this repository is an unmodified cut of the same files, kept so the
stack can be reproduced when the EEA endpoints change. Licence: CC BY 4.0,
attributed in the README.

Lineage through the pipeline:

| step | in | out | what changes |
|------|----|-----|--------------|
| downloader | EEA parquet per station | `data/eea_e1a_2024/<pollutant>/<station>.parquet` | rows outside 2024 dropped |
| preprocess | station files | `data/replay/events.parquet` | merged, sorted by `Start`, then `Samplingpoint` |
| producer | replay file | Kafka `sensor-events`, one JSON event per row | `Validity != 1` dropped, `Start` shifted from UTC+1 to UTC, pollutant code mapped to name, station id cut out of the sampling point id |
| processor | Kafka events | `agg_tumbling_6h`, `agg_sliding_24h` | invalid events dropped and counted, aggregated per window |
| Grafana | aggregate tables | dashboard | read only |

Raw data stays on disk exactly as downloaded; every derived form can be
rebuilt from it (`docker compose run --rm preprocess`, `docker compose
down -v && docker compose up -d`).

## Data contract and validation

The event format is fixed in two places that must agree: the producer's
`to_event()` and the processor's `EVENT_SCHEMA`.

    {"station_id": "DEBE010", "pollutant": "PM10",
     "event_time": "2024-01-01T00:00:00Z", "value": 12.5, "unit": "ug.m-3"}

The processor parses every message with `from_json` against that schema and
drops what does not fit: malformed JSON, a missing or null `station_id`,
`pollutant`, `event_time` or `value`, a value that is not a number, and an
event time in the future. The count of dropped messages is reported per
micro-batch as `invalid` in the processor log, events that arrive after the
3 h watermark as `dropped_late` (see README, "Processing"). Upstream the
producer only sends measurements the EEA marked as valid (`Validity = 1`,
about 10.85 M of 12.12 M rows) and logs how many it skipped. Among the
skipped rows are about 0.92 M with `Validity = 2`, valid readings below the
detection limit. Leaving them out follows the concept, but it biases the
means upward a little in clean periods, since the lowest readings are the
ones missing. A follow-up could let them through and mark them.

Not implemented, on purpose:

- **Dead-letter topic.** Invalid events are counted, not kept. With one
  producer under the same control as the consumer, a parked copy of a
  broken message has nobody to act on it; the counter is enough to notice a
  contract break. A DLQ would be a second `writeStream` on the `~valid`
  rows.
- **Schema registry.** The schema is versioned with the code in the same
  repository, which is what a registry buys you for a single team and a
  single producer. Karapace or Apicurio would be the step up when a second
  producer or a compatibility policy comes into play.

## Access control

- Database roles (`db/02_roles.sh`): `spark` may `SELECT`, `INSERT` and
  `UPDATE` the two aggregate tables and nothing else; `grafana` may
  `SELECT` them. The superuser is only used by the init scripts on first
  start. The test runner connects as `grafana`.
- Kafka runs without authentication on the internal Compose network and
  exposes one plaintext listener on localhost for inspection. Fine for a
  single-host lab setup; a shared deployment would put SASL/SCRAM and ACLs
  on the listeners.
- Grafana allows anonymous viewing of the provisioned dashboard; editing
  needs the admin login. The datasource is provisioned read-only
  (`editable: false`).
- Kafka-UI has no login and can create and delete topics; it is a
  development tool and would not ship in a shared setup.

## Secrets and configuration

All passwords and ports live in `.env`, which is gitignored; `.env.example`
carries placeholders. Compose refuses to start when a password is missing
(`${VAR:?}`), so nothing falls back to a default credential. Secrets reach
the containers as environment variables only.

## Retention

The aggregate tables are hypertables with a retention policy that drops
chunks older than three years, measured against `window_start`. The
replayed data is from 2024, so a shorter policy would delete the aggregates
as they are written; for a live feed the same policy would read weeks.
Kafka keeps the topic with its default retention (7 days); the replay file
and the raw download are regenerable and kept outside version control.

## Privacy

The data describes fixed public measuring stations, not people. Station
ids such as `DEBE010` are EEA identifiers published together with
coordinates and street addresses, so there is no personal data in the
pipeline and no GDPR processing role to document. The concept named
pseudonymised station ids as an option; it was not implemented because it
would hide public information without protecting anyone. If station ids
ever had to be masked, the place to do it is `to_event()` in the producer
(for example an HMAC over the id with a key from `.env`), so that neither
Kafka nor the database ever see the clear id.

## Where things are checked

- Contract and window logic: `tests/test_processor.py`, `tests/test_producer.py`
- Idempotent sink: `tests/test_sink.py`, crash behaviour in `kill-tests.md`
- End to end: `tests/test_e2e.py` compares topic, tables and dashboard with the replay file
