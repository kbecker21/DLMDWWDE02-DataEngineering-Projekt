# Governance and security

## Lineage

| step | output | changes |
|------|--------|---------|
| downloader | `data/eea_e1a_2024/<pollutant>/<station>.parquet` | only 2024 rows |
| preprocess | `data/replay/events.parquet` | merged, sorted by `Start` |
| producer | topic `sensor-events` | `Validity != 1` dropped, UTC+1 -> UTC, station id extracted |
| processor | `agg_tumbling_6h`, `agg_sliding_24h` | invalid events dropped, aggregated |

Raw files stay unchanged; everything downstream can be rebuilt. The release
mirror is an unmodified copy of the EEA files (CC BY 4.0).

## Data contract

`to_event()` in the producer and `EVENT_SCHEMA` in the processor:

    {"station_id": "DEBE010", "pollutant": "PM10",
     "event_time": "2024-01-01T00:00:00Z", "value": 12.5, "unit": "ug.m-3"}

Dropped and counted as `invalid`: malformed JSON, missing fields,
non-numeric value, timestamp in the future. ~0.92M rows with `Validity = 2`
(below detection limit) are not sent, which biases means slightly upward.

No dead-letter topic and no schema registry: one producer, schema versioned
in this repo.

## Access

- `spark`: SELECT, INSERT, UPDATE on the two tables. `grafana`: SELECT.
  Superuser only for the init scripts.
- Passwords only in `.env` (gitignored); Compose fails if one is missing.
- All ports bound to 127.0.0.1. Kafka and Kafka-UI have no authentication;
  a shared setup would need SASL/ACLs and no Kafka-UI.
- Grafana: anonymous view, admin login for editing.

## Retention

Hypertables: 3 years on `window_start` (data is from 2024). Kafka: default
7 days.

## Privacy

Station ids are public EEA identifiers, no personal data. Pseudonymisation
is not implemented; it would go into `to_event()`.
