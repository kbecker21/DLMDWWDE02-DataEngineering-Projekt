# DLMDWWDE02 — Streaming backend for air quality reporting

IU portfolio project (module "Projekt: Data Engineering", task 2): a
containerized streaming pipeline that ingests air quality measurements as a
replayed stream and serves windowed aggregates for reporting.

Planned pipeline: replay producer -> Kafka -> Spark Structured Streaming ->
TimescaleDB -> Grafana. Runs locally with Docker Compose. Work in progress,
built up service by service.

## Data

EEA air quality data (dataset E1a, validated hourly values), Germany 2024,
pollutants NO2, O3, PM10, PM2.5: 1,380 station time series, ~12.1M rows.

Download (one-off setup step, ~380 MB transfer):

    docker compose run --rm downloader

The downloader tries the EEA download API first, then the public blob
container, then a mirror of the 2024 cut attached to a GitHub release.
Output goes to `data/eea_e1a_2024/` (gitignored). Re-running resumes.

There is also a small sample in `data/sample/` (4 Berlin stations, ~3 MB),
so the stack can be tried without downloading anything.

Data © [European Environment Agency](https://www.eea.europa.eu/), licensed
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). The release mirror
is an unmodified subset (year 2024, Germany) of that dataset.

## Layout

    docker-compose.yml    full stack (infrastructure as code)
    .env.example          config template, copy to .env
    downloader/           one-off data downloader
    producer/             replay producer
    processor/            Spark Structured Streaming job
    db/                   TimescaleDB init scripts
    grafana/              provisioned datasource + dashboard
    tests/                pytest
    docs/                 architecture and ops notes

## Requirements

Docker Desktop (developed on Windows 11 / WSL2). No local Python needed.
