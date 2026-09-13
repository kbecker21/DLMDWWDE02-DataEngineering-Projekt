-- Serving tables for the Spark aggregates. One table per query; the primary
-- key is the upsert key, so a replayed micro-batch overwrites its own rows
-- instead of duplicating them.

CREATE TABLE agg_tumbling_6h (
    window_start TIMESTAMPTZ      NOT NULL,
    window_end   TIMESTAMPTZ      NOT NULL,
    station_id   TEXT             NOT NULL,
    pollutant    TEXT             NOT NULL,
    avg_value    DOUBLE PRECISION NOT NULL,
    min_value    DOUBLE PRECISION NOT NULL,
    max_value    DOUBLE PRECISION NOT NULL,
    n_values     BIGINT           NOT NULL,
    updated_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, station_id, pollutant)
);

-- 24 h mean sliding by 1 h, PM10 and PM2.5 only
CREATE TABLE agg_sliding_24h (
    window_start TIMESTAMPTZ      NOT NULL,
    window_end   TIMESTAMPTZ      NOT NULL,
    station_id   TEXT             NOT NULL,
    pollutant    TEXT             NOT NULL,
    avg_value    DOUBLE PRECISION NOT NULL,
    n_values     BIGINT           NOT NULL,
    updated_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, station_id, pollutant)
);

SELECT create_hypertable('agg_tumbling_6h', 'window_start', chunk_time_interval => INTERVAL '1 month');
SELECT create_hypertable('agg_sliding_24h', 'window_start', chunk_time_interval => INTERVAL '1 month');

-- Retention is measured against window_start, and the replayed data is from
-- 2024. Anything shorter than the age of that data would drop the aggregates
-- as soon as they are written, so the policy is generous here; a live feed
-- would use weeks, not years.
SELECT add_retention_policy('agg_tumbling_6h', drop_after => INTERVAL '3 years');
SELECT add_retention_policy('agg_sliding_24h', drop_after => INTERVAL '3 years');
