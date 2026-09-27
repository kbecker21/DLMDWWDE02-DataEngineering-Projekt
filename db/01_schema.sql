-- One table per Spark query, the primary key is the upsert key.

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

-- the data is from 2024, a shorter retention would drop it right away
SELECT add_retention_policy('agg_tumbling_6h', drop_after => INTERVAL '3 years');
SELECT add_retention_policy('agg_sliding_24h', drop_after => INTERVAL '3 years');
