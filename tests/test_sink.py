"""The upsert statement and the value conversion of the TimescaleDB sink."""

from datetime import datetime, timezone

import streaming_job as job


def test_merge_sql_updates_everything_but_the_key():
    cols = ["window_start", "window_end", "station_id", "pollutant", "avg_value", "n_values"]
    sql = job.merge_sql("agg_sliding_24h", cols)
    assert sql.startswith(
        "INSERT INTO agg_sliding_24h (window_start, window_end, station_id, pollutant, avg_value, n_values) "
        "SELECT window_start, window_end, station_id, pollutant, avg_value, n_values FROM staging "
        "ON CONFLICT (window_start, station_id, pollutant) DO UPDATE SET "
    )
    assert sql.endswith(
        "window_end = EXCLUDED.window_end, avg_value = EXCLUDED.avg_value, "
        "n_values = EXCLUDED.n_values, updated_at = now()"
    )
    for key in job.UPSERT_KEY:
        assert f"{key} = EXCLUDED" not in sql


def test_naive_timestamps_are_utc():
    assert job._as_db_value(datetime(2024, 1, 1)) == datetime(2024, 1, 1, tzinfo=timezone.utc)
    aware = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert job._as_db_value(aware) is aware
    assert job._as_db_value(3.5) == 3.5
    assert job._as_db_value("DEBE010") == "DEBE010"
