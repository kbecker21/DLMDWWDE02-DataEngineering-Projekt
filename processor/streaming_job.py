"""Reads sensor events from Kafka, drops invalid ones and upserts 6 h tumbling
and 24 h sliding aggregates into TimescaleDB."""

import logging
import os
import time
from datetime import datetime, timezone

import psycopg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "sensor-events")
TRIGGER_SECONDS = int(os.environ.get("TRIGGER_SECONDS", "10"))
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/checkpoints")
WATERMARK = "3 hours"
PM = ["PM10", "PM25"]

# data contract with the producer; anything else is invalid
EVENT_SCHEMA = StructType([
    StructField("station_id", StringType()),
    StructField("pollutant", StringType()),
    StructField("event_time", TimestampType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
])

UPSERT_KEY = ("window_start", "station_id", "pollutant")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("processor")
logging.getLogger("py4j").setLevel(logging.WARNING)


class BatchLog(StreamingQueryListener):
    """Logs one line per micro-batch."""

    def onQueryStarted(self, event):
        log.info("query started: %s", event.name)

    def onQueryProgress(self, event):
        p = event.progress
        validation = p.observedMetrics.get("validation")
        invalid = validation["invalid"] if validation else 0
        dropped_late = sum(s.numRowsDroppedByWatermark for s in p.stateOperators)
        state_rows = sum(s.numRowsTotal for s in p.stateOperators)
        log.info(
            "query=%s batch=%s input=%s invalid=%s dropped_late=%s state_rows=%s "
            "watermark=%s rate=%.0f/s ms=%s",
            p.name, p.batchId, p.numInputRows, invalid, dropped_late, state_rows,
            p.eventTime.get("watermark", "-"), p.processedRowsPerSecond,
            p.durationMs.get("triggerExecution", "-"),
        )

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.info("query terminated: %s exception=%s", event.id, event.exception)


def _as_db_value(v):
    # PySpark hands timestamps over as naive datetimes in the session time
    # zone (UTC); make that explicit for the timestamptz columns
    if isinstance(v, datetime) and v.tzinfo is None:
        return v.replace(tzinfo=timezone.utc)
    return v


def db_conninfo():
    return psycopg.conninfo.make_conninfo(
        host=os.environ.get("DB_HOST", "timescaledb"),
        dbname=os.environ.get("DB_NAME", "airquality"),
        user=os.environ.get("DB_USER", "spark"),
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
    )


def merge_sql(table, cols):
    """INSERT ... ON CONFLICT DO UPDATE from the staging table into `table`."""
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in UPSERT_KEY)
    return (
        f"INSERT INTO {table} ({', '.join(cols)}) SELECT {', '.join(cols)} FROM staging "
        f"ON CONFLICT ({', '.join(UPSERT_KEY)}) DO UPDATE SET {updates}, updated_at = now()"
    )


def upsert_sink(table, db):
    """COPY the batch into a temp table and merge it into `table`. On error the
    query stops and Spark re-runs the batch after the restart."""

    def write(batch_df, batch_id):
        t0 = time.monotonic()
        cols = batch_df.columns
        # a few thousand rows per batch, fine to collect on the driver
        rows = batch_df.collect()
        t1 = time.monotonic()
        if rows:
            with psycopg.connect(db) as conn, conn.cursor() as cur:
                cur.execute(f"CREATE TEMP TABLE staging (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
                with cur.copy(f"COPY staging ({', '.join(cols)}) FROM STDIN") as copy:
                    for row in rows:
                        copy.write_row(tuple(_as_db_value(v) for v in row))
                cur.execute(merge_sql(table, cols))
        t2 = time.monotonic()
        log.info("sink=%s batch=%s rows=%s compute_ms=%.0f db_ms=%.0f", table, batch_id,
                 len(rows), (t1 - t0) * 1000, (t2 - t1) * 1000)

    return write


def valid_events(raw):
    """Parse the Kafka value column against EVENT_SCHEMA and keep what fits.
    Rows that fail are counted in the `validation` metric."""
    parsed = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
    valid = (
        F.col("e").isNotNull()
        & F.col("e.station_id").isNotNull()
        & F.col("e.pollutant").isNotNull()
        & F.col("e.event_time").isNotNull()
        & F.col("e.value").isNotNull()
        & ~F.isnan("e.value")
        # a future timestamp would push the watermark ahead
        & (F.col("e.event_time") <= F.current_timestamp())
    )
    return (
        parsed.withColumn("valid", valid)
        .observe("validation", F.count(F.when(~F.col("valid"), 1)).alias("invalid"))
        .where("valid")
        .select("e.*")
    )


def tumbling_6h(events):
    return (
        events.groupBy(F.window("event_time", "6 hours"), "station_id", "pollutant")
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count("*").alias("n_values"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "station_id", "pollutant", "avg_value", "min_value", "max_value", "n_values",
        )
    )


def sliding_24h(events):
    # a window starts every hour, so the CET day (23:00-23:00 UTC) is one of them
    return (
        events.where(F.col("pollutant").isin(PM))
        .groupBy(F.window("event_time", "24 hours", "1 hour"), "station_id", "pollutant")
        .agg(F.avg("value").alias("avg_value"), F.count("*").alias("n_values"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "station_id", "pollutant", "avg_value", "n_values",
        )
    )


def main():
    db = db_conninfo()
    spark = SparkSession.builder.appName("sensor-processor").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(BatchLog())

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 500_000)
        .load()
    )
    # each query gets its own copy of this plan and reads the topic itself
    events = valid_events(raw).withWatermark("event_time", WATERMARK)

    for name, df, table in [
        ("tumbling_6h", tumbling_6h(events), "agg_tumbling_6h"),
        ("sliding_24h", sliding_24h(events), "agg_sliding_24h"),
    ]:
        (
            df.writeStream.queryName(name)
            .outputMode("update")
            .foreachBatch(upsert_sink(table, db))
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/{name}")
            .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
            .start()
        )
    log.info("watermark=%s trigger=%ss topic=%s", WATERMARK, TRIGGER_SECONDS, TOPIC)
    # raises if either query fails, so the container exits and gets restarted
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
