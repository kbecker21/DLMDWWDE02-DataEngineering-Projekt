"""Spark Structured Streaming job: validates sensor events from Kafka and
writes windowed aggregates to TimescaleDB.

Two queries run side by side, each with its own checkpoint and target table:

  tumbling_6h   6-hour windows per station and pollutant (avg/min/max/count)
  sliding_24h   24-hour mean sliding by 1 hour, PM10 and PM2.5 only

Events that do not match the JSON schema, lack a required field or carry an
event time in the future are dropped and counted per micro-batch. Late
events are tolerated up to the 3-hour watermark; anything later is dropped
by Spark and shows up in the batch log as well.

The sink is a foreachBatch upsert (INSERT ... ON CONFLICT DO UPDATE) keyed
by window start, station and pollutant. foreachBatch is at-least-once: after
a restart Spark re-runs the last unfinished batch, and the upsert makes that
harmless because the rows overwrite themselves.
"""

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
DB = psycopg.conninfo.make_conninfo(
    host=os.environ.get("DB_HOST", "timescaledb"),
    dbname=os.environ.get("DB_NAME", "airquality"),
    user=os.environ.get("DB_USER", "spark"),
    password=os.environ["DB_PASSWORD"],
    connect_timeout=10,
)

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
    """One log line per micro-batch with the numbers that matter for ops."""

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


def merge_sql(table, cols):
    """INSERT ... ON CONFLICT DO UPDATE from the staging table into `table`."""
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in UPSERT_KEY)
    return (
        f"INSERT INTO {table} ({', '.join(cols)}) SELECT {', '.join(cols)} FROM staging "
        f"ON CONFLICT ({', '.join(UPSERT_KEY)}) DO UPDATE SET {updates}, updated_at = now()"
    )


def upsert_sink(table):
    """foreachBatch function: COPY the batch into a temp table and merge it
    into the target in one statement. A failure raises, which stops the
    query; the container restarts and Spark re-runs the batch from the
    checkpoint."""

    def write(batch_df, batch_id):
        t0 = time.monotonic()
        cols = batch_df.columns
        # aggregates are small (thousands of rows per batch), so collecting
        # them on the driver is fine and keeps the write in one transaction
        rows = batch_df.collect()
        t1 = time.monotonic()
        if rows:
            with psycopg.connect(DB) as conn, conn.cursor() as cur:
                cur.execute(f"CREATE TEMP TABLE staging (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
                with cur.copy(f"COPY staging ({', '.join(cols)}) FROM STDIN") as copy:
                    for row in rows:
                        copy.write_row(tuple(_as_db_value(v) for v in row))
                cur.execute(merge_sql(table, cols))
        t2 = time.monotonic()
        log.info("sink=%s batch=%s rows=%s compute_ms=%.0f db_ms=%.0f", table, batch_id,
                 len(rows), (t1 - t0) * 1000, (t2 - t1) * 1000)

    return write


def main():
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

    parsed = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
    valid = (
        F.col("e").isNotNull()
        & F.col("e.station_id").isNotNull()
        & F.col("e.pollutant").isNotNull()
        & F.col("e.event_time").isNotNull()
        & F.col("e.value").isNotNull()
        & ~F.isnan("e.value")
        # a timestamp in the future would drag the watermark along and turn
        # every later event into a late one
        & (F.col("e.event_time") <= F.current_timestamp())
    )
    # each query gets its own copy of this plan and reads the topic itself
    events = (
        parsed.withColumn("valid", valid)
        .observe("validation", F.count(F.when(~F.col("valid"), 1)).alias("invalid"))
        .where("valid")
        .select("e.*")
        .withWatermark("event_time", WATERMARK)
    )

    tumbling = (
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

    # With a 1-hour slide a window starts at every full hour, so the CET
    # calendar day the PM limit values refer to (23:00-23:00 UTC) is always
    # one of them; no startTime offset needed.
    sliding = (
        events.where(F.col("pollutant").isin(PM))
        .groupBy(F.window("event_time", "24 hours", "1 hour"), "station_id", "pollutant")
        .agg(F.avg("value").alias("avg_value"), F.count("*").alias("n_values"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "station_id", "pollutant", "avg_value", "n_values",
        )
    )

    for name, df, table in [
        ("tumbling_6h", tumbling, "agg_tumbling_6h"),
        ("sliding_24h", sliding, "agg_sliding_24h"),
    ]:
        (
            df.writeStream.queryName(name)
            .outputMode("update")
            .foreachBatch(upsert_sink(table))
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/{name}")
            .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
            .start()
        )
    log.info("watermark=%s trigger=%ss topic=%s", WATERMARK, TRIGGER_SECONDS, TOPIC)
    # raises if either query fails, so the container exits and gets restarted
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
