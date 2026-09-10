"""Spark Structured Streaming job: validates sensor events from Kafka and
aggregates them into 6-hour tumbling windows per station and pollutant.

Events that do not match the JSON schema (or lack a required field) are
dropped and counted per micro-batch. Late events are tolerated up to the
3-hour watermark; anything later is dropped by Spark and shows up in the
batch log as well.

Output goes to the console for now; the TimescaleDB sink follows.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "sensor-events")
TRIGGER_SECONDS = int(os.environ.get("TRIGGER_SECONDS", "10"))
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/checkpoints")

WINDOW = "6 hours"
WATERMARK = "3 hours"

# data contract with the producer; anything else is invalid
EVENT_SCHEMA = StructType([
    StructField("station_id", StringType()),
    StructField("pollutant", StringType()),
    StructField("event_time", TimestampType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
])

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("processor")


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
            "batch=%s input=%s invalid=%s dropped_late=%s output=%s state_rows=%s "
            "watermark=%s rate=%.0f/s",
            p.batchId, p.numInputRows, invalid, dropped_late, p.sink.numOutputRows,
            state_rows, p.eventTime.get("watermark", "-"), p.processedRowsPerSecond,
        )

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.info("query terminated: %s exception=%s", event.id, event.exception)


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
    )
    events = (
        parsed.withColumn("valid", valid)
        .observe("validation", F.count(F.when(~F.col("valid"), 1)).alias("invalid"))
        .where("valid")
        .select("e.*")
    )

    tumbling = (
        events.withWatermark("event_time", WATERMARK)
        .groupBy(F.window("event_time", WINDOW), "station_id", "pollutant")
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

    query = (
        tumbling.writeStream.queryName("tumbling_6h")
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/tumbling_6h")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )
    log.info("window=%s watermark=%s trigger=%ss topic=%s", WINDOW, WATERMARK, TRIGGER_SECONDS, TOPIC)
    query.awaitTermination()


if __name__ == "__main__":
    main()
