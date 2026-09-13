"""
Shared Kafka -> bronze Delta ingestion logic.

start_bronze_stream() builds and starts ONE streaming query for a given
topic/table, but does not block -- this lets a caller start several of these
concurrently within the same SparkSession (see pipeline/bronze/run_bronze_ingestion.py),
rather than needing one process per topic.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, get_json_object

from pipeline.common.spark_session import BRONZE_PATH, CHECKPOINT_ROOT, KAFKA_BOOTSTRAP_SERVERS


def _parse_cdc_envelope(raw: DataFrame) -> DataFrame:
    """
    Cast Kafka's binary key/value to strings and pull out the top-level
    Debezium CDC envelope fields. before/after stay as raw JSON strings --
    silver parses them with a real schema. Keeping bronze this raw makes it
    resilient to upstream schema changes (a new Postgres column just shows up
    inside the JSON string without breaking this job).
    """
    events = raw.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("kafka_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    return events.select(
        "kafka_key",
        "kafka_value",
        get_json_object(col("kafka_value"), "$.op").alias("op"),
        get_json_object(col("kafka_value"), "$.ts_ms").alias("debezium_ts_ms"),
        get_json_object(col("kafka_value"), "$.before").alias("before_json"),
        get_json_object(col("kafka_value"), "$.after").alias("after_json"),
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        current_timestamp().alias("ingested_at"),
    )


def start_bronze_stream(spark: SparkSession, topic: str, table_name: str):
    """
    Build and start (non-blocking) one streaming query: topic -> bronze Delta table.
    Returns the StreamingQuery handle so the caller can track/await it.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        # Only takes effect on a table's very first run (no checkpoint yet) --
        # picks up whatever's already in the topic. Every run after that
        # resumes from the checkpoint regardless of this setting.
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = _parse_cdc_envelope(raw)

    query = (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(CHECKPOINT_ROOT / table_name))
        .trigger(processingTime="10 seconds")
        .queryName(f"bronze_{table_name}")
        .start(str(BRONZE_PATH / table_name))
    )

    print(f"Started stream: {topic} -> {BRONZE_PATH / table_name}")
    return query