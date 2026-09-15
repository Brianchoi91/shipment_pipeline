"""
Shared bronze -> silver logic: Type 1 (latest-wins) reconciliation of CDC diffs.

Bronze is append-only -- every insert/update/delete from Postgres becomes a new
row. Silver's job is to collapse that down to "one row per primary key,
reflecting its current state." This is done with Delta's MERGE INTO, run once
per micro-batch via foreachBatch (which is what lets us run arbitrary SQL like
MERGE inside a streaming query at all).

Three outcomes per incoming record, based on its `op` field:
  - op == 'd'                                  -> DELETE the row from silver
  - op in ('c','u') and it's the newest version -> UPDATE (or INSERT if new)
  - a stale/out-of-order event                  -> ignored (see debezium_ts_ms check)
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, from_json, row_number
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from pipeline.common.schemas import TIMESTAMP_COLUMNS
from pipeline.common.spark_session import BRONZE_PATH, CHECKPOINT_ROOT, SILVER_PATH, ensure_data_dirs, get_spark_session


def _parse_and_dedupe_batch(
    micro_batch_df: DataFrame, schema: StructType, primary_key: str, entity_name: str
) -> DataFrame:
    """
    Parse the raw before/after JSON into typed columns, and collapse each
    micro-batch down to one row per primary key -- keeping only the latest
    event for that key. This matters because a single micro-batch can contain
    several events for the same shipment (e.g. created then immediately
    cancelled), and Delta's MERGE INTO will error if more than one source row
    matches the same target row.
    """
    parsed = (
        micro_batch_df
        # For deletes, after_json is null -- fall back to before_json so we
        # still have the primary key (and last known state) to delete by.
        .withColumn("effective_json", coalesce(col("after_json"), col("before_json")))
        .withColumn("parsed", from_json(col("effective_json"), schema))
        .withColumn("debezium_ts_ms", col("debezium_ts_ms").cast("long"))
        .select("parsed.*", "op", "debezium_ts_ms")
    )

    # Cast this entity's timestamp columns from millis-since-epoch longs to real timestamps.
    for ts_col in TIMESTAMP_COLUMNS.get(entity_name, []):
        parsed = parsed.withColumn(ts_col, (col(ts_col) / 1000).cast("timestamp"))

    window = Window.partitionBy(primary_key).orderBy(col("debezium_ts_ms").desc())
    deduped = (
        parsed.withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )
    return deduped


def _upsert_batch(
    micro_batch_df: DataFrame,
    batch_id: int,
    spark: SparkSession,
    schema: StructType,
    primary_key: str,
    entity_name: str,
    silver_path: str,
) -> None:
    deduped = _parse_and_dedupe_batch(micro_batch_df, schema, primary_key, entity_name)
    deduped.persist()

    try:
        if deduped.count() == 0:
            return

        if not DeltaTable.isDeltaTable(spark, silver_path):
            # First-ever batch: no silver table to merge into yet, so just write
            # the initial rows directly (excluding any deletes -- nothing to
            # delete on a table that doesn't exist yet).
            deduped.filter(col("op") != "d").drop("op", "debezium_ts_ms").write.format(
                "delta"
            ).mode("overwrite").save(silver_path)
            return

        target = DeltaTable.forPath(spark, silver_path)
        (
            target.alias("target")
            .merge(deduped.alias("source"), f"target.{primary_key} = source.{primary_key}")
            .whenMatchedDelete(condition="source.op = 'd'")
            .whenMatchedUpdateAll(
                condition="source.op != 'd' AND source.debezium_ts_ms > target.debezium_ts_ms"
            )
            .whenNotMatchedInsertAll(condition="source.op != 'd'")
            .execute()
        )
    finally:
        deduped.unpersist()


def run_silver_stream(
    bronze_table: str,
    silver_table: str,
    schema: StructType,
    primary_key: str,
    app_name: str,
) -> None:
    """
    Stream a bronze Delta table -> reconciled silver Delta table (Type 1).
    Blocks (awaitTermination) -- meant to run as a long-lived job.
    """
    ensure_data_dirs()
    spark = get_spark_session(app_name=app_name)

    bronze_path = str(BRONZE_PATH / bronze_table)
    silver_path = str(SILVER_PATH / silver_table)
    checkpoint_path = str(CHECKPOINT_ROOT / f"silver_{silver_table}")

    bronze_stream = spark.readStream.format("delta").load(bronze_path)

    query = (
        bronze_stream.writeStream.foreachBatch(
            lambda micro_batch_df, batch_id: _upsert_batch(
                micro_batch_df, batch_id, spark, schema, primary_key, silver_table, silver_path
            )
        )
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"Streaming bronze/{bronze_table} -> silver/{silver_table} (Type 1, key={primary_key})")
    print("Ctrl+C to stop.")
    query.awaitTermination()