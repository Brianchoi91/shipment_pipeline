"""
Gold: shipment_fact -- an accumulating snapshot fact table.

One row per shipment, reflecting its current state (mirrors silver's Type 1
shipments table almost directly). The only real transformation here:
  - derive has_exception: true if this shipment EVER hit an exception event,
    even if it later resolved back to in_transit -- this is different from
    checking current status == 'exception', which would miss resolved cases
  - drop CDC bookkeeping columns (debezium_ts_ms) not meant for consumers

No separate date_dim -- created_at/picked_up_at/etc. are already real
TimestampType columns, so any date attribute (day of week, quarter, ...) is
one date_format()/quarter() call away at query time. Materializing those
into a lookup table wasn't earning its cost here (no BI tool consuming this
gold layer to justify the dimensional-hierarchy pattern date_dim exists for).

This is a BATCH job -- see build_dims.py's docstring for why.

Run with (from the repo root, venv activated):
    python -m pipeline.gold.build_shipment_fact
"""

from pyspark.sql.functions import col

from pipeline.common.spark_session import GOLD_PATH, SILVER_PATH, ensure_data_dirs, get_spark_session

FACT_COLUMNS = [
    "shipment_id",
    "customer_id",
    "carrier_id",
    "origin_location_id",
    "destination_location_id",
    "status",
    "weight_kg",
    "service_level",
    "created_at",
    "picked_up_at",
    "in_transit_at",
    "out_for_delivery_at",
    "delivered_at",
    "exception_at",
    "exception_reason",
    "has_exception",
    "updated_at",
]


def main():
    ensure_data_dirs()
    spark = get_spark_session(app_name="gold-build-shipment-fact")

    silver_path = str(SILVER_PATH / "shipments")
    gold_path = str(GOLD_PATH / "shipment_fact")

    df = (
        spark.read.format("delta")
        .load(silver_path)
        .withColumn("has_exception", col("exception_at").isNotNull())
        .select(*FACT_COLUMNS)
    )

    df.write.format("delta").mode("overwrite").save(gold_path)
    print(f"shipment_fact: {df.count()} rows (from silver/shipments)")


if __name__ == "__main__":
    main()