"""
Gold: customer_dim, carrier_dim, location_dim.

Silver already does the Type 1 reconciliation (one row per entity, latest
state), so these are a straightforward batch read of silver's current state,
with CDC bookkeeping columns (debezium_ts_ms) dropped -- business consumers
of gold shouldn't need to know these tables were built from CDC at all.

This is a BATCH job: it reads silver's current state at the moment it runs
and overwrites gold. It does not stream -- run it on-demand or on a schedule
(see run_gold_batch.py) to refresh gold with whatever's landed in silver
since the last run.

Run with (from the repo root, venv activated):
    python -m pipeline.gold.build_dims
"""

from pipeline.common.spark_session import GOLD_PATH, SILVER_PATH, ensure_data_dirs, get_spark_session

# Each entry: silver table name -> (gold table name, columns to keep, in order)
DIM_DEFINITIONS = {
    "customers": ("customer_dim", ["customer_id", "name", "email", "customer_type", "created_at", "updated_at"]),
    "carriers": ("carrier_dim", ["carrier_id", "carrier_name", "carrier_type", "updated_at"]),
    "locations": ("location_dim", ["location_id", "city", "state", "country", "location_type", "updated_at"]),
}


def build_dim(spark, silver_table: str, gold_table: str, columns: list[str]) -> int:
    silver_path = str(SILVER_PATH / silver_table)
    gold_path = str(GOLD_PATH / gold_table)

    df = spark.read.format("delta").load(silver_path).select(*columns)
    df.write.format("delta").mode("overwrite").save(gold_path)
    return df.count()


def main():
    ensure_data_dirs()
    spark = get_spark_session(app_name="gold-build-dims")

    for silver_table, (gold_table, columns) in DIM_DEFINITIONS.items():
        count = build_dim(spark, silver_table, gold_table, columns)
        print(f"{gold_table}: {count} rows (from silver/{silver_table})")


if __name__ == "__main__":
    main()