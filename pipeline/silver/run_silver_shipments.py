"""
Silver processing for shipments: bronze/shipments -> silver/shipments (Type 1).

This is deliberately scoped to ONE entity first, to prove out the merge logic
before generalizing to customers/carriers/locations the same way bronze went
from one script to a config-driven set.

Run with (from the repo root, venv activated):
    python -m pipeline.silver.run_silver_shipments
"""

from pipeline.common.schemas import SHIPMENT_SCHEMA
from pipeline.common.silver_merge import run_silver_stream

BRONZE_TABLE = "shipments"
SILVER_TABLE = "shipments"
PRIMARY_KEY = "shipment_id"


if __name__ == "__main__":
    run_silver_stream(
        bronze_table=BRONZE_TABLE,
        silver_table=SILVER_TABLE,
        schema=SHIPMENT_SCHEMA,
        primary_key=PRIMARY_KEY,
        app_name="silver-shipments",
    )