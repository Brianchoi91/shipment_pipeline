"""
Silver processing orchestrator: reads pipeline/silver/entities.yaml and starts
one bronze -> silver Type 1 merge stream per entity, all within a single Spark
session/process -- the same pattern used for bronze's run_bronze_ingestion.py.

To add a new entity to silver: add it to entities.yaml AND add its schema to
SCHEMA_REGISTRY in pipeline/common/schemas.py. Nothing else needs to change.

Run with (from the repo root, venv activated):
    python -m pipeline.silver.run_silver_processing

Runs until Ctrl+C, or until any one stream fails.
"""

from pathlib import Path

import yaml

from pipeline.common.schemas import SCHEMA_REGISTRY
from pipeline.common.silver_merge import start_silver_stream
from pipeline.common.spark_session import ensure_data_dirs, get_spark_session

CONFIG_PATH = Path(__file__).resolve().parent / "entities.yaml"


def load_entity_config() -> list[dict]:
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config["entities"]


def main():
    ensure_data_dirs()
    spark = get_spark_session(app_name="silver-processing")

    entities = load_entity_config()
    print(f"Loaded {len(entities)} entity(ies) from {CONFIG_PATH.name}")

    queries = [
        start_silver_stream(
            spark,
            bronze_table=entity["name"],
            silver_table=entity["name"],
            schema=SCHEMA_REGISTRY[entity["name"]],
            primary_key=entity["primary_key"],
        )
        for entity in entities
    ]

    print(f"\n{len(queries)} stream(s) running. Ctrl+C to stop.\n")

    # Same trade-off noted in run_bronze_ingestion.py: if any single stream
    # fails, this returns and the whole process exits. Check spark.streams.active
    # and query.exception() on the stopped one to see which failed and why.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()