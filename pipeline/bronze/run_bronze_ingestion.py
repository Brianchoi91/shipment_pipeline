"""
Bronze ingestion orchestrator: reads pipeline/bronze/topics.yaml and starts one
Structured Streaming query per topic listed there, all within a single Spark
session/process.

To add a new source table to bronze: add an entry to topics.yaml. Nothing else
needs to change.

Run with (from the repo root, venv activated):
    python -m pipeline.bronze.run_bronze_ingestion

Runs until Ctrl+C, or until any one stream fails (see note at the bottom).
"""

from pathlib import Path

import yaml

from pipeline.common.kafka_bronze import start_bronze_stream
from pipeline.common.spark_session import ensure_data_dirs, get_spark_session

CONFIG_PATH = Path(__file__).resolve().parent / "topics.yaml"


def load_topic_config() -> list[dict]:
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config["topics"]


def main():
    ensure_data_dirs()
    spark = get_spark_session(app_name="bronze-ingestion")

    topics = load_topic_config()
    print(f"Loaded {len(topics)} topic(s) from {CONFIG_PATH.name}")

    queries = [
        start_bronze_stream(spark, topic=entry["topic"], table_name=entry["table"])
        for entry in topics
    ]

    print(f"\n{len(queries)} stream(s) running. Ctrl+C to stop.")
    print("Run the generator in another terminal to see new data flow in.\n")

    # awaitAnyTermination() blocks until ONE of the streams stops (Ctrl+C, or a
    # failure in any single query). If one query errors out, this returns and
    # the script exits -- at that point check spark.streams.active to see which
    # queries are still running and query.exception() on the one that stopped
    # to see why.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()