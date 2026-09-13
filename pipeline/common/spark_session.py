"""
Shared Spark session for the shipment pipeline.

Every bronze/silver/gold script should import get_spark_session() from here
rather than building its own SparkSession, so Delta Lake config and paths
stay consistent across the whole pipeline.
"""

from pathlib import Path

from pyspark.sql import SparkSession

# Repo root, resolved relative to this file (pipeline/common/spark_session.py -> repo root)
REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Local Delta Lake storage root. Each layer gets its own subfolder,
# e.g. data/bronze/shipments, data/silver/shipments, data/gold/shipment_fact.
# This whole folder is gitignored -- it's generated data, not source.
DATA_ROOT = REPO_ROOT / "data"

BRONZE_PATH = DATA_ROOT / "bronze"
SILVER_PATH = DATA_ROOT / "silver"
GOLD_PATH = DATA_ROOT / "gold"

# Local checkpoint location for Structured Streaming queries.
# Spark uses this to track what's already been processed from Kafka,
# so a restarted job resumes instead of reprocessing from the beginning.
CHECKPOINT_ROOT = DATA_ROOT / "_checkpoints"

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"  # matches the HOST listener in docker-compose.yml


def get_spark_session(app_name: str = "shipment-pipeline") -> SparkSession:
    """
    Build (or fetch, if one already exists) a SparkSession configured with
    Delta Lake support. Safe to call multiple times within the same process --
    Spark returns the existing session rather than creating a duplicate.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Kafka connector -- needed for Structured Streaming to read from Kafka topics.
        # Version suffix must match your installed pyspark version (3.5.1 here).
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "io.delta:delta-spark_2.12:3.2.0",
        )
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_data_dirs() -> None:
    """Create the local data directories if they don't already exist."""
    for path in [BRONZE_PATH, SILVER_PATH, GOLD_PATH, CHECKPOINT_ROOT]:
        path.mkdir(parents=True, exist_ok=True)