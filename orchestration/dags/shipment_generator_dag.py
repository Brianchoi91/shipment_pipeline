"""
Airflow DAG: orchestrates the shipment lifecycle generator.

Runs create_shipments() then advance_shipments() on a schedule, replacing
manual `python generate_shipments.py --create / --advance` calls with a
scheduled, monitorable job.

--- One-time setup (lean, standalone Airflow -- no Docker, no separate metadata DB) ---

This intentionally uses `airflow standalone`: a single process bundling the
webserver, scheduler, and a SQLite metadata database. That's a deliberate
scope choice for a local portfolio project -- production Airflow runs with
LocalExecutor/CeleryExecutor and a real Postgres metadata store instead.

1. Install (Airflow 3.3.1, matched to Python 3.11):

    pip install "apache-airflow==3.3.1" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.11.txt"

   This is deliberately NOT added to the project's root requirements.txt --
   Airflow needs its own version-matched constraints file to install
   correctly, and pulls in a large, easily-conflicting dependency tree that
   could clash with pyspark/pandas pins already in that file.

2. Point Airflow at THIS repo's DAGs folder, keep its own metadata/logs out
   of version control, AND put the repo root on PYTHONPATH so this DAG can
   import data_generator (run from the repo root):

    export AIRFLOW_HOME="$(pwd)/airflow_home"
    export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/orchestration/dags"
    export PYTHONPATH="$(pwd):$PYTHONPATH"

   (airflow_home/ is gitignored -- it's local Airflow state, not source code.
   Without the PYTHONPATH export, Airflow's DAG processor can't find the
   data_generator package and this DAG fails to import with
   ModuleNotFoundError.)

3. Run everything:

    airflow standalone

   First run prints an auto-generated admin username/password in the
   terminal -- save it. Then open http://localhost:8080, find the
   "shipment_generator" DAG, and toggle it on.

Note: step 2's exports only last for your current terminal session -- you'll
need to re-export all three (or add them to your shell profile) each time you
open a new terminal to run `airflow standalone` in.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task

from data_generator.generate_shipments import advance_shipments, create_shipments, get_connection

# How many new shipments to create per DAG run. Kept small since this runs
# frequently (every 3 minutes) -- the point is steady background activity
# for the pipeline to process, not a burst load.
SHIPMENTS_PER_RUN = 3


@task
def create_shipments_task() -> None:
    conn = get_connection()
    try:
        create_shipments(conn, count=SHIPMENTS_PER_RUN)
    finally:
        conn.close()


@task
def advance_shipments_task() -> None:
    conn = get_connection()
    try:
        advance_shipments(conn)
    finally:
        conn.close()


@dag(
    dag_id="shipment_generator",
    description="Creates new shipments and advances in-flight ones -- automates the manual generator CLI calls.",
    schedule="*/3 * * * *",  # every 3 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["shipment-pipeline"],
)
def shipment_generator_dag():
    create_shipments_task() >> advance_shipments_task()


shipment_generator_dag()