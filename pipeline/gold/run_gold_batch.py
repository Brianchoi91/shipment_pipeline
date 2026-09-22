"""
Gold batch orchestrator: rebuilds the three CDC-sourced dims, then
shipment_fact.

Run with (from the repo root, venv activated):
    python -m pipeline.gold.run_gold_batch

This is a good candidate for a future Airflow DAG (orchestration/dags/),
the same way the generator is scheduled -- not built yet, run manually or
add a DAG when that's worth doing.
"""

from pipeline.gold import build_dims, build_shipment_fact


def main():
    print("=== Building customer_dim, carrier_dim, location_dim ===")
    build_dims.main()

    print("\n=== Building shipment_fact ===")
    build_shipment_fact.main()

    print("\nGold layer refresh complete.")


if __name__ == "__main__":
    main()