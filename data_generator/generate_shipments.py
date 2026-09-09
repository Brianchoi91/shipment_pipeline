"""
Shipment lifecycle generator.

Simulates a logistics OLTP system:
- seeds reference data (customers, carriers, locations)
- creates new shipments
- advances existing shipments through their lifecycle via UPDATEs

Every UPDATE against `shipments` is what Debezium captures as a CDC event,
so this script is effectively "the thing CDC is watching."

Usage:
    python generate_shipments.py --seed                 # one-time reference data
    python generate_shipments.py --create 20             # create 20 new shipments
    python generate_shipments.py --advance                # progress in-flight shipments one step
    python generate_shipments.py --loop --interval 5      # continuously create + advance
"""

import argparse
import os
import random
import time
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from faker import Faker

# Load .env from the repo root, regardless of which directory this script is run from.
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

fake = Faker()

DB_CONFIG = {
    "host": "localhost",
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "shipment_db"),
    "user": os.getenv("POSTGRES_USER", "shipment_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "shipment_pass"),
}

# Happy-path progression. Each shipment moves forward one stage at a time.
HAPPY_PATH = ["created", "picked_up", "in_transit", "out_for_delivery", "delivered"]

# Probability that an in_transit shipment hits an exception instead of progressing normally
EXCEPTION_PROBABILITY = 0.12

EXCEPTION_REASONS = ["damaged", "lost", "weather_delay", "customs_hold"]

# Probability an exception resolves back into transit vs. becomes a return
EXCEPTION_RESOLVES_PROBABILITY = 0.6


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def seed_reference_data(conn, n_customers=25, n_carriers=5, n_locations=15):
    """One-time seed of customers, carriers, locations. Safe to call once."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM customers")
        if cur.fetchone()[0] > 0:
            print("Reference data already seeded, skipping.")
            return

        for _ in range(n_customers):
            cur.execute(
                """
                INSERT INTO customers (name, email, customer_type)
                VALUES (%s, %s, %s)
                """,
                (
                    fake.name(),
                    fake.email(),
                    random.choice(["individual", "business"]),
                ),
            )

        carrier_names = ["Northbound Freight", "SkyLink Air Cargo", "BlueWave Ocean",
                          "SwiftGround Logistics", "Meridian Shipping"]
        carrier_types = ["ground", "air", "ocean", "ground", "ocean"]
        for name, ctype in zip(carrier_names, carrier_types):
            cur.execute(
                "INSERT INTO carriers (carrier_name, carrier_type) VALUES (%s, %s)",
                (name, ctype),
            )

        location_types = ["warehouse", "hub", "customer_address"]
        for _ in range(n_locations):
            cur.execute(
                """
                INSERT INTO locations (city, state, country, location_type)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    fake.city(),
                    fake.state_abbr(),
                    "USA",
                    random.choice(location_types),
                ),
            )

    conn.commit()
    print(f"Seeded {n_customers} customers, {len(carrier_names)} carriers, {n_locations} locations.")


def create_shipments(conn, count=10):
    """Insert new shipments in the 'created' state."""
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM customers")
        customer_ids = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT carrier_id FROM carriers")
        carrier_ids = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT location_id FROM locations")
        location_ids = [row[0] for row in cur.fetchall()]

        if not (customer_ids and carrier_ids and location_ids):
            raise RuntimeError("Reference data missing — run with --seed first.")

        new_ids = []
        for _ in range(count):
            origin, dest = random.sample(location_ids, 2)
            cur.execute(
                """
                INSERT INTO shipments
                    (customer_id, carrier_id, origin_location_id, destination_location_id,
                     status, weight_kg, service_level, created_at, updated_at)
                VALUES (%s, %s, %s, %s, 'created', %s, %s, now(), now())
                RETURNING shipment_id
                """,
                (
                    random.choice(customer_ids),
                    random.choice(carrier_ids),
                    origin,
                    dest,
                    round(random.uniform(0.5, 80.0), 2),
                    random.choice(["standard", "express", "overnight"]),
                ),
            )
            new_ids.append(cur.fetchone()[0])

    conn.commit()
    print(f"Created {count} new shipments: {new_ids}")
    return new_ids


def advance_shipments(conn):
    """
    Progress every in-flight shipment by one lifecycle step.
    This is the function that produces the interesting CDC UPDATE stream.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT shipment_id, status FROM shipments
            WHERE status NOT IN ('delivered', 'returned')
            """
        )
        rows = cur.fetchall()

        advanced, exceptioned, resolved, returned = 0, 0, 0, 0

        for shipment_id, status in rows:
            if status == "exception":
                # Resolve: either go back to in_transit or become a return
                if random.random() < EXCEPTION_RESOLVES_PROBABILITY:
                    cur.execute(
                        """
                        UPDATE shipments
                        SET status = 'in_transit', updated_at = now()
                        WHERE shipment_id = %s
                        """,
                        (shipment_id,),
                    )
                    resolved += 1
                else:
                    cur.execute(
                        """
                        UPDATE shipments
                        SET status = 'returned', updated_at = now()
                        WHERE shipment_id = %s
                        """,
                        (shipment_id,),
                    )
                    returned += 1
                continue

            # Chance to throw an exception while in_transit
            if status == "in_transit" and random.random() < EXCEPTION_PROBABILITY:
                cur.execute(
                    """
                    UPDATE shipments
                    SET status = 'exception',
                        exception_at = now(),
                        exception_reason = %s,
                        updated_at = now()
                    WHERE shipment_id = %s
                    """,
                    (random.choice(EXCEPTION_REASONS), shipment_id),
                )
                exceptioned += 1
                continue

            # Normal happy-path progression
            current_idx = HAPPY_PATH.index(status)
            if current_idx == len(HAPPY_PATH) - 1:
                continue  # already delivered, shouldn't reach here due to WHERE clause

            next_status = HAPPY_PATH[current_idx + 1]
            timestamp_column = {
                "picked_up": "picked_up_at",
                "in_transit": "in_transit_at",
                "out_for_delivery": "out_for_delivery_at",
                "delivered": "delivered_at",
            }.get(next_status)

            if timestamp_column:
                cur.execute(
                    f"""
                    UPDATE shipments
                    SET status = %s, {timestamp_column} = now(), updated_at = now()
                    WHERE shipment_id = %s
                    """,
                    (next_status, shipment_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE shipments
                    SET status = %s, updated_at = now()
                    WHERE shipment_id = %s
                    """,
                    (next_status, shipment_id),
                )
            advanced += 1

    conn.commit()
    print(
        f"Advance pass: {advanced} progressed, {exceptioned} hit exceptions, "
        f"{resolved} resolved, {returned} returned. ({len(rows)} shipments were in flight)"
    )


def main():
    parser = argparse.ArgumentParser(description="Shipment lifecycle generator")
    parser.add_argument("--seed", action="store_true", help="Seed reference data (one-time)")
    parser.add_argument("--create", type=int, help="Create N new shipments")
    parser.add_argument("--advance", action="store_true", help="Advance in-flight shipments one step")
    parser.add_argument("--loop", action="store_true", help="Continuously create + advance")
    parser.add_argument("--interval", type=int, default=5, help="Seconds between loop iterations")
    parser.add_argument("--new-per-cycle", type=int, default=3, help="New shipments per loop iteration")
    args = parser.parse_args()

    conn = get_connection()
    try:
        if args.seed:
            seed_reference_data(conn)

        if args.create:
            create_shipments(conn, args.create)

        if args.advance:
            advance_shipments(conn)

        if args.loop:
            print(f"Starting loop (interval={args.interval}s, ctrl+C to stop)...")
            while True:
                create_shipments(conn, args.new_per_cycle)
                advance_shipments(conn)
                time.sleep(args.interval)

        if not any([args.seed, args.create, args.advance, args.loop]):
            parser.print_help()
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
