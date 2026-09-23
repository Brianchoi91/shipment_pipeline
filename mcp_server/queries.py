"""
DuckDB query layer over gold's Delta tables.

Deliberately does NOT import anything from pipeline.common.spark_session --
that module imports PySpark, which brings in the JVM/Java-version/corporate-
network fragility this project fought through at length getting Spark
working on Windows. An MCP tool call needs to be fast and simple; spinning up
a Spark session per call would be the wrong tool for this job. DuckDB reads
Delta tables directly (via its `delta` extension) with no JVM at all.
"""

from pathlib import Path
from typing import Optional

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
GOLD_PATH = REPO_ROOT / "data" / "gold"


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Open a fresh in-memory DuckDB connection with the delta extension loaded.
    Called once per tool invocation -- DuckDB starts fast enough (milliseconds)
    that there's no need to keep a long-lived connection around.
    """
    con = duckdb.connect(database=":memory:")
    con.sql("INSTALL delta; LOAD delta;")
    return con


def _table_path(table_name: str) -> str:
    return str(GOLD_PATH / table_name)


# Shared SELECT/FROM/JOIN block used by every shipment-returning query below.
# Joins location_dim twice (origin and destination) -- the "role-playing
# dimension" pattern, done here at query time rather than as a stored FK.
_SHIPMENT_SELECT = f"""
    SELECT
        f.shipment_id,
        f.customer_id,
        c.name AS customer_name,
        f.carrier_id,
        cr.carrier_name,
        f.origin_location_id,
        ol.city AS origin_city,
        f.destination_location_id,
        dl.city AS destination_city,
        f.status,
        f.weight_kg,
        f.service_level,
        f.has_exception,
        f.created_at,
        f.picked_up_at,
        f.in_transit_at,
        f.out_for_delivery_at,
        f.delivered_at,
        f.exception_at,
        f.exception_reason,
        f.updated_at
    FROM delta_scan('{_table_path("shipment_fact")}') f
    LEFT JOIN delta_scan('{_table_path("customer_dim")}') c ON f.customer_id = c.customer_id
    LEFT JOIN delta_scan('{_table_path("carrier_dim")}') cr ON f.carrier_id = cr.carrier_id
    LEFT JOIN delta_scan('{_table_path("location_dim")}') ol ON f.origin_location_id = ol.location_id
    LEFT JOIN delta_scan('{_table_path("location_dim")}') dl ON f.destination_location_id = dl.location_id
"""


def _run_shipment_query(where_sql: str = "", params: Optional[list] = None, limit: Optional[int] = None) -> list[dict]:
    query = _SHIPMENT_SELECT
    if where_sql:
        query += f"\n{where_sql}"
    query += "\nORDER BY f.created_at DESC"
    if limit:
        query += f"\nLIMIT {int(limit)}"

    con = get_connection()
    try:
        cursor = con.execute(query, params or [])
        columns = [desc[0] for desc in cursor.description]
        # Deliberately NOT using .df().to_dict(...) here: DuckDB's DataFrame
        # conversion returns pandas Timestamp/NaT for datetime columns, and
        # neither serializes cleanly through the MCP layer's JSON encoding
        # (NaT in particular produces a confusing, unrelated-looking
        # TypeError). fetchall() + description gives plain Python types
        # (datetime.datetime, None) instead.
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        con.close()


def get_shipment(shipment_id: int) -> Optional[dict]:
    rows = _run_shipment_query("WHERE f.shipment_id = ?", [shipment_id])
    return rows[0] if rows else None


def list_shipments_by_status(status: str, limit: int = 50) -> list[dict]:
    return _run_shipment_query("WHERE f.status = ?", [status], limit=limit)


def list_shipments_by_customer(customer_id: int, limit: int = 50) -> list[dict]:
    return _run_shipment_query("WHERE f.customer_id = ?", [customer_id], limit=limit)


def list_exception_shipments(limit: int = 50) -> list[dict]:
    return _run_shipment_query("WHERE f.has_exception = true", limit=limit)


def search_shipments(
    status: Optional[str] = None,
    carrier_id: Optional[int] = None,
    service_level: Optional[str] = None,
    has_exception: Optional[bool] = None,
    created_after: Optional[str] = None,
    created_before: Optional[str] = None,
    limit: int = 50,
) -> list[dict]:
    conditions = []
    params: list = []

    if status is not None:
        conditions.append("f.status = ?")
        params.append(status)
    if carrier_id is not None:
        conditions.append("f.carrier_id = ?")
        params.append(carrier_id)
    if service_level is not None:
        conditions.append("f.service_level = ?")
        params.append(service_level)
    if has_exception is not None:
        conditions.append("f.has_exception = ?")
        params.append(has_exception)
    if created_after is not None:
        conditions.append("f.created_at >= ?")
        params.append(created_after)
    if created_before is not None:
        conditions.append("f.created_at <= ?")
        params.append(created_before)

    where_sql = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    return _run_shipment_query(where_sql, params, limit=limit)