"""
MCP server exposing the shipment-pipeline gold layer as callable tools.

Uses stdio transport, meant to be launched locally by an MCP client (e.g.
Claude Desktop) via its config -- not a standalone web service. Queries run
through DuckDB directly against gold's Delta tables (see queries.py);
no Spark session involved, so tool calls stay fast.

Run directly for a quick manual check (it'll wait on stdio, Ctrl+C to exit):
    python -m mcp_server.server

To connect from Claude Desktop, add an entry to its config pointing at this
script with this project's venv Python -- see project docs for the exact
config once written.
"""

from typing import Optional

from fastmcp import FastMCP

from mcp_server import queries

mcp = FastMCP("shipment-pipeline-gold")


@mcp.tool()
def get_shipment(shipment_id: int) -> dict:
    """
    Look up one shipment by its ID. Returns the full record with customer,
    carrier, and origin/destination city names resolved (not raw foreign keys).
    Returns an error message if no shipment with that ID exists.
    """
    result = queries.get_shipment(shipment_id)
    if result is None:
        return {"error": f"No shipment found with shipment_id={shipment_id}"}
    return result


@mcp.tool()
def list_shipments_by_status(status: str, limit: int = 50) -> list[dict]:
    """
    List shipments currently in the given lifecycle status.
    Valid statuses: created, picked_up, in_transit, out_for_delivery,
    delivered, exception, returned.
    """
    return queries.list_shipments_by_status(status, limit=limit)


@mcp.tool()
def list_shipments_by_customer(customer_id: int, limit: int = 50) -> list[dict]:
    """List every shipment belonging to one customer, most recent first."""
    return queries.list_shipments_by_customer(customer_id, limit=limit)


@mcp.tool()
def list_exception_shipments(limit: int = 50) -> list[dict]:
    """
    List shipments that have EVER hit an exception event (damaged, lost,
    weather_delay, customs_hold), regardless of current status -- including
    ones that later resolved back to in_transit or were delivered anyway.
    """
    return queries.list_exception_shipments(limit=limit)


@mcp.tool()
def search_shipments(
    status: Optional[str] = None,
    carrier_id: Optional[int] = None,
    service_level: Optional[str] = None,
    has_exception: Optional[bool] = None,
    created_after: Optional[str] = None,
    created_before: Optional[str] = None,
    limit: int = 50,
) -> list[dict]:
    """
    Flexible, multi-filter shipment search. All filters are optional and
    combinable (applied with AND). created_after/created_before accept
    ISO date strings, e.g. "2026-09-01".
    """
    return queries.search_shipments(
        status=status,
        carrier_id=carrier_id,
        service_level=service_level,
        has_exception=has_exception,
        created_after=created_after,
        created_before=created_before,
        limit=limit,
    )


if __name__ == "__main__":
    mcp.run(transport="stdio")