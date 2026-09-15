"""
Typed schemas for parsing the raw JSON strings bronze captured (after_json /
before_json) into real columns. These mirror postgres/init.sql exactly.

Timestamps: Debezium is configured with time.precision.mode=connect, so every
temporal column arrives as a plain LongType representing milliseconds since
epoch. Cast with (col / 1000).cast("timestamp") to get a real TimestampType.

Numerics: decimal.handling.mode=double means NUMERIC columns (weight_kg)
arrive as plain DoubleType, not the base64-encoded bytes Debezium uses by
default.
"""

from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

SHIPMENT_SCHEMA = StructType(
    [
        StructField("shipment_id", LongType(), nullable=False),
        StructField("customer_id", LongType(), nullable=False),
        StructField("carrier_id", LongType(), nullable=False),
        StructField("origin_location_id", LongType(), nullable=False),
        StructField("destination_location_id", LongType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("weight_kg", DoubleType(), nullable=False),
        StructField("service_level", StringType(), nullable=False),
        StructField("created_at", LongType(), nullable=True),
        StructField("picked_up_at", LongType(), nullable=True),
        StructField("in_transit_at", LongType(), nullable=True),
        StructField("out_for_delivery_at", LongType(), nullable=True),
        StructField("delivered_at", LongType(), nullable=True),
        StructField("exception_at", LongType(), nullable=True),
        StructField("exception_reason", StringType(), nullable=True),
        StructField("updated_at", LongType(), nullable=True),
    ]
)

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", LongType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("email", StringType(), nullable=False),
        StructField("customer_type", StringType(), nullable=False),
        StructField("created_at", LongType(), nullable=True),
        StructField("updated_at", LongType(), nullable=True),
    ]
)

CARRIER_SCHEMA = StructType(
    [
        StructField("carrier_id", LongType(), nullable=False),
        StructField("carrier_name", StringType(), nullable=False),
        StructField("carrier_type", StringType(), nullable=False),
        StructField("updated_at", LongType(), nullable=True),
    ]
)

LOCATION_SCHEMA = StructType(
    [
        StructField("location_id", LongType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("state", StringType(), nullable=True),
        StructField("country", StringType(), nullable=False),
        StructField("location_type", StringType(), nullable=False),
        StructField("updated_at", LongType(), nullable=True),
    ]
)

# Timestamp columns per entity, so silver can cast them from millis-since-epoch
# longs to real TimestampType in one shared place rather than repeating per script.
TIMESTAMP_COLUMNS = {
    "shipments": [
        "created_at",
        "picked_up_at",
        "in_transit_at",
        "out_for_delivery_at",
        "delivered_at",
        "exception_at",
        "updated_at",
    ],
    "customers": ["created_at", "updated_at"],
    "carriers": ["updated_at"],
    "locations": ["updated_at"],
}