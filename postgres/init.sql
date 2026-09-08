-- =========================================================
-- Shipment pipeline source schema
-- Normalized OLTP-style tables that Debezium will CDC from
-- =========================================================

CREATE TABLE customers (
    customer_id     SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    email           TEXT NOT NULL,
    customer_type   TEXT NOT NULL CHECK (customer_type IN ('individual', 'business')),
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE carriers (
    carrier_id      SERIAL PRIMARY KEY,
    carrier_name    TEXT NOT NULL,
    carrier_type    TEXT NOT NULL CHECK (carrier_type IN ('ground', 'air', 'ocean')),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE locations (
    location_id     SERIAL PRIMARY KEY,
    city            TEXT NOT NULL,
    state           TEXT,
    country         TEXT NOT NULL,
    location_type   TEXT NOT NULL CHECK (location_type IN ('warehouse', 'hub', 'customer_address')),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE shipments (
    shipment_id             SERIAL PRIMARY KEY,
    customer_id             INTEGER NOT NULL REFERENCES customers(customer_id),
    carrier_id              INTEGER NOT NULL REFERENCES carriers(carrier_id),
    origin_location_id      INTEGER NOT NULL REFERENCES locations(location_id),
    destination_location_id INTEGER NOT NULL REFERENCES locations(location_id),
    status                  TEXT NOT NULL CHECK (
        status IN ('created', 'picked_up', 'in_transit', 'out_for_delivery',
                   'delivered', 'exception', 'returned')
    ),
    weight_kg               NUMERIC(10, 2) NOT NULL,
    service_level            TEXT NOT NULL CHECK (service_level IN ('standard', 'express', 'overnight')),
    created_at               TIMESTAMP NOT NULL DEFAULT now(),
    picked_up_at             TIMESTAMP,
    in_transit_at            TIMESTAMP,
    out_for_delivery_at      TIMESTAMP,
    delivered_at             TIMESTAMP,
    exception_at             TIMESTAMP,
    exception_reason         TEXT,
    updated_at                TIMESTAMP NOT NULL DEFAULT now()
);

-- Helpful for the generator to quickly pull "shipments still in progress"
CREATE INDEX idx_shipments_status ON shipments(status);

-- =========================================================
-- Logical replication setup for Debezium
-- =========================================================

-- Each table Debezium should track needs REPLICA IDENTITY FULL
-- so UPDATE/DELETE events include the full "before" row, not just the PK.
ALTER TABLE customers  REPLICA IDENTITY FULL;
ALTER TABLE carriers   REPLICA IDENTITY FULL;
ALTER TABLE locations  REPLICA IDENTITY FULL;
ALTER TABLE shipments  REPLICA IDENTITY FULL;

-- Publication that the Debezium connector will subscribe to
CREATE PUBLICATION shipment_pipeline_pub FOR TABLE customers, carriers, locations, shipments;
