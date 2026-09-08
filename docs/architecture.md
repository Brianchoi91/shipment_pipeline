# Architecture

## Overview

An end-to-end pipeline demonstrating CDC-driven data engineering feeding AI-consumable
data products. A simulated logistics system generates shipment lifecycle events, which
flow through CDC into a medallion (bronze/silver/gold) lakehouse, which is then exposed
to AI agents via two access patterns: RAG (semantic retrieval) and MCP (tool-based
querying).

```
Python generator → Postgres (OLTP) → Debezium (CDC) → Kafka
                                                          │
                                                          ▼
                                        Spark Structured Streaming
                                                          │
                              ┌───────────────┬───────────┴───────────┐
                              ▼               ▼                       ▼
                           Bronze          Silver                   Gold
                        (append-only)   (Type 1, latest)   (star schema: shipment_fact +
                                                             customer/carrier/location/date dims)
                                                                      │
                                                        ┌─────────────┴─────────────┐
                                                        ▼                           ▼
                                                  RAG (embeddings)          MCP server (tool calls)
```

## Key design decisions

### Why Postgres as the source, not raw file drops
Debezium is log-based CDC — it reads a database's write-ahead log, which doesn't exist
for files landing in object storage. Using Postgres as the "OLTP system of record" lets
Debezium do genuine CDC (capturing INSERT/UPDATE/DELETE), which is both more realistic
and a better showcase than file-arrival triggers.

### Why a mutable shipment record, not an append-only event log
Two source patterns were considered:
- **Mutable record** (chosen): one row per shipment, UPDATEd in place as status
  progresses. Debezium captures each transition as a before/after diff — this
  exercises real UPDATE capture, which is the interesting part of CDC.
- **Append-only event log**: a new row per lifecycle event. Simpler, but only
  exercises Debezium's INSERT handling, missing the UPDATE/DELETE story.

This decision also determines the gold fact table shape (see below).

### Why `shipment_fact` is an accumulating snapshot, not a transaction fact
Because the source is a mutable record, gold mirrors that shape: one row per shipment,
with milestone timestamp columns (`picked_up_at`, `delivered_at`, etc.) filled in via
`MERGE INTO` as the shipment progresses. This is the natural counterpart to a mutable
source and matches how shipment status realistically works (a single entity moving
through known stages), rather than reconstructing "events" from state diffs.

### Why normalized source tables (not one wide JSON blob)
`customers`, `carriers`, `locations`, and `shipments` are separate Postgres tables.
This:
- lets Debezium create one topic per table (its default behavior) — clean mapping
  from source table → Kafka topic → bronze table
- avoids conflating entities that change at very different frequencies (a shipment
  updates constantly; a customer's info barely changes)
- keeps SCD logic for dimensions independent of shipment fact logic
- mirrors how a real logistics OLTP system would actually be modeled

### Why `date_dim` isn't CDC-sourced
It's reference data, not an operational entity — generated once via script, not
something that changes based on business events. `shipment_fact` uses a single
`created_date_key` FK to it; the other lifecycle milestones stay as raw timestamps
(better for duration/transit-time calculations than additional date FKs would be).

### Why local-only, JSON (not Avro), no MinIO — for now
All deliberate scope-reduction to get one thing working end-to-end before adding
complexity:
- **Local Docker Compose** instead of cloud Databricks/Kafka: zero cost while
  iterating, avoids solving local-to-cloud connectivity before the pipeline logic
  is even proven.
- **JSON over Avro/Schema Registry**: isolates one variable at a time. Avro +
  registry is a well-scoped follow-up once JSON CDC is flowing correctly — not
  a redesign.
- **Local disk over MinIO**: Delta Lake's storage layer is swappable (`s3a://`
  paths later) without touching transformation logic, so there's no cost to
  deferring this.

## Current status

- [x] Postgres schema + logical replication setup
- [x] Debezium connector config (JSON)
- [x] Shipment generator with lifecycle + exception branching
- [ ] Airflow orchestration of the generator
- [ ] Spark structured streaming: Kafka → Bronze
- [ ] Silver: Type 1 cleansing per entity
- [ ] Gold: accumulating snapshot fact + dimensions
- [ ] RAG layer on gold
- [ ] MCP server on gold

## Deferred / future upgrades

- Avro + Schema Registry (replacing JSON converters)
- MinIO (S3-compatible object storage, replacing local disk paths)
- Deploy to real Databricks + cloud Kafka/Event Hubs
