# Shipment Pipeline

An end-to-end data engineering project: a simulated logistics system feeds a change-data-capture (CDC) pipeline into a medallion lakehouse (bronze/silver/gold on Spark + Delta Lake), orchestrated with Airflow, with an MCP server exposing the gold layer so an AI agent (Claude) can query it directly.

Everything runs locally — Postgres, Kafka, Spark, and the MCP server all run on a laptop via Docker Compose and a local Python environment. No cloud infrastructure required to try it.

## Architecture

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
                                                             customer/carrier/location dims)
                                                                      │
                                                                      ▼
                                                          MCP server (DuckDB-backed)
                                                                      │
                                                                      ▼
                                                          Claude Desktop (AI agent)

Orchestration: Airflow schedules the generator, driving continuous simulated activity
through the whole pipeline.
```

## What this demonstrates

- **Change data capture**: Debezium reading Postgres's write-ahead log, capturing inserts, updates, and deletes as a real CDC event stream — not batch polling.
- **Medallion architecture**: raw CDC events (bronze) → reconciled current-state entities via Delta Lake `MERGE INTO` (silver, Type 1 slowly-changing dimensions) → a proper dimensional model (gold: an accumulating snapshot fact table plus conformed dimensions).
- **Stream processing**: PySpark Structured Streaming, with config-driven ingestion (adding a new source table is a YAML entry, not new code).
- **Workflow orchestration**: an Airflow DAG driving the simulated data source on a schedule.
- **AI tool integration**: an MCP (Model Context Protocol) server exposing the gold layer as callable tools, connected to Claude Desktop — deliberately built on DuckDB rather than Spark, since a tool call needs to be fast and shouldn't require a JVM session per invocation.

## Tech stack

| Layer | Technology |
|---|---|
| Source data generation | Python, Faker, psycopg2 |
| Source database | PostgreSQL |
| Change data capture | Debezium |
| Streaming | Apache Kafka (KRaft mode, no Zookeeper) |
| Stream processing | PySpark Structured Streaming |
| Storage format | Delta Lake |
| Orchestration | Apache Airflow |
| AI tool query layer | DuckDB (reads Delta tables directly, no JVM) |
| AI integration | MCP (FastMCP), connected to Claude Desktop |
| Local infrastructure | Docker Compose |

## Repo structure

```
shipment-pipeline/
├── docker-compose.yml          # Postgres, Kafka, Kafka Connect, Kafka UI
├── requirements.txt             # single dependency file for the whole project
├── postgres/init.sql            # source schema + logical replication setup
├── debezium/                    # CDC connector configuration
├── data_generator/               # simulates shipment lifecycle activity
├── orchestration/dags/           # Airflow DAG scheduling the generator
├── pipeline/
│   ├── common/                    # shared Spark session config, schemas, merge logic
│   ├── bronze/                    # config-driven Kafka → Delta ingestion
│   ├── silver/                    # config-driven Type 1 reconciliation (MERGE INTO)
│   └── gold/                      # star schema build (batch)
├── mcp_server/                    # MCP server exposing gold as tools (DuckDB-backed)
└── docs/architecture.md           # detailed design decisions and rationale
```

## Data model

**Source (Postgres):** `shipments`, `customers`, `carriers`, `locations` — normalized tables, each with its own CDC topic.

**Gold (star schema):**
- `shipment_fact` — an accumulating snapshot: one row per shipment, milestone timestamps (`created_at`, `picked_up_at`, `delivered_at`, etc.) filled in as the shipment progresses, plus a `has_exception` flag capturing whether the shipment ever hit a problem, even if later resolved
- `customer_dim`, `carrier_dim`, `location_dim` — Type 1 (current-state only) dimensions, resolved via silver's CDC reconciliation

See [`docs/architecture.md`](docs/architecture.md) for the reasoning behind specific design choices (why a mutable source record instead of an append-only event log, why an accumulating snapshot fact, why gold refreshes in batch rather than streaming, why DuckDB instead of Spark for the MCP layer, and others).

## Getting started

```bash
# 1. Clone and set up the environment
git clone <this-repo>
cd shipment-pipeline
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

# 2. Start local infrastructure
docker compose up -d

# 3. Register the Debezium connector
curl -X POST -H "Content-Type: application/json" \
  --data @debezium/register-postgres-connector.json \
  http://localhost:8083/connectors

# 4. Seed and generate activity
cd data_generator
python generate_shipments.py --seed
python generate_shipments.py --create 20
python generate_shipments.py --advance
cd ..

# 5. Run the pipeline (each in its own terminal)
python -m pipeline.bronze.run_bronze_ingestion
python -m pipeline.silver.run_silver_processing

# 6. Build gold (batch — run after bronze/silver have real data)
python -m pipeline.gold.run_gold_batch

# 7. Run the MCP server
python -m mcp_server.server
```

To connect the MCP server to Claude Desktop, add an entry to its config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS) pointing at this project's venv Python and `mcp_server.server` — see [`mcp_server/server.py`](mcp_server/server.py) for the exact command shape.

## Current status

- [x] CDC ingestion: Postgres → Debezium → Kafka
- [x] Bronze: raw, append-only capture, config-driven across all source tables
- [x] Silver: Type 1 reconciliation via Delta `MERGE INTO`, including delete handling
- [x] Gold: star schema (accumulating snapshot fact + three dimensions)
- [x] Orchestration: Airflow DAG driving the data generator
- [x] MCP server: gold exposed as tools, connected to Claude Desktop, verified working
- [ ] RAG: semantic retrieval over gold, planned next

## Notes on scope

This is a personal portfolio project, and some choices reflect deliberately keeping scope manageable rather than gaps in understanding — documented in [`docs/architecture.md`](docs/architecture.md):
- Local-only infrastructure (no cloud deployment) — a deliberate choice to prove the pipeline logic before adding cloud networking complexity
- JSON over Avro/Schema Registry for Kafka messages (a documented, scoped-out enhancement)
- Batch, not streaming, gold refresh — matches how most real analytics consumers actually use a gold layer