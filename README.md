# Shipment Pipeline

An end-to-end data engineering + AI project: a simulated logistics system feeds a
CDC pipeline (Postgres → Debezium → Kafka), through a medallion lakehouse
(bronze/silver/gold on Spark + Delta), exposed to AI agents via RAG and MCP.

See [`docs/architecture.md`](docs/architecture.md) for the full design and the
reasoning behind key decisions.

## Repo structure

```
shipment-pipeline/
├── docker-compose.yml       # all local infra services
├── postgres/                 # source schema DDL
├── debezium/                 # CDC connector config
├── data_generator/                 # fake shipment lifecycle data generator
├── orchestration/dags/        # Airflow DAGs (next stage)
├── pipeline/                  # PySpark bronze/silver/gold jobs (next stage)
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── common/                 # shared Spark/Delta session config
├── mcp_server/                 # MCP server exposing gold layer (later stage)
├── rag/                        # embeddings/retrieval over gold layer (later stage)
├── docs/architecture.md        # design decisions and diagram
└── tests/
```

## Stage 1: CDC infra + generator (current)

Postgres → Debezium → Kafka, plus the Python generator that drives shipment
lifecycle activity. Everything below gets you this stage running.

## What's in the stack

- **Postgres** (`debezium/postgres:16` image, pre-configured with logical replication)
- **Kafka** — single-node, KRaft mode (no Zookeeper)
- **Kafka Connect + Debezium** — Postgres CDC connector, JSON (no schema registry yet)
- **Kafka UI** — browser inspection of topics/messages

## 1. Configure environment

```bash
cp .env.example .env
```

Defaults work fine as-is for local dev. If you change credentials in `.env`, also
update `debezium/register-postgres-connector.json` to match (it's a static REST
payload, not templated from `.env`).

## 2. Start the stack

```bash
docker compose up -d
```

Give it ~30 seconds for Postgres and Kafka Connect to fully initialize. Check status:

```bash
docker compose ps
```

Postgres will automatically run `postgres/init.sql` on first startup, creating the
`customers`, `carriers`, `locations`, `shipments` tables and the logical replication
publication.

## 3. Register the Debezium connector

Once Kafka Connect is up (check `curl http://localhost:8083/` returns a version),
register the Postgres connector:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @debezium/register-postgres-connector.json \
  http://localhost:8083/connectors
```

Verify it registered and is running:

```bash
curl http://localhost:8083/connectors/shipment-postgres-connector/status
```

You should see `"state": "RUNNING"` for both the connector and its task.

## 4. Generate data

```bash
cd data_generator
pip install -r requirements.txt

# One-time: seed customers, carriers, locations
python generate_shipments.py --seed

# Create some shipments
python generate_shipments.py --create 20

# Progress them one lifecycle step
python generate_shipments.py --advance

# Or run continuously (creates + advances every N seconds)
python generate_shipments.py --loop --interval 5 --new-per-cycle 3
```

## 5. Watch CDC events flow

Open Kafka UI at [http://localhost:8080](http://localhost:8080). You should see topics:

- `shipment.public.customers`
- `shipment.public.carriers`
- `shipment.public.locations`
- `shipment.public.shipments`

Click into `shipment.public.shipments` and watch messages appear as you run
`--create` and `--advance`. Each message has `before` / `after` / `op` fields —
`op: "c"` for the initial create, `op: "u"` for every lifecycle update.

## Troubleshooting

- **Connector fails to register / "publication does not exist"** — Postgres's
  `init.sql` didn't run. Check `docker compose logs postgres` — if the volume
  already existed from a prior run, `init.sql` won't re-run. Fix: `docker compose
  down -v` to wipe the volume and start clean.
- **No messages in Kafka UI** — confirm the connector status is `RUNNING` (step 2),
  and that you've actually run `--create` / `--advance` against Postgres.
- **Kafka Connect can't reach Postgres** — service name in the connector config is
  `postgres`, which only resolves inside the Docker network. Don't change it to
  `localhost`.

## Next steps

- [ ] Airflow DAG to orchestrate the generator loop
- [ ] PySpark structured streaming: Kafka → Bronze (Delta, append-only)
- [ ] Silver: Type 1 cleansing/dedupe per entity
- [ ] Gold: `shipment_fact` (accumulating snapshot) + `customer_dim`, `carrier_dim`,
      `location_dim`, `date_dim`
