# Roadmap

VaireDB is early-stage software.

The tables below reflect the current planned work. Status labels are the project's own.

## Toward v0.1

| Status | Description |
|--------|-------------|
| PLANNED | **Compliance** — define a DAG of connected tables and create a vectorized representation of data takeout or deletion to be performed; plus a set of anonymization algorithms for hashing specified columns. |
| PLANNED | **Data quality** — async data quality checks and metrics defined with a specific SQL instruction (designed on top of the DuckDB SQL dialect). |
| PLANNED | Review & fix E2E test ignored bugs. |

## Toward v0.2

| Status | Description |
|--------|-------------|
| TODO | VaireDB CLI with a massive data-import SQL command and a psql client. |
| TODO | Microbenchmark core pieces of the codebase. |
| TODO | v0.1 performance tests (distributed). |
| TODO | Coordinator WAL. |
| VALIDATE | Metadata / Catalog API. |

## Known limitations (v0.1)

These are tracked as [non-goals](concepts/design-goals.md#non-goals-for-v01) for
the current version:

- **Single coordinator** — a single point of failure for reads and writes.
- **No online resharding** — shard count and key are fixed at table creation.
- **No multi-shard or multi-statement transactions** — single-statement,
  per-shard atomicity only.
- **No automatic failover / shard rebuild** — recovery is reconnect-based;
  permanent failures need manual intervention.
- **No snapshots yet** — durability relies on each node's DuckDB WAL plus
  replication.
- **No built-in security or observability** — TLS, auth, RBAC, metrics, and
  tracing are planned.
