# Communication Layer

Communication is split into two tiers:

1. **Client ↔ Coordinator** — [PostgreSQL wire protocol (v3)](https://www.postgresql.org/docs/current/protocol.html). The coordinator terminates the connection and translates SQL into internal execution paths.
2. **Coordinator ↔ Core Nodes** — gRPC, via:
   - **Ballista** for distributed SELECT execution — the coordinator hosts the Ballista **scheduler**; core nodes host Ballista **executors** that exchange columnar data over **Arrow Flight**.
   - **WriteService** for DML replication.

A third gRPC service (**NodeService**) handles the control plane (registration, heartbeats, failure reporting) with core nodes as clients and the coordinator as server.

## Inter-Node Protocol (Coordinator ↔ Core Nodes)

All coordinator-to-core-node communication uses gRPC:

| Service | gRPC server on | Proto definition | Purpose |
|---------|----------------|------------------|---------|
| **Ballista scheduler** | Coordinator | _(Apache Ballista)_ | Executor registration and task scheduling for distributed SELECT queries. |
| **Arrow Flight** | Core nodes | _(Apache Arrow Flight spec)_ | Columnar data exchange between executors (and back to the coordinator) during distributed SELECT queries. |
| **WriteService** | Core nodes | [`write_service.proto`](../../proto/vairedb/v1/write_service.proto) | Coordinator sends shard-local DML statements (INSERT/UPDATE/DELETE) to core nodes for execution and replication. |
| **NodeService** | Coordinator | [`node_service.proto`](../../proto/vairedb/v1/node_service.proto) | Core node registration, heartbeats, and failure reporting. |

## Wire Format

| Tier | Path | Format | Rationale |
|------|------|--------|-----------|
| **Client ↔ Coordinator** | All client SQL and results | [PostgreSQL wire protocol (v3)](https://www.postgresql.org/docs/current/protocol.html) | Standard protocol — no custom SDK required. See [Client Protocol](#client-protocol) for details. |
| **Coordinator ↔ Core Nodes** | Data exchange (SELECT) | Apache Arrow IPC via Arrow Flight (inherited via Apache Ballista) | Zero-copy friendly, columnar, native to DataFusion and Ballista. |
| **Coordinator ↔ Core Nodes** | Write service (DML) | Protocol Buffers via gRPC | Compact encoding for shard-local SQL statements and quorum acknowledgments. |
| **Coordinator ↔ Core Nodes** | Control plane | Protocol Buffers via gRPC | Node registration, heartbeats, failure reporting. |

## Client Protocol

VaireDB exposes the [PostgreSQL wire protocol (v3)](https://www.postgresql.org/docs/current/protocol.html) as its client-facing interface. Since DuckDB is an embedded, in-process engine with no built-in network protocol, the coordinator node implements the PostgreSQL wire protocol to provide network accessibility to the cluster.

This is a deliberate choice to maximize compatibility with the vast ecosystem of tools, drivers, and libraries that already speak PostgreSQL:

- **CLI tools**: `psql`, `pgcli`
- **JDBC / ODBC**: any application or BI tool that connects via standard PostgreSQL drivers (e.g. DBeaver, Tableau, Metabase)
- **Language drivers**: `libpq` (C), `psycopg` (Python), `node-postgres` (Node.js), `pgx` (Go), `sqlx`/`tokio-postgres` (Rust), and many others
- **ORMs and query builders**: SQLAlchemy, Django ORM, ActiveRecord, JOOQ, Diesel

By implementing the PostgreSQL wire protocol at the coordinator node, clients connect to VaireDB exactly as they would to a standard PostgreSQL instance — no custom SDK or proprietary driver is required. This eliminates the need to develop and maintain client libraries in multiple languages (see [Design Goals — Non Goals](design-goals.md)) and removes adoption friction, since teams can point their existing PostgreSQL-compatible tooling directly at VaireDB.

The coordinator terminates the PostgreSQL wire protocol connection, translates incoming SQL into the internal execution paths (Ballista scheduler for SELECTs, gRPC write service for DML), and returns results formatted as PostgreSQL wire protocol messages.
