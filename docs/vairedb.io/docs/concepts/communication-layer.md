# Communication Layer

Communication is split into two tiers:

1. **Client ↔ Coordinator** — the
   [PostgreSQL wire protocol (v3)](https://www.postgresql.org/docs/current/protocol.html).
   The coordinator terminates the connection and translates SQL into internal
   execution paths.
2. **Coordinator ↔ Core Nodes** — gRPC, via:
    - **Ballista** for distributed SELECT execution — the coordinator hosts the
      Ballista **scheduler**; core nodes host **executors** that exchange
      columnar data over **Arrow Flight**.
    - **WriteService** for DML replication.

A third gRPC service, **NodeService**, handles the control plane (registration,
heartbeats, failure reporting) with core nodes as clients and the coordinator as
server.

## Inter-node protocol (Coordinator ↔ Core Nodes)

| Service | gRPC server on | Purpose |
|---------|----------------|---------|
| **Ballista scheduler** | Coordinator | Executor registration and task scheduling for distributed SELECT queries. |
| **Arrow Flight** | Core nodes | Columnar data exchange between executors (and back to the coordinator) during distributed SELECT queries. |
| **WriteService** | Core nodes | Coordinator sends shard-local DML (INSERT/UPDATE/DELETE) to core nodes for execution and replication. |
| **NodeService** | Coordinator | Core node registration, heartbeats, and failure reporting. |

## Wire format

| Tier | Path | Format | Rationale |
|------|------|--------|-----------|
| **Client ↔ Coordinator** | All client SQL and results | PostgreSQL wire protocol (v3) | Standard protocol — no custom SDK required. |
| **Coordinator ↔ Core Nodes** | Data exchange (SELECT) | Apache Arrow IPC via Arrow Flight (inherited from Ballista) | Zero-copy friendly, columnar, native to DataFusion and Ballista. |
| **Coordinator ↔ Core Nodes** | Write service (DML) | Protocol Buffers via gRPC | Compact encoding for shard-local SQL statements and quorum acks. |
| **Coordinator ↔ Core Nodes** | Control plane | Protocol Buffers via gRPC | Node registration, heartbeats, failure reporting. |

## Client protocol

VaireDB exposes the
[PostgreSQL wire protocol (v3)](https://www.postgresql.org/docs/current/protocol.html)
as its client-facing interface. Since DuckDB is an embedded, in-process engine
with no built-in network protocol, the coordinator implements the PostgreSQL
wire protocol to provide network access to the cluster.

This is a deliberate choice to maximize compatibility with the PostgreSQL
ecosystem:

- **CLI tools**: `psql`, `pgcli`
- **JDBC / ODBC**: DBeaver, Tableau, Metabase, and any tool using standard
  PostgreSQL drivers
- **Language drivers**: `libpq` (C), `psycopg` (Python), `node-postgres`
  (Node.js), `pgx` (Go), `sqlx`/`tokio-postgres` (Rust), and many others
- **ORMs and query builders**: SQLAlchemy, Django ORM, ActiveRecord, JOOQ,
  Diesel

Clients connect to VaireDB exactly as they would to a standard PostgreSQL
instance — no custom SDK or proprietary driver is required. The coordinator
terminates the connection, translates incoming SQL into the internal execution
paths (Ballista scheduler for SELECTs, gRPC write service for DML), and returns
results formatted as PostgreSQL wire protocol messages.
