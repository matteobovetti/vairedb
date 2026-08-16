# VaireDB

> A distributed SQL database that combines **PostgreSQL wire compatibility** with **DuckDB's columnar vectorized execution engine** for high-throughput analytical workloads across a horizontally scalable cluster.

VaireDB exposes a unified SQL interface through the PostgreSQL wire protocol (v3),
allowing connections from any standard PostgreSQL client (`psql`, JDBC, etc.).
Under the hood, data is hash-sharded across core nodes, each running an embedded
DuckDB instance optimized for OLAP queries.

[Get started in 5 minutes :material-rocket-launch:](getting-started/quickstart.md){ .md-button .md-button--primary }
[Read the concepts :material-book-open-variant:](concepts/index.md){ .md-button }

## Why VaireDB

<div class="grid cards" markdown>

-   :material-elephant: __PostgreSQL-compatible__

    ---

    Speak the PostgreSQL wire protocol (v3). Connect with `psql`, JDBC/ODBC,
    `psycopg`, `pgx`, or any BI tool — no proprietary driver required.

-   :material-arrow-expand-horizontal: __Horizontally scalable__

    ---

    Data is hash-sharded across core nodes. Add core nodes to scale read and
    write throughput. (Range sharding planned.)

-   :material-lightning-bolt: __Analytical performance__

    ---

    Powered by DuckDB's columnar, vectorized engine, with distributed query
    execution via Apache Ballista and DataFusion.

-   :material-shield-check: __Fault tolerant__

    ---

    Configurable replication factor with synchronous quorum writes and
    asynchronous tail replication to lagging replicas.

</div>

## Key characteristics

- **PostgreSQL-compatible SQL** via DataFusion parser with automatic dialect 
  translation to DuckDB.
- **Horizontal scalability** through hash sharding across core nodes (range 
  sharding planned).
- **Analytical performance** powered by DuckDB's columnar vectorized engine.
- **Fault tolerance** with configurable replication factor and quorum-based writes.
- **Column pseudonymization** for compliance: declared columns are HMAC-SHA256 
  hashed in the coordinator so plaintext never reaches storage.
- **Operational simplicity** as self-contained Rust binaries with YAML configuration.

## When to use VaireDB

VaireDB is designed to sit alongside your microservices' transactional databases
and serve analytical, read-optimized workloads. Common use cases:

- **CQRS read side** — an analytical database positioned close to
  microservices' transactional databases, serving the query (read) side of a
  CQRS architecture. Transactional systems keep handling writes in their own
  stores, while VaireDB absorbs the heavy read and aggregation traffic that
  would otherwise contend with operational workloads — keeping write paths fast
  and read paths scalable.

- **Shared read model** — a denormalized, read-optimized view of data shared
  across a microservices ecosystem. Instead of each service repeatedly joining
  and reshaping data from many sources, VaireDB holds a consolidated,
  query-friendly representation that teams can reuse, reducing duplicated effort
  and keeping cross-service reporting consistent.

- **Data fabric or micro-fabric** — deployable both as a wide data fabric
  spanning the whole ecosystem and as a micro-fabric local to each bounded
  context. The same engine scales from a single bounded context to an
  organization-wide layer, so you can start small within one domain and grow
  toward a shared analytical backbone without changing technology.

- **Compliance datastore** — supporting data take-out (export), deletion, and
  anonymization. By centralizing a queryable copy of data, VaireDB makes it
  easier to satisfy regulatory obligations such as subject-access exports,
  right-to-be-forgotten deletions, and anonymization, without hunting through
  every individual service store.

## How it fits together

```
        Clients (psql, JDBC, any PG driver)
                      │  PostgreSQL wire protocol (v3, port 5432)
                      ▼
              Coordinator Node
        (catalog · router · Ballista scheduler)
            │ gRPC writes        │ reads (Ballista)
            ▼                    ▼
   ┌────────────┐  ┌────────────┐  ┌────────────┐
   │ Core Node  │  │ Core Node  │  │ Core Node  │
   │  (DuckDB)  │  │  (DuckDB)  │  │  (DuckDB)  │
   └────────────┘  └────────────┘  └────────────┘
```

- The **Coordinator** accepts client connections, parses and plans SQL, manages
  cluster metadata, and dispatches work to core nodes.
- **Core Nodes** store data shards in DuckDB, execute shard-local queries, and
  replicate writes.

See [System Architecture](concepts/system-architecture.md) for the full picture.

!!! warning "Project status"
    VaireDB is an early-stage (v0.1) project. The single coordinator is a known
    single point of failure, and several features (snapshots, range sharding,
    automatic failover, security, observability) are planned but not yet
    implemented. See the [Roadmap](roadmap.md) for details.

## Next steps

<div class="grid cards" markdown>

-   [:material-rocket-launch: __Quick Start__](getting-started/quickstart.md)

    Spin up a 1-coordinator + 5-core cluster with Docker Compose and run your
    first queries.

-   [:material-book-open-variant: __Concepts__](concepts/index.md)

    Understand the architecture: sharding, replication, query processing, and
    consistency.

-   [:material-database-search: __SQL Guide__](sql/index.md)

    Create tables, insert data, and query VaireDB with standard SQL.

-   [:material-cog: __Configuration__](getting-started/configuration.md)

    Every coordinator and core node setting, explained.

</div>
