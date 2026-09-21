# VaireDB

VaireDB is a cloud native, distributed SQL database that combines PostgreSQL 
wire compatibility, with DuckDB's columnar vectorized execution engine for 
high-throughput analytical workloads across a horizontally scalable cluster.

Read the [When use VaireDB](#when-use-vairedb) section for understanding where 
VaireDB is best suited for your use cases.

> [!IMPORTANT]  
> VaireDB is currently under active development. Breaking changes 
> may occur and major features may be added.
> Consider this a work in progress and not yet ready for production use.
> Any contributions are welcome for targeting the production-ready v1.0 release.

## Overview

VaireDB exposes a unified SQL interface through the PostgreSQL wire protocol 
(v3), allowing connections from any standard PostgreSQL client 
(`psql`, JDBC, etc.). Under the hood, data is hash-sharded across core nodes, 
each running an embedded DuckDB instance optimized for OLAP queries.

**Key characteristics:**

- **PostgreSQL-compatible SQL** via DataFusion parser with automatic dialect 
  translation to DuckDB.
- **Horizontal scalability** through hash sharding across core nodes (range 
  sharding planned).
- **Analytical performance** powered by DuckDB's columnar vectorized engine.
- **Fault tolerance** with configurable replication factor and quorum-based writes.
- **Column pseudonymization** for compliance: declared columns are HMAC-SHA256 
  hashed in the coordinator so plaintext never reaches storage.
- **Operational simplicity** as self-contained Rust binaries with YAML configuration.

## When use VaireDB

### Use Cases

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

## Architecture

VaireDB follows a coordinator/worker topology:

```
┌──────────────────────────────────────────────────┐
│                   Clients                         │
│            (psql, JDBC, any PG driver)           │
└──────────────────────┬───────────────────────────┘
                       │ PostgreSQL wire protocol (port 5432)
                       ▼
┌──────────────────────────────────────────────────┐
│               Coordinator Node                    │
│  ┌─────────┐ ┌────────┐ ┌──────────────────┐    │
│  │ Catalog │ │ Router │ │ Ballista Sched.  │    │
│  └─────────┘ └────────┘ └──────────────────┘    │
└──────┬───────────────┬───────────────────────────┘
       │ gRPC writes   │ reads (Ballista)
       ▼               ▼
┌────────────┐  ┌────────────┐  ┌────────────┐
│ Core Node  │  │ Core Node  │  │ Core Node  │
│  (DuckDB)  │  │  (DuckDB)  │  │  (DuckDB)  │
└────────────┘  └────────────┘  └────────────┘
```

- **Coordinator** handles client connections, SQL parsing, query planning, metadata management, and dispatching work to core nodes.
- **Core Nodes** store data in DuckDB, execute shard-local queries, and replicate writes.

For full details, see the [Architecture Documentation](docs/specs/ARCHITECTURE.md).

### Project Documentation

[VaireDB Documentation](https://matteobovetti.github.io/vairedb/)

### System Design Documentation

All documentation lives under `docs/`, split into architecture references, feature designs, and testing notes.

**Architecture** (`docs/specs/`):

| Document | Description |
|----------|-------------|
| [Architecture Index](docs/specs/ARCHITECTURE.md) | Top-level index with references to all architecture sections |
| [Overview](docs/specs/overview.md) | What VaireDB is and its high-level value proposition |
| [Design Goals](docs/specs/design-goals.md) | Goals and non-goals for the current version |
| [System Architecture](docs/specs/system-architecture.md) | High-level topology and node types |
| [Core Node (DuckDB)](docs/specs/core-node.md) | Embedded DuckDB engine, storage, and query execution |
| [Coordinator Node](docs/specs/coordinator-node.md) | Query routing, distributed planning, and metadata catalog |
| [Data Distribution](docs/specs/data-distribution.md) | Sharding strategy and replication |
| [Cluster Coordination](docs/specs/cluster-coordination.md) | Node discovery, leader election, failure detection |
| [Communication Layer](docs/specs/communication-layer.md) | Protocols, wire formats, client interface |
| [Distributed Query Processing](docs/specs/distributed-query-processing.md) | Query lifecycle and optimization |
| [Transactions and Consistency](docs/specs/transactions-consistency.md) | Consistency model and distributed transactions |
| [Fault Tolerance and Recovery](docs/specs/fault-tolerance.md) | WAL, snapshotting, node recovery, quorum |
| [SQL Compatibility Status](docs/specs/sql-compatibility-status.md) | The per-axis status of PostgreSQL compatibility, summarized from the gap census |
| [SQL Gap Analysis](docs/specs/gap-analysis.md) | What a PostgreSQL client cannot fully do against VaireDB today |
| [Roadmap](docs/specs/internal-roadmap.md) | Roadmap for next releases |
| [Glossary](docs/specs/glossary.md) | Term definitions |
| [Links](docs/specs/links.md) | External references |

**Features** (`docs/features/`):

| Document | Description |
|----------|-------------|
| [Compliance](docs/features/compliance/COMPLIANCE.md) | Data pseudonymization, take-out, and deletion for regulated environments |

## Getting Started

### Prerequisites

- **Rust** 1.95 or later (2024 edition)
- **Protobuf compiler** (`protoc`) for gRPC code generation
- **Make** for build automation

### Building

```bash
# Debug build
make build

# Release build
make build-release

# Type-check only (faster feedback loop)
make check
```

### Running Locally - Binary

Start the coordinator:

```bash
make run-coordinator
```

In a separate terminal, start a core node:

```bash
make run-core
```

Connect with any PostgreSQL client:

```bash
psql -h localhost -p 5432
```

### Running Locally - Docker Compose

Start a small VaireDB cluster (1 coordinator + 5 core node):
```bash
make e2e-up
```

Stop the small VaireDB cluster:
```bash
make e2e-down
```

### Configuration

Configuration uses YAML files with environment-based overlays:

```
config/
├── coordinator/
│   └── config.yml   # Default coordinator configurations
└── core/
    └── config.yml   # Default core configurations
```

**Default ports:**

| Port  | Service                                          |
| ----- | ------------------------------------------------ |
| 5432  | PostgreSQL wire protocol (client connections)    |
| 50040 | Coordinator gRPC (node registration, heartbeats) |
| 50041 | Core node gRPC (write dispatch)                  |
| 50050 | Ballista scheduler (distributed query execution) |

## Contributing

### Project Layout

```
vairedb/
├── config/                    # YAML configuration files
├── crates/
│   ├── vairedb-coordinator/   # Coordinator node binary
│   ├── vairedb-core/          # Core node binary
│   └── vairedb-common/        # Shared protobuf code, config, scan plans
├── docker/                    # Dockers file
├── docs/                      # Architecture and testing documentation
├── proto/vairedb/v1/          # Protobuf service definitions
├── tests/e2e/                 # E2E tests
└── Makefile                   # Build automation
```

### Development Workflow

```bash
# Format code
make fmt

# Run linter (clippy, fails on warnings)
make lint

# Run all tests
make test

# Run tests for a single crate
cargo test --package vairedb-coordinator

# Run a specific test
cargo test --package vairedb-coordinator -- test_name

# Run E2E tests
make e2e

# Generate code coverage
make coverage
```

### Key Modules

**Coordinator** (`crates/vairedb-coordinator/src/`):

| Module | Responsibility |
|--------|---------------|
| `anonymization` | Column pseudonymization: HMAC-SHA256 hashing and in-statement rewriting of declared columns before writes leave the coordinator |
| `catalog` | Persistent metadata catalog (tables, shards, nodes, anonymization secrets) exposed as a store and as queryable virtual tables |
| `channel_pool` | Connection pool for gRPC channels to core nodes |
| `column_types` | Read-path mapping from a catalog-declared column type to the Arrow type (and PostgreSQL OID) the coordinator advertises for it |
| `config` | YAML configuration loading |
| `error` | Coordinator error types and their mapping to wire error codes |
| `node_service` | gRPC `NodeService` (register/heartbeat/report) and the heartbeat-based failure detector |
| `pgwire_handler` | PostgreSQL wire-protocol handler: the single SQL parse and read-path AST rewrites, statement classification and table-name extraction, query routing, DDL/DML execution, result encoding, and catalog introspection |
| `replication` | Quorum writes plus a background retry/backoff loop that tails missed writes to lagging replicas |
| `scheduler` | Embedded Ballista scheduler: distributed read planning, plan codecs, and shard-affinity task distribution |
| `write_router` | Resolves shards and dispatches writes to core nodes |
| `write_sql_cl` | Write-path-only compatibility layer: PostgreSQL-dialect rewriting to DuckDB-compatible form plus shard-routing decisions |
| `util` | Cross-cutting helpers (epoch timestamps, shard-local table naming) |

**Core Node** (`crates/vairedb-core/src/`):

| Module | Responsibility |
|--------|---------------|
| `ballista_exec` | Ballista executor for distributed SELECT |
| `config` | YAML configuration loading |
| `engine` | DuckDB instance management |
| `error` | Core node error types |
| `heartbeat` | Registration and periodic heartbeats to coordinator |
| `table_provider` | Custom DataFusion ExecutionPlan that runs shard-local SQL against DuckDB shards |
| `write_queue` | Bounded channel serializing writes to DuckDB |
| `write_service` | gRPC service receiving DML from coordinator, with dedup cache and param conversion |

**Shared** (`crates/vairedb-common/src/`):

Code that must be identical on both sides of the wire. A distributed stage crosses the
wire naming its functions, and the executor resolves those names in its own registry, so
every function whose PostgreSQL semantics differ from the engine default is implemented
once here and registered on every node that plans or executes a read.

| Module | Responsibility |
|--------|---------------|
| `avg_udaf` | PostgreSQL-exact `avg` over integer columns: `numeric` at PostgreSQL's per-value division scale |
| `bytea_in` | `bytea` text input conversion (`'\xDEADBEEF'::bytea` as four bytes, not ten) |
| `config` | YAML configuration loading |
| `error` | Error types, SQLSTATE mapping, message sanitization, and error codes carried across the Ballista scheduler boundary |
| `float_div` | PostgreSQL floating-point division: `22012` for a zero divisor where IEEE 754 answers an infinity |
| `json_agg` | `json_agg` and `jsonb_agg`, including their `ORDER BY` form |
| `json_pg` | `json` / `jsonb` input conversion and the `->`, `->>`, `#>`, `#>>` accessors |
| `not_in` | PostgreSQL three-valued `NOT IN` over a candidate list |
| `nth_value` | `nth_value`, which rejects an offset of zero instead of answering NULL |
| `ntile` | `ntile`, typed `integer` rather than DataFusion's `UInt64` |
| `pg_datetime` | Datetime family: `age`, `make_timestamp`, `make_interval`, `isfinite`, `justify_*`, `clock_timestamp`, `timeofday` |
| `pg_format` | String-building family: `format()`, `quote_literal()`, `quote_nullable()` |
| `pg_format_type` | `format_type(oid, typemod)` with the argument spellings PostgreSQL accepts |
| `pg_typeof` | `pg_typeof()` and the Arrow → PostgreSQL type-name table it needs |
| `pg_udf` | `pg_catalog` scalar functions beyond what `datafusion-pg-catalog` registers |
| `proto` | Protobuf-generated gRPC types, compiled from `proto/vairedb/v1/` by `build.rs` |
| `scan_plan` | Cross-node scan-plan payload |
| `stats_udaf` | Variance and standard deviation family, exact over an exact input |
| `udaf` | Ordered-set aggregates `percentile_cont` and `percentile_disc` |
| `uuid_in` | `uuid` input conversion, accepting PostgreSQL's alternative spellings |
| `within_group` | Remaining `WITHIN GROUP` aggregates: `mode()` and the hypothetical-set family |

### Protobuf Definitions

Service definitions live in `proto/vairedb/v1/`:

- `node_service.proto` -- Register, Heartbeat (bidirectional stream), ReportFailure
- `write_service.proto` -- ExecuteWrite (DML dispatch to core nodes)
- `catalog.proto` -- Metadata messages (TableMeta, ColumnDef, ShardMeta, NodeMeta, AnonymizationSecret)
- `error.proto` -- Shared `VdbErrorCode` enum (query, storage, cluster, catalog, internal codes)

Code is generated automatically by `vairedb-common/build.rs` during `cargo build`.

## License

Apache License 2.0 -- see [LICENSE](LICENSE)
