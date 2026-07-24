# VaireDB

VaireDB is a cloud native, distributed SQL database that combines PostgreSQL 
wire compatibility, with DuckDB's columnar vectorized execution engine for 
high-throughput analytical workloads across a horizontally scalable cluster.

Read the [When use VaireDB](#when-use-vairedb) section for understanding where 
VaireDB is best suited for your use cases.

> [!IMPORTANT]  
> VaireDB is currently in v0.1 and under active development. Breaking changes 
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

- Analytical/Read optimized database positioned close to microservices' 
  transactional databases, serving the query (read) side of a CQRS architecture.
- A denormalized, read-optimized view of data shared across a microservices ecosystem.
- Deployable both as a wide data fabric spanning the whole ecosystem and as a 
  micro-fabric local to each bounded context.
- A compliance datastore supporting data take-out (export), deletion, and anonymization.

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

For full details, see the [Architecture Documentation](docs/spec/ARCHITECTURE.md).

### Documentation

All documentation lives under `docs/`, split into architecture references, feature designs, and testing notes.

**Architecture** (`docs/spec/`):

| Document | Description |
|----------|-------------|
| [Architecture Index](docs/specs/ARCHITECTURE.md) | Top-level index with references to all architecture sections |
| [Overview](docs/spec/overview.md) | What VaireDB is and its high-level value proposition |
| [Design Goals](docs/spec/design-goals.md) | Goals and non-goals for v0.1 |
| [System Architecture](docs/spec/system-architecture.md) | High-level topology and node types |
| [Core Node](docs/spec/core-node.md) | Embedded DuckDB engine, storage, and query execution |
| [Coordinator Node](docs/spec/coordinator-node.md) | Query routing, distributed planning, and metadata catalog |
| [Data Distribution](docs/spec/data-distribution.md) | Sharding strategy and replication |
| [Cluster Coordination](docs/spec/cluster-coordination.md) | Node discovery, leader election, and failure detection |
| [Communication Layer](docs/spec/communication-layer.md) | Protocols, wire formats, and client interface |
| [Distributed Query Processing](docs/spec/distributed-query-processing.md) | Query lifecycle and optimization |
| [Transactions & Consistency](docs/spec/transactions-consistency.md) | Consistency model and distributed transactions |
| [Fault Tolerance](docs/spec/fault-tolerance.md) | WAL, snapshotting, node recovery, and quorum |
| [Roadmap](docs/spec/roadmap.md) | Roadmap to reach v1.0 |
| [Glossary](docs/spec/glossary.md) | Term definitions |
| [Links](docs/spec/links.md) | External references |

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

### Running Locally

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
| `config` | YAML configuration loading |
| `error` | Coordinator error types and their mapping to wire error codes |
| `node_service` | gRPC `NodeService` (register/heartbeat/report) and the heartbeat-based failure detector |
| `pgwire_handler` | PostgreSQL wire-protocol handler: query routing, DDL/DML execution, result encoding, and catalog introspection |
| `query_router` | SQL statement classification and table-name extraction for routing |
| `replication` | Quorum writes plus a background retry/backoff loop that tails missed writes to lagging replicas |
| `scheduler` | Embedded Ballista scheduler: distributed read planning, plan codecs, and shard-affinity task distribution |
| `sql_compat` | PostgreSQL-dialect rewriting to DuckDB-compatible form plus shard-routing decisions |
| `write_router` | Resolves shards and dispatches writes to core nodes |
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

### Protobuf Definitions

Service definitions live in `proto/vairedb/v1/`:

- `node_service.proto` -- Register, Heartbeat (bidirectional stream), ReportFailure
- `write_service.proto` -- ExecuteWrite (DML dispatch to core nodes)
- `catalog.proto` -- Metadata messages (TableMeta, ColumnDef, ShardMeta, NodeMeta, AnonymizationSecret)
- `error.proto` -- Shared `VdbErrorCode` enum (query, storage, cluster, catalog, internal codes)

Code is generated automatically by `vairedb-common/build.rs` during `cargo build`.

## License

Apache License 2.0 -- see [LICENSE](LICENSE)
