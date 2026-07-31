# Changelog

All notable changes to VaireDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-31

First public release of **VaireDB**, a cloud-native, distributed SQL database that
combines **PostgreSQL wire compatibility** with **DuckDB's columnar vectorized
execution engine** for high-throughput analytical workloads across a horizontally
scalable cluster.

> **Important:** v0.1 is an early, under-active-development release. Breaking changes
> may occur and major features may still be added. It is **not yet ready for
> production use** — contributions toward the production-ready v1.0 are welcome.

### Highlights

- **PostgreSQL wire protocol (v3)** — connect from any standard PostgreSQL client
  (`psql`, JDBC, any PG driver) on port `5432`.
- **PostgreSQL-compatible SQL** — parsed via DataFusion with automatic dialect
  translation to DuckDB, including `pg_catalog` emulation for client compatibility.
- **Horizontal scalability** — data is hash-sharded across core nodes, each running
  an embedded DuckDB instance tuned for OLAP.
- **Analytical performance** — DuckDB's columnar, vectorized engine, with distributed
  reads planned and executed through an embedded Ballista scheduler.
- **Fault tolerance** — configurable replication factor with quorum-based writes and a
  background retry/backoff loop that tails missed writes to lagging replicas.
- **Column pseudonymization (compliance)** — declared columns are HMAC-SHA256 hashed
  in the coordinator so plaintext never reaches storage; secret keys stay in the
  coordinator catalog.
- **Operational simplicity** — self-contained Rust binaries (Rust 1.95, 2024 edition)
  configured with YAML.

### Added

- Coordinator and core node binaries with gRPC-based cluster coordination
  (registration, heartbeat-based failure detection).
- SQL DDL/DML/SELECT support with PostgreSQL → DuckDB dialect rewriting.
- Hash sharding with configurable `shards`, `replication_factor`, and `shard_by`.
- Quorum writes with a background replica-catch-up retry loop.
- Column pseudonymization via `anonymized_columns` in `CREATE TABLE` (HMAC-SHA256
  computed in the coordinator).
- Persistent metadata catalog (redb) exposed as queryable virtual tables.
- Embedded Ballista scheduler for distributed read planning with shard affinity.
- Full documentation set (architecture specs, compliance feature design) and a
  published docs site.
- End-to-end test suite (`make e2e`).

### Architecture

A coordinator/worker topology:

- **Coordinator node** — client connections, SQL parsing, query routing and
  distributed planning, metadata catalog, write dispatch, replication, and
  anonymization.
- **Core nodes** — embedded DuckDB storage, shard-local query execution, and write
  replication.

### Known limitations (non-goals for v0.1)

- Single coordinator (no HA — known SPOF).
- No online resharding; the sharding strategy is fixed at table creation/load time.
- Manual failover and recovery.
- No built-in authentication, authorization, or encryption.
- No metrics/tracing endpoints.
- Only the 1-coordinator + N-core-nodes topology is supported.

See [Design Goals](docs/specs/design-goals.md) for the full list of goals and non-goals.

### Roadmap to v0.2

- Compliance data take-out and deletion (DAG-based).
- Extended DuckDB SQL support.
- VaireDB CLI with bulk-import SQL command and psql client.
- Micro-benchmarks and distributed performance tests.

See the full [Roadmap](docs/specs/roadmap.md).

[0.1.0]: https://github.com/matteobovetti/vairedb/releases/tag/v0.1.0
