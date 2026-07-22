<!-- Same of Overview inside README.md -->
# Overview

A distributed SQL database that combines PostgreSQL wire compatibility with DuckDB's columnar vectorized execution engine for high-throughput analytical workloads across a horizontally scalable cluster.

VaireDB exposes a unified SQL interface through the PostgreSQL wire protocol (v3), allowing connections from any standard PostgreSQL client (`psql`, JDBC, etc.). Under the hood, data is hash-sharded across core nodes, each running an embedded DuckDB instance optimized for OLAP queries.

**Key characteristics:**

- **PostgreSQL-compatible SQL** via DataFusion parser with automatic dialect translation to DuckDB
- **Horizontal scalability** through hash sharding across core nodes (range sharding planned)
- **Analytical performance** powered by DuckDB's columnar vectorized engine
- **Fault tolerance** with configurable replication factor and quorum-based writes
- **Operational simplicity** as self-contained Rust binaries with YAML configuration
