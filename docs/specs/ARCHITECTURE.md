# VaireDB Architecture

> Distributed database built on DuckDB-powered core nodes.

## Index

- [Overview](overview.md) — What VaireDB is and its high-level value proposition
- [Design Goals](design-goals.md) — Goals and non-goals for v0.1
- [System Architecture](system-architecture.md) — High-level topology and node types
  - [High-Level Topology](system-architecture.md#high-level-topology)
  - [Node Types](system-architecture.md#node-types)
- [Core Node (DuckDB)](core-node.md) — Embedded DuckDB engine, storage, and query execution
  - [Embedded DuckDB Engine](core-node.md#embedded-duckdb-engine)
  - [Local Storage Layer](core-node.md#local-storage-layer)
  - [Query Execution](core-node.md#query-execution)
- [Coordinator Node](coordinator-node.md) — Query routing, distributed planning, and metadata catalog
  - [Query Router](coordinator-node.md#query-router)
  - [Distributed Query Planner](coordinator-node.md#distributed-query-planner)
  - [Metadata Catalog](coordinator-node.md#metadata-catalog-database-catalog)
- [Data Distribution](data-distribution.md) — Sharding strategy and replication
  - [Sharding Strategy](data-distribution.md#sharding-strategy)
  - [Replication](data-distribution.md#replication)
- [Cluster Coordination](cluster-coordination.md) — Node discovery, leader election, failure detection
  - [Node Discovery and Membership](cluster-coordination.md#node-discovery-and-membership)
  - [Leader Election](cluster-coordination.md#leader-election)
  - [Failure Detection](cluster-coordination.md#failure-detection)
- [Communication Layer](communication-layer.md) — Protocols, wire formats, client interface
  - [Inter-Node Protocol](communication-layer.md#inter-node-protocol-coordinator--core-nodes)
  - [Wire Format](communication-layer.md#wire-format)
  - [Client Protocol](communication-layer.md#client-protocol)
- [Distributed Query Processing](distributed-query-processing.md) — Query lifecycle and optimization
  - [Query Lifecycle](distributed-query-processing.md#query-lifecycle)
  - [Query Optimization](distributed-query-processing.md#query-optimization-delegated-to-ballista--datafusion)
- [Transactions and Consistency](transactions-consistency.md) — Consistency model and distributed transactions
  - [Consistency Model](transactions-consistency.md#consistency-model)
  - [Distributed Transactions](transactions-consistency.md#distributed-transactions)
- [Fault Tolerance and Recovery](fault-tolerance.md) — WAL, snapshotting, node recovery, quorum
  - [Write-Ahead Log (WAL)](fault-tolerance.md#write-ahead-log-wal)
  - [Snapshotting](fault-tolerance.md#snapshotting-planned)
  - [Node Recovery](fault-tolerance.md#node-recovery)
  - [Quorum and Availability](fault-tolerance.md#quorum-and-availability)
- [Roadmap](roadmap.md) — Roadmap for v0.1 and v0.2
- [Glossary](glossary.md) — Term definitions
- [Links](links.md) — External references
