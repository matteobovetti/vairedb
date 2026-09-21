# VaireDB Architecture

> Distributed database built on DuckDB-powered core nodes.

## Index

- [Overview](overview.md) — What VaireDB is and its high-level value proposition
- [Design Goals](design-goals.md) — Goals and non-goals for the current version
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
  - [Query Optimization](distributed-query-processing.md#query-optimization)
- [Transactions and Consistency](transactions-consistency.md) — Consistency model and distributed transactions
  - [Consistency Model](transactions-consistency.md#consistency-model)
  - [Distributed Transactions](transactions-consistency.md#distributed-transactions)
- [Fault Tolerance and Recovery](fault-tolerance.md) — WAL, snapshotting, node recovery, quorum
  - [Write-Ahead Log (WAL)](fault-tolerance.md#write-ahead-log-wal)
  - [Snapshotting](fault-tolerance.md#snapshotting-planned)
  - [Node Recovery](fault-tolerance.md#node-recovery)
  - [Quorum and Availability](fault-tolerance.md#quorum-and-availability)
- [SQL Compatibility Status](sql-compatibility-status.md) — The per-axis status of PostgreSQL compatibility, summarized from the gap census
  - [Totals](sql-compatibility-status.md#totals)
  - [Status by axis](sql-compatibility-status.md#status-by-axis)
  - [Two caveats that qualify every count](sql-compatibility-status.md#two-caveats-that-qualify-every-count-above)
- [SQL Gap Analysis](gap-analysis.md) — What a PostgreSQL client cannot fully do against VaireDB today
  - [The gap table](gap-analysis.md#2--the-gap-table)
  - [Not implemented by decision](gap-analysis.md#3--not-implemented-by-decision-)
  - [Supersets that must NOT be "fixed"](gap-analysis.md#4--supersets-that-must-not-be-fixed)
  - [Executable counterpart](gap-analysis.md#5--executable-counterpart)
- [Roadmap](intenal-roadmap.md) — Roadmap for next releases
- [Glossary](glossary.md) — Term definitions
- [Links](links.md) — External references
