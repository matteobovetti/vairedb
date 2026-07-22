# Coordinator Node

The coordinator is the cluster's front door and brain. It accepts client SQL,
decides how each statement runs, and tracks all cluster metadata.

## Query Router

The coordinator embeds an [Apache Ballista](https://github.com/apache/datafusion-ballista)
**scheduler** as an in-process component, connected via gRPC to every core
node's Ballista executor. For DML it acts as a client of the gRPC write service
each core node hosts.

- Accepts incoming client SQL statements.
- **SELECT queries** are submitted to the Ballista scheduler for distributed
  execution:
    - The scheduler consults the metadata catalog to resolve which core nodes
      hold the relevant shards.
    - **Single-shard queries**: a plan with a single stage targeting the owning
      core node's executor.
    - **Multi-shard queries**: the distributed query planner decomposes the
      query into a multi-stage execution plan.
- **Write operations** (INSERT/UPDATE/DELETE) bypass Ballista entirely. The
  coordinator parses the statement via DataFusion, resolves target shards,
  translates into DuckDB dialect, and sends shard-local statements to core nodes
  via the WriteService. See [Data Distribution — Write path](data-distribution.md#replication).
- **DDL operations** (CREATE/ALTER/DROP TABLE) also bypass Ballista. The
  coordinator parses the statement, updates the metadata catalog (schemas, shard
  map, replica map), translates the DDL into DuckDB dialect, and broadcasts
  shard-local DDL to all core nodes hosting shards of the affected table.
  `CREATE TABLE` is applied **atomically** — the coordinator waits for all
  target nodes to acknowledge and rolls back on any failure. `ALTER TABLE` and
  `DROP TABLE` are applied **best-effort** — the command fails only if a target
  node is unreachable.

## Distributed Query Planner

The Ballista scheduler leverages
[DataFusion](https://datafusion.apache.org/)'s query planner to produce and
distribute execution plans across the cluster.

- Parses the original SQL via DataFusion into a **logical plan**.
- The logical plan is optimized by DataFusion's built-in rules (predicate
  push-down, projection pruning, constant folding).
- The optimized plan becomes a **physical plan**, partitioned by the scheduler
  into **query stages** — units of work that execute independently on core node
  executors.
- The scheduler inserts **exchange operators** (repartition, broadcast, gather)
  between stages to manage data shuffling via Arrow Flight (gRPC).
- Each stage is assigned to one or more core node executors based on data
  locality (the shard map) and executor availability. Because Ballista's default
  scheduling policy does not support hard placement constraints, VaireDB
  implements a custom `DistributionPolicy` that pins each stage to the executor
  hosting the required shard.

## Metadata Catalog

The coordinator maintains a cluster-wide catalog:

| Metadata | Description |
|----------|-------------|
| **Table schemas** | Column definitions, types, constraints. |
| **Shard map** | Which shards live on which core nodes, plus shard assignment metadata (hash bucket for hash-sharded tables; key-range assignment reserved for range sharding). |
| **Replica map** | Where replicas of each shard reside. |
| **Node registry** | Live set of core nodes and their status. |

**Storage backend.** The catalog is stored in a local embedded KV store on the
single coordinator node. A future version may introduce a distributed KV store
with Raft consensus (similar to [tikv](https://tikv.org) or
[etcd](https://etcd.io/)) to support multi-coordinator HA (see the
[Roadmap](../roadmap.md)).

!!! tip "Inspect the catalog with SQL"
    The catalog is queryable as virtual tables under the `vairedb_catalog`
    schema — for example `SELECT * FROM vairedb_catalog.tables;`. See the
    [SQL Guide](../sql/tables.md#inspecting-the-catalog).
