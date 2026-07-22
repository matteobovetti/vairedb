# Coordinator Node

## Query Router

The coordinator node embeds an [Apache Ballista](https://github.com/apache/datafusion-ballista) scheduler as an in-process component (async task within the same binary) that is connected via gRPC to every core node's Ballista executor. For DML operations the coordinator acts as a client of the gRPC write service that each core node hosts.

- Accepts incoming client SQL statements.
- **SELECT queries** are submitted to the Ballista scheduler for distributed execution:
  - The scheduler consults the metadata catalog to resolve which core nodes hold the relevant shards.
  - For **single-shard queries**: the scheduler creates a plan with a single stage targeting the owning core node's executor.
  - For **multi-shard queries**: the scheduler invokes the distributed query planner to decompose the query into a multi-stage execution plan.
- **Write operations** (INSERT/UPDATE/DELETE) bypass Ballista entirely. The coordinator parses the statement via DataFusion, resolves target shards, translates into DuckDB dialect, and sends shard-local statements to core nodes via WriteService. See [Replication — Write path](data-distribution.md#replication) for the full flow.
- **DDL operations** (CREATE TABLE, ALTER TABLE, DROP TABLE) bypass Ballista entirely. The coordinator parses the statement via DataFusion, updates the metadata catalog (table schemas, shard map, replica map), translates the DDL into DuckDB dialect, and broadcasts shard-local DDL to all core nodes hosting shards of the affected table via WriteService. CREATE TABLE is applied atomically — the coordinator waits for all target nodes to acknowledge and rolls back on any failure. ALTER TABLE and DROP TABLE are applied best-effort — the command fails only if a target node is unreachable.

## Distributed Query Planner

The Ballista scheduler leverages [DataFusion](https://datafusion.apache.org/)'s query planner to produce and distribute execution plans across the cluster.

- Parses the original SQL via DataFusion and produces a **logical plan**.
- The logical plan is optimized by DataFusion's built-in optimizer rules (predicate push-down, projection pruning, constant folding).
- The optimized logical plan is converted into a **physical plan**, which the Ballista scheduler partitions into **query stages** — units of work that can execute independently on core node executors.
- The scheduler inserts **exchange operators** (repartition, broadcast, gather) between stages to manage data shuffling across executors via Arrow Flight (gRPC).
- Applies push-down optimizations (filters, projections, partial aggregations) to minimize data movement between core nodes.
- Each stage is assigned to one or more core node executors based on data locality (shard map) and executor availability. Ballista's default scheduling policy does not support hard placement constraints, so VaireDB implements a custom `DistributionPolicy` that integrates with the metadata catalog to pin stages to the executor hosting the required shard. The executor then runs the custom `ExecutionPlan` operator that bridges DataFusion to the local DuckDB instance (see [Query Execution](core-node.md#query-execution)).

## Metadata Catalog (Database catalog)

The coordinator maintains a cluster-wide catalog:

| Metadata | Description |
|----------|-------------|
| **Table schemas** | Column definitions, types, constraints. |
| **Shard map** | Which shards live on which core nodes, and the shard assignment metadata (hash bucket for hash-sharded tables; key-range assignment is reserved for range sharding, which is planned). |
| **Replica map** | Where replicas of each shard reside. |
| **Node registry** | Live set of core nodes and their status. |

**Storage backend**

The metadata catalog is stored in a local embedded KV store on the single coordinator node. A future version may introduce a distributed KV store with Raft consensus (similar to [tikv](https://tikv.org) or [etcd](https://etcd.io/)) to support multi-coordinator HA (see [Roadmap](roadmap.md)).
