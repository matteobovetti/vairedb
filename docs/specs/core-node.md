# Core Node (DuckDB)

Each core node embeds a DuckDB instance as its local query and storage engine. Distributed read queries (SELECT) are executed via the [Apache Ballista](https://github.com/apache/datafusion-ballista) executor, which provides a distributed compute layer on top of Apache Arrow and DataFusion. Write operations (INSERT/UPDATE/DELETE) bypass Ballista and are received as shard-local SQL through the node's own gRPC write service, which the coordinator calls.

## Embedded DuckDB Engine

- Each core node runs a single DuckDB in-process instance.
- DuckDB provides: columnar storage, vectorized execution, parallel intra-query processing, SQL parsing and optimization (used internally for shard-local query execution).
- The node wraps DuckDB behind an internal API that exposes:
  - Read query execution (accept SQL queries from the TableProvider, execute locally, return Arrow-compatible results)
  - Write execution (accept shard-local INSERT/UPDATE/DELETE SQL statements from the coordinator's write path, execute locally)
  - Schema management (create/alter/drop tables for local shards)
  - Liveness reporting (registration and periodic health status to the coordinator)

DuckDB only supports a single concurrent writer per database file. Each core node runs a single DuckDB instance that hosts all its local shards. Since concurrent writes targeting different shards on the same node share the same DuckDB instance, they must be serialized via a per-node write queue. The write queue processes incoming DML statements sequentially, ensuring only one write executes at a time against the local DuckDB instance.

## Local Storage Layer

| Aspect | Detail |
|--------|--------|
| **Storage format** | DuckDB native columnar format. |
| **Data directory** | Configurable per-node; each node owns its local data exclusively. |
| **Persistence** | DuckDB WAL for local durability (periodic checkpointing/snapshots planned, see [Fault Tolerance](fault-tolerance.md#snapshotting-planned)). |

## Query Execution

Each core node embeds an [Apache Ballista](https://github.com/apache/datafusion-ballista) executor as an in-process component (async task within the same binary) that connects to the Ballista scheduler living on the coordinator node. The Ballista scheduler assigns query plan fragments to executors, which in turn leverage [DataFusion](https://datafusion.apache.org/) as their local execution engine.

To bridge DataFusion with the embedded DuckDB instance, each core node provides a custom DataFusion [`ExecutionPlan`](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html) operator. The Ballista scheduler serializes this operator into the query plan fragments it ships to executors. When executed on a core node, the operator:

- Translates the plan fragment into a SQL query targeting shard-local DuckDB tables (e.g. `table1` becomes `table1_shard0`).
- Executes the translated query against the local DuckDB instance.
- Returns results as Apache Arrow `RecordBatch` streams, which DataFusion and Ballista natively consume.

**Read path (SELECT — via Ballista):**

1. The Ballista scheduler on the coordinator decomposes the distributed query plan into fragments and assigns each fragment to the appropriate core node's Ballista executor (via the custom `DistributionPolicy` that enforces shard-to-executor placement).
2. The executor receives the fragment as a DataFusion physical plan. When the plan references a table, the custom `ExecutionPlan` operator executes.
3. The custom `ExecutionPlan` generates a SQL query against its shard-local DuckDB table (e.g. `orders_shard0`) and executes it against the embedded DuckDB instance.
4. DuckDB returns columnar results, which the operator converts into Arrow `RecordBatch` streams.
5. The executor may apply additional DataFusion operators (filters, projections, partial aggregations) on top of the Arrow stream if required by the plan.
6. Final or intermediate results are streamed back to the Ballista scheduler on the coordinator via Arrow Flight (gRPC).

**Write path (INSERT/UPDATE/DELETE — outside Ballista):**

Write operations do **not** flow through the Ballista/DataFusion pipeline. From the core node's perspective: it receives shard-local DML (already translated into DuckDB dialect) from the coordinator via gRPC WriteService, enqueues it in the per-node write queue, and executes it sequentially against the local DuckDB instance. See [Replication — Write path](data-distribution.md#replication) for the full end-to-end flow including parsing, dialect translation, quorum protocol, and tail replication.

**DDL path (CREATE TABLE, ALTER TABLE, DROP TABLE — outside Ballista):**

DDL operations also bypass Ballista. From the core node's perspective: it receives shard-local DDL (already translated into DuckDB dialect) from the coordinator via gRPC WriteService and executes it against the local DuckDB instance (e.g. `CREATE TABLE orders_shard0 (...)`). See [Query Router](coordinator-node.md#query-router) for the coordinator-side DDL flow.
