# Core Node

Each core node embeds a **DuckDB** instance as its local query and storage
engine. Distributed read queries (SELECT) run via an
[Apache Ballista](https://github.com/apache/datafusion-ballista) executor;
writes (INSERT/UPDATE/DELETE) arrive as shard-local SQL over the node's gRPC
write service.

## Embedded DuckDB engine

- Each core node runs a single in-process DuckDB instance.
- DuckDB provides columnar storage, vectorized execution, parallel intra-query
  processing, and SQL parsing/optimization (used internally for shard-local
  execution).
- The node wraps DuckDB behind an internal API exposing:
    - **Read query execution** — accept SQL, execute locally, return
      Arrow-compatible results.
    - **Write execution** — accept shard-local INSERT/UPDATE/DELETE SQL from the
      coordinator's write path and execute locally.
    - **Schema management** — create/alter/drop tables for local shards.
    - **Liveness reporting** — registration and periodic health status to the
      coordinator.

!!! note "Single writer, per-node write queue"
    DuckDB supports only a **single concurrent writer** per database file. Each
    core node runs one DuckDB instance hosting all its local shards, so
    concurrent writes to different shards on the same node must be serialized via
    a **per-node write queue**. The queue processes incoming DML sequentially,
    so only one write executes at a time against the local DuckDB instance.

## Local storage layer

| Aspect | Detail |
|--------|--------|
| **Storage format** | DuckDB native columnar format. |
| **Data directory** | Configurable per node; each node owns its local data exclusively. |
| **Persistence** | DuckDB WAL for local durability (periodic checkpointing/snapshots planned — see [Fault Tolerance](fault-tolerance.md#snapshotting-planned)). |

## Query execution

Each core node embeds a Ballista **executor** as an in-process async task that
connects to the Ballista scheduler on the coordinator. The scheduler assigns
query plan fragments to executors, which use
[DataFusion](https://datafusion.apache.org/) as their local execution engine.

To bridge DataFusion with the embedded DuckDB instance, each core node provides
a custom DataFusion `ExecutionPlan` operator. When executed, the operator:

- Translates the plan fragment into a SQL query targeting shard-local DuckDB
  tables (e.g. `table1` becomes `table1_shard0`).
- Executes the translated query against the local DuckDB instance.
- Returns results as Apache Arrow `RecordBatch` streams, which DataFusion and
  Ballista natively consume.

### Read path (SELECT — via Ballista)

1. The Ballista scheduler on the coordinator decomposes the distributed plan
   into fragments and assigns each to the appropriate core node's executor (via
   the custom `DistributionPolicy` enforcing shard-to-executor placement).
2. The executor receives the fragment as a DataFusion physical plan. When the
   plan references a table, the custom `ExecutionPlan` operator runs.
3. The operator generates a SQL query against its shard-local DuckDB table (e.g.
   `orders_shard0`) and executes it against the embedded DuckDB instance.
4. DuckDB returns columnar results, converted into Arrow `RecordBatch` streams.
5. The executor may apply additional DataFusion operators (filters, projections,
   partial aggregations) on top of the Arrow stream if the plan requires it.
6. Results are streamed back to the scheduler on the coordinator via Arrow
   Flight (gRPC).

### Write path (INSERT/UPDATE/DELETE — outside Ballista)

Writes do **not** flow through the Ballista/DataFusion pipeline. The core node
receives shard-local DML (already translated to DuckDB dialect) from the
coordinator via the gRPC WriteService, enqueues it in the per-node write queue,
and executes it sequentially against DuckDB. See
[Data Distribution — Write path](data-distribution.md#replication) for the full
end-to-end flow.

### DDL path (outside Ballista)

DDL also bypasses Ballista. The core node receives shard-local DDL from the
coordinator via the WriteService and executes it against DuckDB (e.g.
`CREATE TABLE orders_shard0 (...)`).
