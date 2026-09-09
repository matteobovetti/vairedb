# Query Processing

Every statement a client sends arrives over the PostgreSQL wire protocol and is
dispatched by the coordinator to one of three paths: **read** (SELECT, via
Ballista), **write** (DML, via the gRPC WriteService), or **DDL**.

## Read path (SELECT — via Ballista)

```
Client SQL (SELECT)
    │  PostgreSQL wire protocol (v3)
    v
[Parse & Analyze]            (Coordinator: DataFusion parser)
    │
    v
[Logical Plan]               (Coordinator: DataFusion optimizer)
    │
    v
[Physical Plan]              (Coordinator: Ballista scheduler)
    │
    v
[Stage Decomposition]        (Coordinator: split into query stages)
    │  Ballista scheduler gRPC (executors pull task assignments)
    v
[Stage Assignment]           (Core Node Ballista executors run assigned stages)
    │
    v
[Shard-Local Scan -> DuckDB] (Core Node: execute shard-local SQL on DuckDB)
    │
    v
[Arrow RecordBatch Stream]   (Core Node -> Coordinator via Arrow Flight)
    │
    v
[Merge / Final Agg]          (Coordinator: DataFusion operators)
    │  PostgreSQL wire protocol (v3)
    v
[Return to Client]
```

## Write path (INSERT/UPDATE/DELETE — via gRPC WriteService)

```
Client SQL (DML)
    │  PostgreSQL wire protocol (v3)
    v
[Parse & Identify Shards]        (Coordinator: DataFusion parser)
    │
    v
[Rewrite to Shard-Local SQL]     (Coordinator: e.g. orders -> orders_shard0)
    │
    v
[Translate to DuckDB Dialect]    (Coordinator: compatibility layer)
    │  gRPC WriteService
    v
[Send to Primary + Replicas]     (Coordinator -> Core Nodes)
    │
    v
[Per-Node Write Queue]           (Each Core Node: serialize concurrent writes)
    │
    v
[Execute on DuckDB]              (Each Core Node)
    │  gRPC WriteService
    v
[Quorum Acknowledgment]          (Coordinator waits for floor(N/2)+1 acks)
    │  PostgreSQL wire protocol (v3)
    v
[Return to Client]
```

See [Data Distribution — Write path](data-distribution.md#replication) for the
quorum and tail-replication details.

A write whose rows do not appear in the statement itself — `INSERT ... SELECT`,
`CREATE TABLE ... AS SELECT`, `COPY ... FROM` — cannot be sharded as written, because
the coordinator decides placement from the shard key's value. Those forms run their
source first (the read path above, or a CSV file on the coordinator) and re-enter this
write path as ordinary rows, so placement and replication work exactly as they do for
a hand-written `INSERT`.

## DDL path

DDL (CREATE/ALTER/DROP TABLE) also bypasses Ballista. The coordinator parses the
statement, updates the metadata catalog (schemas, shard map, replica map),
translates the DDL into DuckDB dialect, and broadcasts shard-local DDL to all
core nodes hosting shards of the affected table:

- `CREATE TABLE` is applied **atomically** — the coordinator waits for all
  target nodes to acknowledge and rolls back on any failure.
- `ALTER TABLE` and `DROP TABLE` are applied **best-effort** — the command fails
  only if a target node is unreachable.

## Query optimization

Join strategies (co-located, broadcast, shuffle), two-phase aggregation, and the
logical rewrites that come before them — constant folding, projection pruning,
moving a predicate down the plan — come from
[Apache Ballista](https://github.com/apache/datafusion-ballista) and
[DataFusion](https://datafusion.apache.org/). VaireDB does not reimplement any of
that.

The one piece VaireDB owns is the last hop: what the shard's own `SELECT` contains.
A shard runs DuckDB, not the engine that planned the query, so the coordinator
decides predicate by predicate whether both engines would read it the same way, and
sends only those to the shard. Comparisons, `AND`/`OR`/`NOT`, `IS NULL`, `IN`,
`BETWEEN` and `LIKE` over columns and literals qualify; function calls, casts and
arithmetic stay at the coordinator. Two shapes are held back on purpose: an ordering
comparison (`<`, `>`, `BETWEEN`) on a text column, and any predicate at all on a
column whose type VaireDB reports as `text` without the shard storing it as text —
`uuid` and `json` are the everyday examples. A `LIMIT` with no filter under it
travels too, as a per-shard cap.

Both are bandwidth optimizations only. The coordinator keeps its own copy of every
pushed predicate and applies the query's real `LIMIT` over the union of the shards'
answers, so a shard that returns more rows than asked costs network traffic and
nothing else.
