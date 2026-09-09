# Distributed Query Processing

## Query Lifecycle

### Read path (SELECT — via Ballista)

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
[Merge / Final Agg]         (Coordinator: DataFusion operators)
    │  PostgreSQL wire protocol (v3)
    v
[Return to Client]
```

### Write path (INSERT/UPDATE/DELETE — via gRPC WriteService)

```
Client SQL (DML)
    │  PostgreSQL wire protocol (v3)
    v
[Parse & Identify Shards]        (Coordinator: PostgreSQL-compatible parser)
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

Shard identification is a gate, not a guess: a write whose target shards cannot be
determined is rejected before anything is sent, rather than routed somewhere plausible.
See [SQL command gap analysis](gap-analysis-command.md) for which forms that rules out.

## Query Optimization

Join strategies (co-located, broadcast, shuffle), two-phase aggregation, and the
logical rewrites that precede them — constant folding, projection pruning, moving a
predicate down the plan — come from [Apache Ballista](https://github.com/apache/datafusion-ballista)
and [DataFusion](https://datafusion.apache.org/). VaireDB does not reimplement any of
that.

What VaireDB does own is the last hop: the predicate and row limit that reach the
shard's own `SELECT`. DataFusion can only offer a filter to a table provider that
declares it can evaluate one, and the shard runs a different engine than the one that
planned the query — so the coordinator decides, per predicate, whether both engines
would agree on its meaning, and renders only those into the shard statement. The
coordinator keeps its own copy of every pushed predicate and applies the query's real
limit over the union of the shards' answers, so push-down changes how much data crosses
the network and never what the answer is.

That guarantee holds in one direction only: a shard returning too *many* rows costs
bandwidth, while a shard returning too few has silently changed the answer, and a shard
that *errors* has lost it altogether. So what may be pushed is a whitelist of shapes with
identical meaning in both engines rather than everything the unparser will render, and it
excludes columns whose declared type the coordinator reports as `text` without the shard
storing them as text. See
[gap-analysis-operator-literal.md](gap-analysis-operator-literal.md#-closed-filter-pushdown-which-would-have-broken-the-moment-it-was-enabled)
for the shapes that qualify and the measurements behind each exclusion.
