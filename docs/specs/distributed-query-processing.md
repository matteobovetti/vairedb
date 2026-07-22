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

## Query Optimization (Delegated to Ballista + DataFusion)

Distributed query optimization — including join strategies (co-located, broadcast, shuffle), aggregation (two-phase, multi-level), and push-down (filters, projections, partial aggregations) — is entirely handled by [Apache Ballista](https://github.com/apache/datafusion-ballista) and [DataFusion](https://datafusion.apache.org/). VaireDB does not implement custom logic for these concerns; it relies on the built-in optimizer rules and execution strategies provided by the Ballista scheduler and DataFusion engine.
