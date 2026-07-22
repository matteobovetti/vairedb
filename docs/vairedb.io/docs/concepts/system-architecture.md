# System Architecture

VaireDB follows a **coordinator/worker** topology: a single coordinator node
fronts the cluster, and N core nodes hold the data.

## High-level topology

```
                          +-----------+
                          |  Client   |
                          +-----+-----+
                                |
                                v
                         +------------+
                         |Coordinator |    Coordinator node:
                         |   Node     |    - Ballista scheduler (SELECT)
                         | (metadata) |    - gRPC write client (DML)
                         +--+--+--+---+    - metadata store (local KV)
                            |  |  |
               +------------+  |  +------------+
               |               |               |
          +----v----+    +-----v---+    +------v--+
          |  Core   |    |  Core   |    |  Core   |    Core nodes:
          | Node 0  |    | Node 1  |    | Node N  |    - DuckDB engine
          | (DuckDB)|    | (DuckDB)|    | (DuckDB)|    - Ballista executor
          |         |    |         |    |         |    - data shards
          | S0 S3   |    | S1 S0'  |    | S2 S1'  |    - shard replicas
          +---------+    +---------+    +---------+    (replica factor per table)

          S = primary shard    S' = replica shard
```

## Node types

| Node type | Role |
|-----------|------|
| **Core Node** | Stores data shards and executes distributed read queries via a Ballista executor backed by an embedded DuckDB instance. Receives write operations (INSERT/UPDATE/DELETE) directly from the coordinator as shard-local SQL. Also hosts shard replicas based on the replica factor defined at table level. |
| **Coordinator Node** | Accepts client connections. Routes SELECT queries through a Ballista scheduler for distributed execution. Routes write operations directly to core nodes as shard-local SQL via a dedicated gRPC write service. Manages database metadata via a local embedded KV store. |

!!! warning "Single coordinator"
    Only a single coordinator node is supported, making it a single point of
    failure for both reads and writes. See [Design Goals](design-goals.md).

## Where to go next

- [Coordinator Node](coordinator-node.md) — routing, planning, and the catalog.
- [Core Node](core-node.md) — the DuckDB engine and local execution.
- [Data Distribution](data-distribution.md) — how shards and replicas are placed.
