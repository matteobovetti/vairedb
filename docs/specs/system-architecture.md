# System Architecture

## High-Level Topology

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

## Node Types

| Node Type | Role |
|-----------|------|
| **Core Node** | Stores data shards and executes distributed read queries via a Ballista executor backed by an embedded DuckDB instance. Receives write operations (INSERT/UPDATE/DELETE) directly from the coordinator as shard-local SQL. Also hosts a number of shard replicas based on the replica factor defined at table level. |
| **Coordinator Node** | Accepts client connections. Routes SELECT queries through a Ballista scheduler for distributed execution. Routes write operations (INSERT/UPDATE/DELETE) directly to core nodes as shard-local SQL via a dedicated gRPC write service. Handles database metadata via a local embedded KV store. Only a single coordinator node is supported, making it a single point of failure for both reads and writes (see [Design Goals](design-goals.md)). |
