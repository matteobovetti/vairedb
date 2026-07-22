# Data Distribution

## Sharding Strategy

| Strategy | Description |
|----------|-------------|
| **Hash sharding** | Rows assigned to shards via `hash(shard_key) % S` where `S` is the shard count. This is the only sharding strategy currently implemented. |
| **Range sharding** *(planned)* | Rows assigned based on key ranges; better for range scans. Not yet implemented (see [Roadmap](roadmap.md)). |

**Shard granularity**

Shard count and assignment are fixed at table creation time. The number of shards is determined by the initial configuration and does not change automatically. Online resharding (splitting or merging shards when they grow or shrink) is deferred to a future version (see [Roadmap](roadmap.md)).

**Integration with query routing**

The shard map maintained by the metadata catalog (see [Metadata Catalog](coordinator-node.md#metadata-catalog-database-catalog)) is used by both query paths on the coordinator:

- **SELECT queries**: the Ballista scheduler uses the shard map to assign query stages to the correct core node executors based on data locality.
- **Write operations**: the coordinator's write service uses the shard map to resolve the primary and replica core nodes for each target shard.

The coordinator translates logical table references into shard-local DuckDB tables (e.g. `orders` on shard S0 becomes `orders_shard0`) on both paths — when rewriting writes and when building per-shard remote scans. Each core node receives an already-shard-local name and scans the corresponding DuckDB table directly.

## Replication

| Parameter | Description |
|-----------|-------------|
| **Replication factor** | Number of copies of each shard. Defaults to a cluster-wide configured value, overridable per table at creation. `N` refers to the replication factor throughout this section. |
| **Replica placement** | Primaries and replicas are distributed round-robin across the alive nodes. Rack/zone-aware placement to survive correlated failures is *planned* (see [Roadmap](roadmap.md)). |

DuckDB does not provide built-in WAL replication. All replication in VaireDB is handled at the application level.

**Write path (synchronous quorum)**

Write operations (INSERT/UPDATE/DELETE) are handled by the coordinator's dedicated write service, outside of the Ballista pipeline. This is the canonical description of the full write flow:

1. The coordinator parses the DML statement via DataFusion (`sqlparser-rs`) and resolves the target shard(s) via the metadata catalog.
2. The coordinator translates the SQL into DuckDB dialect (compatibility layer) and rewrites it into shard-local statements (e.g. `INSERT INTO orders VALUES (...)` becomes `INSERT INTO orders_shard0 VALUES (...)`).
3. The coordinator sends the shard-local SQL to the **primary core node** and all **replica core nodes** for the target shard via gRPC WriteService.
4. Each core node enqueues the statement in its per-node write queue and executes it sequentially against the local DuckDB instance, then acknowledges completion.
5. The coordinator waits for acknowledgment from a **quorum**: `floor(N / 2) + 1` nodes (including the primary) must confirm the write. The primary is always required in the quorum — if the primary is unreachable, the shard is unavailable for writes.
6. Once quorum is reached, the coordinator responds to the client.
7. Any nodes that have not yet acknowledged will catch up asynchronously in the background.

This is a **synchronous quorum write with asynchronous tail replication**: the write path blocks until a majority confirms, but lagging nodes are not on the critical path. The coordinator retries the same shard-local SQL statement against lagging nodes in the background until they acknowledge. If a node remains unreachable beyond the heartbeat timeout, it is marked as dead and stops receiving retries (see [Failure Detection](cluster-coordination.md#failure-detection)).

A different replication factor can be set per table at creation time, otherwise the cluster-wide default applies.

**Read path**

SELECT queries are planned by the embedded Ballista scheduler, which uses a single shard-affinity policy to assign each per-shard scan to a core node executor:

- **Primary-preferred**: a scan is assigned to the executor on the core node that owns the shard's primary whenever that node has capacity.
- **Replica fallback**: if the primary cannot take the scan, it falls back to an executor holding a replica of the shard.

Scans for shards an executor neither owns nor replicates are never assigned to it. There is currently no client-selectable read mode; the affinity policy above always applies.

**Consistency**

Reads served from a primary see the latest committed writes. Reads that fall back to a replica may serve slightly stale data if the replica is lagging. **Known limitation**: for multi-shard queries, different shards may be read from replicas at different replication lag, producing an inconsistent snapshot across shards within a single query (e.g. a join may reference rows not yet visible on a lagging replica).

**Replication transport**

The coordinator sends shard-local SQL statements directly to primary and replica core nodes via a dedicated gRPC write service. Each core node executes the received SQL against its local DuckDB instance. This write service is separate from the Ballista executor-scheduler gRPC connections used for distributed SELECT queries.
