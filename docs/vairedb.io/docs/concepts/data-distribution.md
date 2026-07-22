# Data Distribution

VaireDB distributes a table's rows across core nodes as **shards**, and keeps
multiple copies of each shard as **replicas** for fault tolerance.

## Sharding strategy

| Strategy | Description |
|----------|-------------|
| **Hash sharding** | Rows assigned to shards via `hash(shard_key) % S`, where `S` is the shard count. This is the only sharding strategy currently implemented. |
| **Range sharding** *(planned)* | Rows assigned by key ranges; better for range scans. Not yet implemented — see the [Roadmap](../roadmap.md). |

**Shard granularity.** Shard count and assignment are fixed at table creation
time and do not change automatically. Online resharding (splitting or merging
shards) is deferred to a future version.

**Integration with query routing.** The shard map in the metadata catalog is
used by both query paths on the coordinator:

- **SELECT queries** — the Ballista scheduler uses the shard map to assign query
  stages to the correct core node executors based on data locality.
- **Write operations** — the write service uses the shard map to resolve the
  primary and replica core nodes for each target shard.

The coordinator translates logical table references into shard-local DuckDB
tables (e.g. `orders` on shard `S0` becomes `orders_shard0`) on both paths. Each
core node receives an already-shard-local name and scans the corresponding
DuckDB table directly.

## Replication

| Parameter | Description |
|-----------|-------------|
| **Replication factor** (`N`) | Number of copies of each shard. Defaults to a cluster-wide configured value, overridable per table at creation. |
| **Replica placement** | Primaries and replicas are distributed round-robin across the alive nodes. Rack/zone-aware placement is *planned*. |

DuckDB has no built-in WAL replication, so **all replication is handled at the
application level** by the coordinator.

### Write path (synchronous quorum)

This is the canonical description of the full write flow:

1. The coordinator parses the DML via DataFusion (`sqlparser-rs`) and resolves
   the target shard(s) via the metadata catalog.
2. It translates the SQL into DuckDB dialect and rewrites it into shard-local
   statements (e.g. `INSERT INTO orders …` becomes `INSERT INTO orders_shard0 …`).
3. It sends the shard-local SQL to the **primary core node** and all **replica
   core nodes** for the target shard via gRPC WriteService.
4. Each core node enqueues the statement in its per-node write queue, executes
   it sequentially against DuckDB, and acknowledges completion.
5. The coordinator waits for acknowledgment from a **quorum** —
   `floor(N / 2) + 1` nodes (including the primary). The primary is always
   required; if it is unreachable, the shard is unavailable for writes.
6. Once quorum is reached, the coordinator responds to the client.
7. Nodes that have not yet acknowledged catch up asynchronously in the
   background.

This is a **synchronous quorum write with asynchronous tail replication**: the
write path blocks until a majority confirms, but lagging nodes are not on the
critical path. The coordinator retries the same shard-local SQL against lagging
nodes in the background until they acknowledge. If a node stays unreachable
beyond the heartbeat timeout, it is marked dead and stops receiving retries
(see [Failure Detection](cluster-coordination.md#failure-detection)).

A different replication factor can be set per table at creation time; otherwise
the cluster-wide default applies.

### Read path

SELECT queries are planned by the embedded Ballista scheduler, which uses a
single shard-affinity policy to assign each per-shard scan:

- **Primary-preferred** — a scan is assigned to the executor on the node that
  owns the shard's primary whenever that node has capacity.
- **Replica fallback** — if the primary cannot take the scan, it falls back to
  an executor holding a replica.

Scans for shards an executor neither owns nor replicates are never assigned to
it. There is currently no client-selectable read mode.

### Consistency

Reads served from a primary see the latest committed writes. Reads that fall
back to a replica may serve slightly stale data if the replica is lagging.

!!! warning "Cross-shard read consistency"
    For multi-shard queries, different shards may be read from replicas at
    different replication lag, producing an inconsistent snapshot across shards
    within a single query (e.g. a join may reference rows not yet visible on a
    lagging replica). See [Transactions & Consistency](transactions-consistency.md).

### Replication transport

The coordinator sends shard-local SQL directly to primary and replica core nodes
via a dedicated gRPC write service. This service is separate from the Ballista
executor-scheduler connections used for distributed SELECTs.
