# Fault Tolerance and Recovery

## Write-Ahead Log (WAL)

- Each core node's DuckDB instance maintains its own local WAL for crash recovery.
- DuckDB does not support WAL replication. Replication is handled at the application level by the coordinator's write service, which sends shard-local SQL to all replicas (see [Replication](data-distribution.md#replication)).
- The WAL is used only for local durability: if a core node crashes, it replays its DuckDB WAL on restart to recover committed writes not yet folded into the database file.

## Snapshotting *(planned)*

Snapshotting is not yet implemented. The intended design takes periodic full snapshots of each shard using DuckDB's native checkpoint mechanism:

1. The coordinator pauses the per-node write queue on the target core node (drains in-flight writes).
2. The core node executes `FORCE CHECKPOINT` against its local DuckDB instance, flushing the WAL into the `.duckdb` file.
3. The resulting self-contained `.duckdb` file is copied to durable object storage (e.g. S3, GCS, MinIO).
4. The write queue is resumed.

This approach is simple but requires briefly pausing writes on the target node during the file copy. A later version aims to avoid this pause with a dedicated VaireDB snapshot system (see [Roadmap](roadmap.md)).

## Node Recovery

1. **Transient failure**: the core node restarts, DuckDB recovers from its local WAL, and the node reconnects to the coordinator and rejoins the cluster.
2. **Permanent failure**: in v0.1, requires manual intervention — a new core node must be provisioned and data restored from snapshots. Automatic shard rebuild from replicas is deferred to a future version (see [Roadmap](roadmap.md)).
3. **Network partition (core nodes)**: if a core node cannot reach the coordinator, it cannot receive writes or serve Ballista-dispatched reads. In v0.1, the coordinator marks unreachable nodes as dead after the heartbeat timeout and returns errors to clients for operations targeting the affected shards. Automatic traffic redirection to replicas is deferred to a future version (see [Roadmap](roadmap.md)).

## Quorum and Availability

Where `N` is the replication factor (number of copies of each shard):

- **Write quorum**: `floor(N / 2) + 1` nodes (including the primary) must acknowledge a write before the coordinator responds to the client. The primary is always required — if the primary is unreachable, the shard is unavailable for writes until the node reconnects or an operator intervenes (see [Failure Detection](cluster-coordination.md#failure-detection)).
- **Read consistency**: reads are routed primary-preferred with replica fallback (see [Replication — Read path](data-distribution.md#replication)). A read served by the primary is strongly consistent; a read that falls back to a lagging replica may be eventually consistent. The read routing is fixed, not client-selectable.
- **Availability**: a shard remains writable as long as the primary and at least `floor(N / 2)` additional replicas are reachable. Loss of the primary makes the shard unavailable for writes regardless of replica availability.
