# Transactions and Consistency

## Consistency Model

VaireDB's consistency guarantees come from synchronous quorum writes combined with the cluster's primary-preferred read routing (see [Replication](data-distribution.md#replication)). These guarantees apply **per shard**. Since multi-shard writes are not atomic (see [Distributed Transactions](#distributed-transactions)), cross-shard consistency is not guaranteed — one shard may commit while another fails.

Reads served from a shard's primary observe the latest committed write (strong consistency per shard). When a read falls back to a replica, it may observe slightly stale data if that replica is lagging (eventual consistency). There is no client-selectable read mode — the routing policy is fixed.

## Distributed Transactions

**Single-statement writes** are executed and committed by the local DuckDB instance, which provides full ACID guarantees. The coordinator forwards each shard-local statement to its target shard, where DuckDB commits it independently. Multi-statement client transactions are not supported: transaction-control commands (`BEGIN`/`COMMIT`/`ROLLBACK`) are accepted but not honored as atomic units — each statement commits on its own.

**Multi-shard transactions** are not supported. When a write operation targets multiple shards, the coordinator sends shard-local SQL to each shard independently via the gRPC WriteService. Each shard commits independently after quorum acknowledgment. There is no cross-shard atomicity — if one shard commits and another fails, the system does not roll back the successful shard.

This is a deliberate trade-off: as an OLAP database, VaireDB targets analytical workloads where multi-shard atomic writes are not needed. Avoiding distributed transaction protocols (2PC, 3PC, Saga) eliminates significant coordinator complexity and write-path latency.
