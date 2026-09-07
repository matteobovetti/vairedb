# Transactions and Consistency

## Consistency Model

VaireDB's consistency guarantees come from synchronous quorum writes combined with the cluster's primary-preferred read routing (see [Replication](data-distribution.md#replication)). These guarantees apply **per shard**. Since multi-shard writes are not atomic (see [Distributed Transactions](#distributed-transactions)), cross-shard consistency is not guaranteed — one shard may commit while another fails.

Reads served from a shard's primary observe the latest committed write (strong consistency per shard). When a read falls back to a replica, it may observe slightly stale data if that replica is lagging (eventual consistency). There is no client-selectable read mode — the routing policy is fixed.

## Distributed Transactions

**Single-statement writes** are executed and committed by the local DuckDB instance, which provides full ACID guarantees. The coordinator forwards each shard-local statement to its target shard, where DuckDB commits it independently.

**Multi-statement client transactions** (`BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`) are supported without a distributed commit protocol: the transaction block is never held open on the shards. The coordinator buffers the block's writes for the duration of the block and submits them when the client commits. Two consequences follow, and they are the guarantee VaireDB offers:

- `ROLLBACK` — including `ROLLBACK TO SAVEPOINT` — is honored exactly, because nothing has reached a shard yet.
- A block whose writes all belong to one shard's replica set is applied as a single shard-local transaction, so it is atomic. A block spanning several is not, and is therefore **refused at commit time** rather than half-applied; operators who accept non-atomic commits can opt in by configuration.

Because the writes are buffered rather than executed as they arrive, a block can only contain statements whose outcome the coordinator can report truthfully before running them. Statements that cannot be — those whose affected-row count is only known once the shards run them, schema changes, and reads of a table the same block has already written — are rejected inside a block, naming the alternative. No statement is ever accepted with an invented answer.

**Multi-shard atomicity** is absent in both paths: a single statement spanning shards, and a committed block spanning replica sets with the opt-in enabled, commit shard by shard after quorum acknowledgment. If one shard commits and another fails, the successful shard is not rolled back; the failure is reported as a partial one, distinguishable from a clean rollback.

This is a deliberate trade-off: as an OLAP database, VaireDB targets analytical workloads where multi-shard atomic writes are not needed. Avoiding distributed transaction protocols (2PC, 3PC, Saga) eliminates significant coordinator complexity and write-path latency, while the buffered block still gives ordinary clients and ORMs the transaction semantics they expect.
