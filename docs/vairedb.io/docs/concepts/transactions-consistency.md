# Transactions and Consistency

VaireDB's consistency guarantees come from synchronous quorum writes combined
with primary-preferred read routing. These guarantees apply **per shard**.

## Consistency model

- Reads served from a shard's **primary** observe the latest committed write —
  **strong consistency per shard**.
- Reads that fall back to a **replica** may observe slightly stale data if that
  replica is lagging — **eventual consistency**.
- There is no client-selectable read mode; the routing policy is fixed.

Because multi-shard writes are not atomic (see below), **cross-shard consistency
is not guaranteed** — one shard may commit while another fails.

## Distributed transactions

**Single-statement writes** are executed and committed by the local DuckDB
instance, which provides full ACID guarantees. The coordinator forwards each
shard-local statement to its target shard, where DuckDB commits it
independently.

!!! warning "Multi-statement transactions are not atomic"
    `BEGIN` / `COMMIT` / `ROLLBACK` are accepted but **not honored as atomic
    units** — each statement commits on its own.

!!! warning "Multi-shard transactions are not supported"
    When a write targets multiple shards, the coordinator sends shard-local SQL
    to each shard independently. Each shard commits independently after quorum
    acknowledgment. There is **no cross-shard atomicity** — if one shard commits
    and another fails, the successful shard is not rolled back.

This is a deliberate trade-off. As an OLAP database, VaireDB targets analytical
workloads where multi-shard atomic writes are not needed. Avoiding distributed
transaction protocols (2PC, 3PC, Saga) eliminates significant coordinator
complexity and write-path latency.
