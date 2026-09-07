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

**Multi-statement transactions** — `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT` and
`RELEASE SAVEPOINT` — are supported, so a driver or ORM that opens a transaction
implicitly works as it expects to. VaireDB has no distributed commit protocol, so
the block is never held open on the shards: the coordinator **buffers** the
block's writes and submits them when you commit.

That gives you:

- **A real `ROLLBACK`.** Nothing has reached a shard yet, so discarding the buffer
  *is* the rollback. `ROLLBACK TO SAVEPOINT` is exact for the same reason.
- **Atomicity within one shard group.** A block whose writes all belong to a
  single shard's replica set is applied as one shard-local transaction: all of it,
  or none of it.
- **No silent half-writes across shard groups.** A block spanning several is
  refused at `COMMIT` — nothing is written — unless you opt in with
  [`allow_cross_shard_transactions`](../reference/configuration.md).

!!! info "What a transaction block will not accept"
    Because the writes are buffered instead of run as they arrive, a block only
    accepts statements VaireDB can answer truthfully before running them.
    `INSERT` and reads are fine. These are refused inside a block, each error
    naming what to do instead:

    - `UPDATE` and `DELETE` — the affected-row count is only known once the
      shards run the statement, so reporting one now would be a guess.
    - DDL (`CREATE`, `ALTER`, `DROP`, `TRUNCATE`) — it applies to the catalog and
      every shard immediately, so `COMMIT` could not undo it.
    - Reading a table the same block has already written — the write is still
      buffered, so the read would silently miss it.

    Run those outside a transaction block. Isolation levels are accepted and
    ignored; `READ ONLY` is honored.

!!! warning "Multi-shard writes are not atomic"
    A **single statement** spanning shards — and a committed block spanning
    shard groups when the opt-in above is enabled — is sent to each shard
    independently and committed there after quorum acknowledgment. There is **no
    cross-shard atomicity**: if one shard commits and another fails, the
    successful shard is not rolled back. The failure is reported as a partial
    one, so it is never mistaken for a clean rollback.

This is a deliberate trade-off. As an OLAP database, VaireDB targets analytical
workloads where multi-shard atomic writes are not needed. Avoiding distributed
transaction protocols (2PC, 3PC, Saga) eliminates significant coordinator
complexity and write-path latency, while the buffered block still gives ordinary
clients the transaction semantics they expect.
