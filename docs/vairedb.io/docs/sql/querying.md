# Querying Data

## Insert data

Each `INSERT` is routed to the shard that owns the row (based on the table's
`shard_by` key) and replicated to that shard's replicas. The coordinator
acknowledges once a [quorum](../concepts/fault-tolerance.md#quorum-and-availability)
confirms.

```sql
INSERT INTO foo_table (id, name, email, created_at)
VALUES (1, 'Alice', 'alice@example.com', '2026-01-15 10:30:00');

INSERT INTO foo_table (id, name, email, created_at)
VALUES (2, 'Bob', 'bob@example.com', '2026-02-20 14:00:00');

INSERT INTO foo_table (id, name, email, created_at)
VALUES (3, 'Charlie', 'charlie@example.com', '2026-03-10 09:15:00');
```

!!! note "Single-statement atomicity only"
    Each statement commits independently on its target shard. Multi-statement
    (`BEGIN`/`COMMIT`) and multi-shard atomic writes are not supported — see
    [Transactions & Consistency](../concepts/transactions-consistency.md).

## Select data

`SELECT` queries are planned by the embedded Ballista scheduler, which
dispatches shard-local scans to the core nodes and merges the results.

```sql
-- Full scan across all shards
SELECT * FROM foo_table;

-- Point lookup (routed to the owning shard)
SELECT id, name FROM foo_table WHERE id = 1;

-- Filter on a non-key column
SELECT name, email FROM foo_table WHERE name = 'Bob';

-- IN predicate
SELECT name, email FROM foo_table WHERE name IN ('Bob', 'Alice');
```

Filters, projections, aggregations, and joins are optimized and distributed by
Ballista and DataFusion. See
[Query Processing](../concepts/query-processing.md) for the full read path.

!!! warning "Cross-shard read snapshots"
    A query that touches multiple shards may read different shards at different
    replication lag if some scans fall back to replicas, producing an
    inconsistent snapshot across shards. See
    [Data Distribution — Consistency](../concepts/data-distribution.md#consistency).
