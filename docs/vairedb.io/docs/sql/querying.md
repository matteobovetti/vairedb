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

!!! note "Atomicity is per shard group"
    Outside a transaction block each statement commits independently on its
    target shard. A `BEGIN` … `COMMIT` block is buffered by the coordinator and
    applied atomically when all its writes belong to one shard's replica set;
    `ROLLBACK` is honored for real, and `UPDATE`/`DELETE`/DDL are refused inside
    a block. Multi-shard atomic writes are not supported — see
    [Transactions & Consistency](../concepts/transactions-consistency.md).

## Insert the result of a query

`INSERT ... SELECT` works against sharded tables: the coordinator runs the query
first, then routes each produced row to the shard its key belongs to.

```sql
INSERT INTO foo_archive (id, name, email, created_at)
SELECT id, name, email, created_at FROM foo_table WHERE created_at < '2026-02-01';
```

The source is read to completion before anything is written, so a statement that
reads and writes the same table terminates instead of chasing its own output.

!!! note "`RETURNING` is not available on this form"
    The rows are written across several shards, so there is no single result set to
    hand back. Run the `SELECT` as its own statement if you need the rows.

## Merge rows (upsert)

`MERGE INTO` updates the rows a source matches, inserts the ones it does not, and
deletes the ones you ask it to — in one statement. VaireDB applies it **shard by
shard**, which it can do because equal shard keys always hash to the same shard: if
the `ON` clause requires the target's shard key to equal a source column, a target
row's only possible match lives on the same shard it does.

That makes the `ON` clause the rule to remember: **it must equate the table's
`shard_by` column with a column of the source.** Extra `AND` conditions are fine.

The source can be an inline list of rows, which is the upsert case and needs no
staging table and no unique index:

```sql
MERGE INTO foo_table AS t
USING (VALUES (1, 'Alice', 'alice@example.com'),
              (4, 'Dana',  'dana@example.com')) AS s (id, name, email)
   ON t.id = s.id
 WHEN MATCHED THEN UPDATE SET name = s.name, email = s.email
 WHEN NOT MATCHED THEN INSERT (id, name, email) VALUES (s.id, s.name, s.email);
```

Or another table, which is the batch-reconciliation case:

```sql
MERGE INTO foo_table AS t
USING foo_staging AS s
   ON t.id = s.id
 WHEN MATCHED AND s.deleted THEN DELETE
 WHEN MATCHED THEN UPDATE SET name = s.name, email = s.email
 WHEN NOT MATCHED THEN INSERT (id, name, email) VALUES (s.id, s.name, s.email)
 WHEN NOT MATCHED BY SOURCE THEN DELETE;
```

A table source has to line up with the target: same `shard_by` column as the one the
`ON` clause matches on, the same number of shards, and shards on the same nodes.
Create the two tables with the same `WITH (...)` options and they will.

`WHEN MATCHED`, `WHEN NOT MATCHED [BY TARGET]` and `WHEN NOT MATCHED BY SOURCE` are
all honored, with their `AND` conditions, and each can `UPDATE`, `INSERT` or
`DELETE`. `WHEN NOT MATCHED BY SOURCE` needs a **table** source: with an inline
`VALUES` list, a shard that owns none of the listed rows never sees the statement,
and its rows are exactly the ones that clause is about — so VaireDB rejects that
combination instead of silently skipping them.

!!! note "What VaireDB rejects, and why"
    Every rejection below is a shape whose rows the coordinator cannot place, so it
    is refused up front with nothing written:

    - a source it cannot enumerate — a subquery, a join, or a `VALUES` list with
      `LIMIT`/`ORDER BY`. Materialize it into a table first.
    - an `ON` clause that does not pin the shard key, or pins it only through an
      `OR`. Each shard would decide "not matched" without seeing the match, and
      insert a duplicate.
    - `UPDATE SET <shard key>` — that would move the row to another shard, the same
      restriction `UPDATE` has.
    - an `INSERT` clause that omits the shard key, or sets it to anything other than
      the column the `ON` clause matched.
    - `RETURNING`: the rows come back from every shard the merge touched, so there is
      no single result set to hand back.
    - a `MERGE` inside a `BEGIN` … `COMMIT` block, and a `MERGE` into a table with
      pseudonymized columns.

!!! warning "Not atomic across shards, and not re-runnable"
    Every shard's statement is planned before any is sent, so a merge that cannot be
    routed writes nothing. But a failure part-way through leaves the shards that
    already applied it applied, and a `MERGE` is generally not safe to simply re-run.
    Check the affected rows before retrying.

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

Aggregations and joins are planned and distributed by Ballista and DataFusion.
Projections, and the filters both engines read identically, are also carried into each
shard's own scan so a shard sends back the rows you asked for rather than its whole
table — the four queries above all qualify. A `LIMIT` with no filter under it becomes a
per-shard cap the same way.

None of that changes an answer. The coordinator re-applies every pushed filter and the
real `LIMIT` over the union of the shards' rows, so a filter it cannot safely delegate —
a function call, a cast, arithmetic — simply runs on the coordinator instead. See
[Query Processing](../concepts/query-processing.md#query-optimization) for the read path
and the shapes that qualify.

!!! warning "Cross-shard read snapshots"
    A query that touches multiple shards may read different shards at different
    replication lag if some scans fall back to replicas, producing an
    inconsistent snapshot across shards. See
    [Data Distribution — Consistency](../concepts/data-distribution.md#consistency).

## Bulk load and export

`COPY` moves rows between a table and a **CSV file on the coordinator**. An export
gathers from every shard; an import routes each row by its shard key.

```sql
-- Export: one file holding every shard's rows
COPY foo_table TO '/var/lib/vairedb/foo_table.csv' (FORMAT CSV, HEADER);

-- Export the result of a query instead of a whole table
COPY (SELECT id, name FROM foo_table WHERE id > 10)
  TO '/var/lib/vairedb/some_users.csv' (FORMAT CSV, HEADER);

-- Import: each row is routed to the shard that owns it
COPY foo_table (id, name, email, created_at)
  FROM '/var/lib/vairedb/foo_table.csv' (FORMAT CSV, HEADER);
```

`FORMAT CSV` is required — PostgreSQL's default is its own `TEXT` encoding, which
VaireDB does not write. `HEADER`, `DELIMITER` and `QUOTE` are honored; any other
option is rejected rather than ignored, so the file always matches what you asked
for. An import must include the table's shard key, and with `HEADER` the file's
header names decide which columns are filled.

!!! warning "The file is on the coordinator, not on your client"
    The path is resolved by the coordinator process, with the coordinator's
    filesystem permissions, and VaireDB has no user or role model to restrict it.
    Treat the ability to connect as the ability to read and write any path the
    coordinator can. The client-side streaming forms — `COPY ... FROM STDIN`,
    `COPY ... TO STDOUT` and therefore `psql`'s `\copy` — are not supported yet.

!!! note "Not atomic across shards"
    An import is a multi-shard write. Rows that cannot be routed are rejected with
    nothing written, but a failure part-way through leaves the earlier rows in place
    and reports that rather than claiming a rollback.
