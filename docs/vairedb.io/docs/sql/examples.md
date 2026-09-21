# Worked Example

A complete session against a running cluster (see the
[Quick Start](../getting-started/quickstart.md) to bring one up).

## 1. Connect

```bash
psql -h 127.0.0.1 -p 5432 "sslmode=disable"
```

## 2. Create a sharded, replicated table

On a five-node cluster, spread the table across three shards, each replicated
three times:

```sql
CREATE TABLE users (
    id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP
) WITH (
    shards = 3,
    replication_factor = 3,
    shard_by = 'HASH(id)'
);
```

## 3. Confirm it landed in the catalog

```sql
SELECT * FROM vairedb_catalog.tables;
```

## 4. Insert rows

Rows hash to different shards by `id`:

```sql
INSERT INTO users (id, name, email, created_at)
VALUES (1, 'Alice', 'alice@example.com', '2026-01-15 10:30:00');

INSERT INTO users (id, name, email, created_at)
VALUES (2, 'Bob', 'bob@example.com', '2026-02-20 14:00:00');

INSERT INTO users (id, name, email, created_at)
VALUES (3, 'Charlie', 'charlie@example.com', '2026-03-10 09:15:00');

INSERT INTO users (id, name, email, created_at)
VALUES (4, 'Dana', 'dana@example.com', '2026-03-11 09:15:00');
```

## 5. Query

```sql
-- Distributed full scan, merged on the coordinator
SELECT * FROM users ORDER BY id;

-- Point lookup routed to the owning shard
SELECT id, name FROM users WHERE id = 1;

-- Aggregated in two phases: partially per shard's rows, finalized on the coordinator
SELECT count(*) AS total, min(created_at) AS first_signup
FROM users;
```

## 6. Evolve the schema

```sql
ALTER TABLE users ADD COLUMN age INTEGER;

UPDATE users SET age = 30 WHERE id = 1;

ALTER TABLE users RENAME COLUMN email TO email_address;
```

## 7. Bulk load and export

`COPY` writes a file holding every shard's rows, and reads one back routing each
row to the shard that owns it. Parquet is the format to reach for here: it carries
its own column names and types, so the import reads typed values instead of
re-parsing text.

Export the table — note the column is `email_address` now, after the rename in
step 6:

```sql
COPY users (id, name, email_address) TO '/tmp/users.parquet' (FORMAT PARQUET);
-- COPY 4
```

Load it into a second sharded table:

```sql
CREATE TABLE users_archive (
    id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    email_address VARCHAR(255)
) WITH (
    shards = 3,
    replication_factor = 3,
    shard_by = 'HASH(id)'
);

COPY users_archive FROM '/tmp/users.parquet' (FORMAT PARQUET);
-- COPY 4

SELECT count(*) FROM users_archive;
```

The import named no columns, so the file's own schema decided which ones it
filled. State them to map by position instead — the list then has to be exactly as
wide as the file:

```sql
-- An import appends, so clear the table first or the four rows land twice
TRUNCATE users_archive;

COPY users_archive (id, name, email_address)
  FROM '/tmp/users.parquet' (FORMAT PARQUET);
-- COPY 4
```

`FORMAT PARQUET` takes no options: `HEADER`, `DELIMITER` and `QUOTE` describe a CSV
layout and are rejected rather than ignored. CSV works here too, and is the only
format the streaming forms carry — note that both sides have to agree on the
columns, since a CSV header is just text:

```sql
TRUNCATE users_archive;

COPY users (id, name, email_address) TO '/tmp/users.csv' (FORMAT CSV, HEADER);
COPY users_archive FROM '/tmp/users.csv' (FORMAT CSV, HEADER);
-- COPY 4
```

!!! warning "The path is the coordinator's"
    `/tmp/users.parquet` is resolved inside the **coordinator** process, not on the
    machine running `psql`. On the Quick Start cluster that is the coordinator
    container, so a file produced elsewhere has to be put there first:

    ```bash
    docker cp users.parquet vairedb-coordinator:/tmp/users.parquet
    ```

    To move rows to and from your own client instead, use `psql`'s `\copy`, which
    is CSV-only — see [Bulk load and export](querying.md#bulk-load-and-export) for
    why Parquet has no streaming form.

## 8. Clean up

```sql
DROP TABLE users_archive;
DROP TABLE users;
```

## What happened under the hood

- `CREATE TABLE` registered the schema and shard/replica map in the coordinator
  catalog, then broadcast shard-local `CREATE TABLE users_shardN (...)` to the
  owning core nodes atomically.
- Each `INSERT` was hashed to a shard, rewritten to `INSERT INTO users_shardN`,
  and sent to that shard's primary and replicas until a quorum acknowledged.
- Each `SELECT` was planned by the Ballista scheduler, executed as shard-local
  DuckDB scans, streamed back via Arrow Flight, and merged on the coordinator.
- `COPY … TO` ran its source on that same read path, so the file gathered every
  shard's rows; `COPY … FROM` decoded the file into batches and re-entered the
  write path above, so each row was hashed and replicated exactly as a
  hand-written `INSERT` is.

Follow these flows in detail in
[Query Processing](../concepts/query-processing.md).
