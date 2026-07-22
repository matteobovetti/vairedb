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

-- Aggregation pushed down to shards, finalized on the coordinator
SELECT count(*) AS total, min(created_at) AS first_signup
FROM users;
```

## 6. Evolve the schema

```sql
ALTER TABLE users ADD COLUMN age INTEGER;

UPDATE users SET age = 30 WHERE id = 1;

ALTER TABLE users RENAME COLUMN email TO email_address;
```

## 7. Clean up

```sql
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

Follow these flows in detail in
[Query Processing](../concepts/query-processing.md).
