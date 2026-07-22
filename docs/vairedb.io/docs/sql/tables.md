# Tables & Schema

## Create a table

`CREATE TABLE` takes a `WITH (...)` clause that controls how the table is
sharded and replicated across core nodes:

```sql
CREATE TABLE foo_table (
    id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP
) WITH (
    shards = 1,
    replication_factor = 1,
    shard_by = 'HASH(id)'
);
```

| Option | Meaning |
|--------|---------|
| `shards` | Number of shards the table is split into. Fixed at creation time. |
| `replication_factor` | Number of copies (`N`) of each shard. Overrides the cluster-wide `default_replication_factor`. |
| `shard_by` | The sharding expression. `HASH(<column>)` assigns rows via `hash(column) % shards`. |

!!! tip "Pick a high-cardinality shard key"
    Hash sharding distributes rows by `hash(shard_key) % shards`. Choose a column
    with many distinct values (like an `id`) so shards stay balanced. See
    [Data Distribution](../concepts/data-distribution.md#sharding-strategy).

!!! warning "Sharding is fixed at creation"
    `shards` and `shard_by` cannot be changed later — online resharding is not
    yet supported (see the [Roadmap](../roadmap.md)). Replication factor for a
    table is also chosen at creation time.

## Inspecting the catalog

The coordinator's metadata catalog is queryable as virtual tables under the
`vairedb_catalog` schema:

```sql
-- List VaireDB-managed tables via standard information_schema
SELECT * FROM information_schema.tables
WHERE table_schema = 'vairedb_catalog';

-- Or query the catalog directly
SELECT * FROM vairedb_catalog.tables;
```

## Alter a table

### Add a column

```sql
ALTER TABLE foo_table ADD COLUMN age INTEGER;
ALTER TABLE foo_table ADD COLUMN status VARCHAR NOT NULL;
ALTER TABLE foo_table ADD COLUMN IF NOT EXISTS name VARCHAR;
```

### Drop a column

```sql
ALTER TABLE foo_table DROP COLUMN age;
ALTER TABLE foo_table DROP COLUMN IF EXISTS nonexistent_column;
```

### Rename a column

```sql
ALTER TABLE foo_table RENAME COLUMN email TO email_address;
```

### Change a column type

```sql
ALTER TABLE foo_table ALTER COLUMN name SET DATA TYPE TEXT;
```

### Change nullability

```sql
ALTER TABLE foo_table ALTER COLUMN email SET NOT NULL;
ALTER TABLE foo_table ALTER COLUMN email DROP NOT NULL;
```

### Change a default

```sql
ALTER TABLE foo_table ALTER COLUMN created_at SET DEFAULT '2026-01-01 00:00:00';
ALTER TABLE foo_table ALTER COLUMN created_at DROP DEFAULT;
```

### Multiple operations in one statement

```sql
ALTER TABLE foo_table ADD COLUMN phone VARCHAR, ADD COLUMN address TEXT;
```

!!! note "DDL semantics"
    `CREATE TABLE` is applied **atomically** across all target core nodes — it
    rolls back if any node fails. `ALTER TABLE` and `DROP TABLE` are
    **best-effort** and fail only if a target node is unreachable. See
    [Query Processing — DDL path](../concepts/query-processing.md#ddl-path).

## Drop a table

```sql
DROP TABLE foo_table;
```
