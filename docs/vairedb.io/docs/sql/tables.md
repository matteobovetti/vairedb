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

## Create a table from a query

`CREATE TABLE ... AS SELECT` takes its column list from the query's result, so the
only thing you have to add is the sharding clause:

```sql
CREATE TABLE recent_users WITH (
    shards = 1,
    replication_factor = 1,
    shard_by = 'HASH(id)'
) AS SELECT id, name, created_at FROM foo_table WHERE created_at > '2026-01-01';
```

The query runs first; its rows are then routed to their shards like any `INSERT`.

!!! warning "The `WITH (...)` clause goes before `AS SELECT`"
    Written after the query it is parsed as a hint on the source table instead of a
    table option, and the statement is then rejected for naming no shard key. A
    `CREATE TABLE ... AS SELECT` must name `shard_by` explicitly — there is no column
    list to infer one from — and the column it names must appear in the result.

!!! note "Not atomic across shards"
    Filling the new table is a multi-shard write. If it fails part-way the table is
    dropped again, so a half-filled table is never left behind under a name you would
    read from.

## Schemas

A schema is a namespace for tables, views and indexes. Create it first, then qualify the
names you put in it:

```sql
CREATE SCHEMA sales;
CREATE SCHEMA IF NOT EXISTS sales;

CREATE TABLE sales.orders (
    id INTEGER NOT NULL,
    amount INTEGER
) WITH (shards = 3, replication_factor = 2, shard_by = 'HASH(id)');

INSERT INTO sales.orders (id, amount) VALUES (1, 100);
SELECT SUM(amount) FROM sales.orders;
```

Two schemas are independent namespaces: `sales.orders` and `billing.orders` are two
different tables, with their own columns, shard layout and rows. A schema is a
coordinator-side namespace only — nothing about it changes how a table is sharded or
replicated.

An unqualified name always means the default schema, `public`. There is no `search_path`:
a name means the same thing on every connection, and to reach another schema you write its
qualifier.

A schema has to exist before something is created in it — `CREATE TABLE` or `CREATE VIEW`
into a schema that does not exist is rejected and names the schema, rather than creating it
for you.

Dropping one requires it to be empty:

```sql
DROP SCHEMA sales;
DROP SCHEMA IF EXISTS sales;
```

The error names a relation that is still in the schema. There is no `CASCADE` — dropping
tables (and their per-shard storage on every replica) as a side effect of a statement about
a namespace is not something VaireDB will do for you. Drop the relations first.

### Which schema an object lives in

| Object | Schema |
|--------|--------|
| Table, view | The one its name says; the default schema if unqualified. |
| Index | The schema of the table it indexes. `CREATE INDEX` takes a **bare** name, as in PostgreSQL — `CREATE INDEX idx_amount ON sales.orders (amount)` creates `sales.idx_amount`, and `DROP INDEX sales.idx_amount` needs the qualifier. |
| Constraint | Named per table, so you always write the plain name you declared — `ALTER TABLE sales.orders DROP CONSTRAINT uq_id`. |

The same index name can therefore be used once per schema, which is what one DDL script
applied per tenant produces.

A rename stays inside the schema: `ALTER TABLE sales.orders RENAME TO invoices` gives
`sales.invoices`. A qualified destination is rejected — `RENAME TO` is not a way to move a
table between schemas, and there is no `ALTER TABLE ... SET SCHEMA`.

`public`, `pg_catalog`, `information_schema` and `vairedb_catalog` always exist and are not
yours to create or drop. `CREATE SCHEMA ... AUTHORIZATION` is rejected: VaireDB has no role
model, so an owner could only be discarded. `ALTER SCHEMA ... RENAME TO` is not supported.

!!! warning "A dot in a name is always a qualifier"
    Each shard stores a qualified table under a single physical name with the qualifier
    folded in, so `sales.orders` is stored as `sales_orders_shard<n>`. Two consequences:
    `sales.orders` and a table literally named `sales_orders` cannot both exist — the
    second one is rejected and names the first — and a quoted name containing a dot
    (`"sales.orders"`) is read as the qualified name, not as a one-part name with a dot in
    it.

!!! note "Not inside a transaction block"
    Like view DDL, `CREATE SCHEMA` and `DROP SCHEMA` are written to the coordinator catalog
    immediately, so `COMMIT` and `ROLLBACK` have nothing to decide. Run them outside a
    `BEGIN` ... `COMMIT` block.

## Inspecting the catalog

The coordinator's metadata catalog is queryable as virtual tables under the
`vairedb_catalog` schema:

```sql
-- List VaireDB-managed tables via standard information_schema
SELECT * FROM information_schema.tables
WHERE table_schema = 'vairedb_catalog';

-- Or query the catalog directly
SELECT * FROM vairedb_catalog.tables;

-- The schemas you created (the default schema is not listed — it always exists)
SELECT schema_name, created_at FROM vairedb_catalog.schemas;
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

### Rename a table

```sql
ALTER TABLE foo_table RENAME TO bar_table;
```

The shard layout, shard key and column definitions follow the table, so the new
name reads and accepts writes immediately and the old one stops resolving.
`RENAME TO` must be a statement of its own — it cannot be combined with other
`ALTER TABLE` actions — and the destination name must be free.

!!! note "DDL semantics"
    `CREATE TABLE` is applied **atomically** across all target core nodes — it
    rolls back if any node fails. `ALTER TABLE` and `DROP TABLE` are
    **best-effort** and fail only if a target node is unreachable. See
    [Query Processing — DDL path](../concepts/query-processing.md#ddl-path).

## Constraints

Every constraint is enforced by each shard on its own rows, so which kinds VaireDB
accepts follows from that:

```sql
CREATE TABLE orders (
    id INTEGER NOT NULL,
    customer VARCHAR NOT NULL,
    amount INTEGER,
    PRIMARY KEY (id),
    CONSTRAINT amount_positive CHECK (amount > 0)
) WITH (shards = 3, replication_factor = 2, shard_by = 'HASH(id)');
```

| Kind | Accepted | Why |
|------|----------|-----|
| `CHECK` | Always | A predicate on one row, so a shard checking its own rows checks all of them. |
| `NOT NULL` | Always | Same — a per-row check, written on the column. |
| `UNIQUE`, `PRIMARY KEY` | Only if the columns include the **shard key** | Equal shard keys share a shard, so one shard sees every row that could collide. |
| `FOREIGN KEY` | Never | The referenced row usually lives on another shard, and no shard can check a reference it cannot see. |

A `UNIQUE` or `PRIMARY KEY` off the shard key is rejected rather than accepted and
enforced per shard — the same rule as a [unique index](#unique-indexes). The error names
the shard key the constraint would have to include.

### Add or drop a constraint later

One form works on a table that already exists — a named `UNIQUE` over the shard key:

```sql
ALTER TABLE orders ADD CONSTRAINT uq_id UNIQUE (id);
ALTER TABLE orders DROP CONSTRAINT uq_id;
ALTER TABLE orders DROP CONSTRAINT IF EXISTS uq_id;
```

It is applied as one unique index per shard, so it validates the rows already stored (a
shard holding duplicates rejects the statement and nothing is recorded) and it can be
dropped again. Like an index, its name shares the table namespace, and it must be the
only action in its `ALTER TABLE` statement.

!!! warning "Everything else has to be declared at `CREATE TABLE`"
    `ADD CONSTRAINT ... CHECK`, `ADD PRIMARY KEY`, `ADD FOREIGN KEY`, `DROP PRIMARY KEY`
    and dropping a constraint that came from the table's own definition are rejected: the
    storage engine implements none of them, so there is no honest way to apply them
    shard by shard. To add one, recreate the table with the constraint declared.

!!! warning "A constraint restricts what you can alter"
    A column covered by a declared `UNIQUE`, `PRIMARY KEY` or `CHECK` cannot be retyped,
    and one covered by a `UNIQUE` or `PRIMARY KEY` cannot be dropped — the error names
    the constraint. Dropping a column a `CHECK` covers is allowed and takes the `CHECK`
    with it. Renaming a column carries its constraints along, `CHECK` body included.
    `ALTER COLUMN ... DROP NOT NULL` on a primary-key column is rejected: the primary key
    implies `NOT NULL`, so the statement would have no effect.

!!! note "Not atomic across shards"
    Adding or dropping a constraint is applied shard by shard. If a node is unreachable
    the statement reports the partial failure and the catalog is left unchanged;
    re-running it finishes the job.

## Indexes

An index is created once per shard, on every replica, and dropped the same way:

```sql
CREATE INDEX idx_created_at ON foo_table (created_at);
CREATE INDEX IF NOT EXISTS idx_name_email ON foo_table (name, email);

DROP INDEX idx_created_at;
DROP INDEX IF EXISTS idx_name_email;
```

Index names live in the same namespace as tables and views, so an index cannot take a
name one of those holds — and `DROP INDEX` can never drop a table or a view. An index on a
table in another schema belongs to that schema; see
[Which schema an object lives in](#which-schema-an-object-lives-in).

### Unique indexes

A `UNIQUE` index must include the **shard key**:

```sql
-- foo_table is sharded by id
CREATE UNIQUE INDEX idx_id ON foo_table (id);
```

Rows with the same shard key always live on the same shard, so a per-shard unique
index sees every row that could collide — the constraint holds cluster-wide. A
`UNIQUE` index on any other column is rejected: two equal values can land on
different shards, where neither would notice the other. Narrowing a unique index
with `WHERE` or `NULLS NOT DISTINCT` is rejected for the same reason.

A unique index on the shard key also serves as the conflict target for
`INSERT ... ON CONFLICT`, so an existing table can be made upsertable without
recreating it. `ALTER TABLE ... ADD CONSTRAINT ... UNIQUE (id)` is the same thing under a
constraint's name — see [Constraints](#constraints).

!!! note "Index options are about speed, not results"
    `USING <method>`, `INCLUDE`, `CONCURRENTLY`, collations, operator classes, sort
    order and `WITH (...)` are accepted and dropped: the storage engine has a single
    index type, and none of these change query results. Anything that *would* change
    results is rejected instead — including an unnamed index and an index over an
    expression rather than plain columns.

!!! warning "Drop the index before altering the table"
    While a table carries an index, its columns cannot be dropped, renamed, retyped
    or made (non-)nullable, and the table cannot be renamed — the storage engine
    refuses to alter a table an index depends on, whether or not the index covers
    the column in question. Adding a column and changing a column default are the
    exceptions. The error names the index to drop first. A `UNIQUE` constraint added with
    `ALTER TABLE` counts as an index here, because that is what enforces it — the error
    then names the `DROP CONSTRAINT` to run.

!!! note "Not atomic across shards"
    Index DDL is applied shard by shard. If a node is unreachable the statement
    reports the partial failure; re-running it finishes the job. Like all DDL, it
    cannot be issued inside a `BEGIN` ... `COMMIT` block.

## Empty a table

```sql
TRUNCATE TABLE foo_table;
```

Every shard is emptied; the table, its schema and its shard layout survive.
`CASCADE`, `RESTRICT`, `RESTART IDENTITY` and truncating several tables in one
statement are rejected rather than silently approximated.

## Drop a table

```sql
DROP TABLE foo_table;
```

## Views

A view is a stored query you can select from by name:

```sql
CREATE VIEW active_users AS
    SELECT id, name, email FROM foo_table WHERE status = 'active';

SELECT name FROM active_users ORDER BY name;
```

The view is kept in the coordinator as the query you wrote, and that query is planned
again on every read. So a view is never out of date, and it costs exactly what the same
query costs written by hand: the same shards are read, the same filters pushed down.
Nothing is copied and nothing needs refreshing.

`IF NOT EXISTS`, an explicit column list, and a view that reads another view all work:

```sql
CREATE VIEW IF NOT EXISTS user_names (label) AS SELECT name FROM foo_table;
CREATE VIEW active_names AS SELECT name FROM active_users;
```

### Redefine and drop

```sql
CREATE OR REPLACE VIEW active_users AS SELECT id, name FROM foo_table WHERE status = 'active';
ALTER VIEW active_users AS SELECT id, name, email FROM foo_table WHERE status = 'active';

DROP VIEW active_users;
DROP VIEW IF EXISTS active_names;
```

The definition is checked when you write it, not the first time someone reads it — a view
over a table that does not exist, or with a column list that does not match its query, is
rejected right away. `ALTER VIEW` will not create a view that does not already exist, and
`ALTER VIEW ... RENAME TO` is not supported (drop it and create it under the new name).

### What to expect

A view shares the relation namespace with tables and indexes: it cannot take a name one of
those holds, and neither can they take its name. Every statement that needs a real table
says so rather than doing something surprising — `DROP TABLE`, `ALTER TABLE`, `TRUNCATE`
and `CREATE INDEX` aimed at a view are rejected and name the statement to use instead, and
`DROP VIEW` aimed at a table refuses and leaves the table alone.

To read a definition back, query the catalog:

```sql
SELECT view_name, definition FROM vairedb_catalog.views;
```

!!! note "A view is read-only"
    `INSERT`, `UPDATE`, `DELETE` and `MERGE` on a view are rejected — write to the tables
    it reads. Routing a write through a view's definition to the right shard is not
    something the coordinator can do.

!!! note "No materialized views"
    `CREATE MATERIALIZED VIEW` is rejected: the coordinator stores no rows, only the
    definition. To keep a snapshot, create a real table with
    `CREATE TABLE ... WITH (shard_by = ...) AS SELECT ...`.

!!! warning "Some forms are rejected on purpose"
    `TEMPORARY`, `WITH (...)`, a typed column in the column list and the other dialect
    decorations are rejected by name rather than accepted and ignored. A view cannot be
    defined over `pg_catalog`, `information_schema` or `vairedb_catalog`, nor take the
    name of a `pg_catalog` table — introspection does not go through views, so such a view
    would never resolve. A definition that reads itself, directly or through another view,
    is rejected too.

!!! note "Not inside a transaction block"
    View DDL is written to the coordinator catalog immediately, so `ROLLBACK` could not
    undo it. Run it outside a `BEGIN` ... `COMMIT` block.
