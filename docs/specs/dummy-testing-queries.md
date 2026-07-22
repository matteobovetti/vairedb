# Coordinator Node - Testing Queries

Connect to the coordinator using any PostgreSQL client:

```bash
psql -h 127.0.0.1 -p 5432 sslmode=disable
```

## Create a table

```sql
CREATE TABLE foo_table (
    id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP
) WITH (
    shards = 5,
    replication_factor = 2,
    shard_by = 'HASH(id)'
);
```

## Show tables

```sql
SELECT * FROM information_schema.tables where table_schema = 'vairedb_catalog';

SELECT * FROM vairedb_catalog.tables;
```

## Insert data

```sql
INSERT INTO foo_table (id, name, email, created_at)
VALUES (1, 'Alice', 'alice@example.com', '2026-01-15 10:30:00');

INSERT INTO foo_table (id, name, email, created_at)
VALUES (2, 'Bob', 'bob@example.com', '2026-02-20 14:00:00');

INSERT INTO foo_table (id, name, email, created_at)
VALUES (3, 'Charlie', 'charlie@example.com', '2026-03-10 09:15:00');

INSERT INTO foo_table (id, name, email, created_at, age)
VALUES (4, 'Matteo', 'matteo@gmail.com', '2026-03-11 09:15:00', 20);
```

## Select data

```sql
SELECT * FROM foo_table;

SELECT id, name FROM foo_table WHERE id = 1;

SELECT name, email FROM foo_table WHERE name = 'Bob';

SELECT name, email FROM foo_table WHERE name IN ('Bob', 'Alice');
```

## Alter table

### Add column

```sql
ALTER TABLE foo_table ADD COLUMN age INTEGER;

ALTER TABLE foo_table ADD COLUMN status VARCHAR NOT NULL;

ALTER TABLE foo_table ADD COLUMN IF NOT EXISTS name VARCHAR;
```

### Drop column

```sql
ALTER TABLE foo_table DROP COLUMN age;

ALTER TABLE foo_table DROP COLUMN IF EXISTS nonexistent_column;
```

### Rename column

```sql
ALTER TABLE foo_table RENAME COLUMN email TO email_address;
```

### Alter column type

```sql
ALTER TABLE foo_table ALTER COLUMN name SET DATA TYPE TEXT;
```

### Alter column nullability

```sql
ALTER TABLE foo_table ALTER COLUMN email SET NOT NULL;

ALTER TABLE foo_table ALTER COLUMN email DROP NOT NULL;
```

### Alter column default

```sql
ALTER TABLE foo_table ALTER COLUMN created_at SET DEFAULT '2026-01-01 00:00:00';

ALTER TABLE foo_table ALTER COLUMN created_at DROP DEFAULT;
```

### Multiple operations

```sql
ALTER TABLE foo_table ADD COLUMN phone VARCHAR, ADD COLUMN address TEXT;
```

## Drop table

```sql
DROP TABLE foo_table;
```
