# Connecting

Clients connect to the **coordinator** over the PostgreSQL wire protocol on port
`5432`. You never connect directly to core nodes.

## psql

```bash
psql -h 127.0.0.1 -p 5432 "sslmode=disable"
```

!!! note "TLS"
    v0.1 does not implement TLS (it is a [non-goal](../concepts/design-goals.md)
    for now), so pass `sslmode=disable`.

## Connection strings

Any PostgreSQL-compatible driver works. A few examples pointing at a local
cluster:

=== "URI"

    ```
    postgresql://127.0.0.1:5432/vairedb?sslmode=disable
    ```

=== "Python (psycopg)"

    ```python
    import psycopg

    with psycopg.connect("host=127.0.0.1 port=5432 sslmode=disable") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM foo_table")
            for row in cur.fetchall():
                print(row)
    ```

=== "Go (pgx)"

    ```go
    conn, err := pgx.Connect(ctx, "postgres://127.0.0.1:5432/vairedb?sslmode=disable")
    ```

=== "Node.js (node-postgres)"

    ```js
    import { Client } from "pg";

    const client = new Client({ host: "127.0.0.1", port: 5432, ssl: false });
    await client.connect();
    ```

## Compatible tooling

Because VaireDB looks like PostgreSQL on the wire, the broader ecosystem works
out of the box:

- **CLI**: `psql`, `pgcli`
- **BI / GUI**: DBeaver, Tableau, Metabase (via JDBC/ODBC)
- **ORMs**: SQLAlchemy, Django ORM, ActiveRecord, JOOQ, Diesel

See [Communication Layer](../concepts/communication-layer.md#client-protocol)
for why the PostgreSQL protocol was chosen.
