# SQL Guide

VaireDB speaks the **PostgreSQL wire protocol**, so you interact with it using
standard SQL through any PostgreSQL client. SQL submitted to the coordinator is
parsed by DataFusion, translated to DuckDB's dialect, and dispatched to the
core nodes that own the relevant shards.

<div class="grid cards" markdown>

-   [:material-connection: __Connecting__](connecting.md)

    Connect with `psql` and other PostgreSQL clients.

-   [:material-table-cog: __Tables & Schema__](tables.md)

    `CREATE TABLE` with sharding, `ALTER TABLE`, `DROP TABLE`, and catalog
    introspection.

-   [:material-table-search: __Querying Data__](querying.md)

    `INSERT` and `SELECT` against sharded tables.

-   [:material-shield-lock: __Column Pseudonymization__](pseudonymization.md)

    Declare columns to be HMAC-SHA256 hashed in the coordinator for compliance.

-   [:material-script-text: __Worked Example__](examples.md)

    An end-to-end session from empty cluster to query results.

</div>

!!! note "Dialect"
    Table DDL accepts a `WITH (...)` clause for sharding options (`shards`,
    `replication_factor`, `shard_by`). Statement-level SQL is
    PostgreSQL-compatible and translated to DuckDB internally.
