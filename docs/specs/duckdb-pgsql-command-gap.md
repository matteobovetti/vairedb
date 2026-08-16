# DuckDB vs. VaireDB (PostgreSQL wire protocol) — SQL Command Gap

Gap analysis of the SQL **statements** DuckDB documents against what VaireDB's
coordinator currently accepts over the PostgreSQL wire protocol.

- **Reference (DuckDB):** [SQL Statements overview](https://duckdb.org/docs/current/sql/statements/overview) — 35 documented entries.
- **VaireDB dispatch:** every statement is classified by
  [`classify_statement`](../../crates/vairedb-coordinator/src/query_router/query_router.rs)
  into one of 7 `QueryType`s; anything unrecognized becomes `QueryType::Other`
  and is rejected. Wire path: `pgwire_handler/handler.rs` →
  `handle_select` / `handle_dml` / `handle_create_table` / `handle_drop_table` /
  `handle_alter_table`.

## How VaireDB decides

VaireDB is a **sharded coordinator that speaks the PG wire protocol and executes
on per-shard DuckDB backends** — it is not a drop-in DuckDB. It recognizes only
the statement kinds it can shard, route, and replicate. The parser is
`sqlparser 0.58` with `PostgreSqlDialect`, so a statement can fail at two points:

| Rejection point | Error | SQLSTATE | When |
|---|---|---|---|
| Parse | `SqlSyntaxError` | `42601` | Statement doesn't parse under `PostgreSqlDialect` (most DuckDB-only syntax: `PIVOT`, `SUMMARIZE`, `ATTACH`, `INSTALL`, …). |
| Classification | `FeatureNotSupported` | `0A000` | Parses fine but isn't one of the 7 routed kinds (`SET`, `SHOW`, `BEGIN`, `CREATE VIEW`, `TRUNCATE`, `EXPLAIN`, …). See `unsupported_statement_error`, `handler.rs:125`. |

## Summary

| Support | Count | Statements |
|---|---:|---|
| ✅ Supported | 5 | SELECT, INSERT, UPDATE, DELETE, CREATE TABLE |
| 🟡 Partial | 2 | ALTER TABLE, DROP |
| ❌ Not supported | 29 | all others (see table) |

## Full statement gap table

| # | DuckDB statement | VaireDB `QueryType` | Status | Notes |
|---|---|---|---|---|
| 1 | `SELECT` | `Select` | ✅ | Full read path. Planned on DataFusion; distributed via Ballista `session_ctx`, or `local_ctx` for `pg_catalog`/`vairedb_catalog` introspection. |
| 2 | `INSERT` | `Insert` | ✅ | Requires an **explicit column list naming the shard key** with a non-NULL value per row. `INSERT … SELECT` and positional inserts are rejected (`validate_insert_shard_key`). Multi-row inserts are split per shard. |
| 3 | `UPDATE` | `Update` | ✅ | Supported **except mutating the shard-key column** (row relocation, rejected `0A000`). |
| 4 | `DELETE` | `Delete` | ✅ | Routed/broadcast to owning shards under quorum. |
| 5 | `CREATE TABLE` | `CreateTable` | ✅ | VaireDB-extended: `WITH (shards, replication_factor, shard_by, anonymized_columns)`. PG types mapped to DuckDB (`BYTEA`→`BLOB`, `JSONB`→`JSON`). `IF NOT EXISTS` honored. `CREATE TABLE AS SELECT` not validated for sharding. |
| 6 | `ALTER TABLE` | `AlterTable` | 🟡 | Only column ops: ADD / DROP / RENAME COLUMN, ALTER COLUMN {SET DATA TYPE, SET/DROP NOT NULL, SET/DROP DEFAULT}. Other ops (constraints, rename table, …) → `0A000`. Cannot drop the shard key or touch anonymized columns. |
| 7 | `DROP` | `DropTable` | 🟡 | Only `DROP TABLE` is meaningful — the catalog only knows tables. `DROP VIEW/INDEX/SCHEMA/SEQUENCE/…` reach the same handler and effectively fail (no such object). `IF EXISTS` honored for tables. |
| 8 | `ALTER VIEW` | `Other` | ❌ | Views unsupported. |
| 9 | `ANALYZE` | `Other` | ❌ | No planner statistics surface. |
| 10 | `ATTACH` / `DETACH` | `Other` | ❌ | Single-node DuckDB concept; not applicable to the sharded model. Likely `42601` (parse). |
| 11 | `CALL` | `Other` | ❌ | No stored/table procedures. |
| 12 | `CHECKPOINT` | `Other` | ❌ | Per-shard storage concern, not coordinator-exposed. |
| 13 | `COMMENT ON` | `Other` | ❌ | No catalog comment storage. |
| 14 | `COPY` | `Other` | ❌ | **High-value gap** — bulk import/export. `copy_handler` is a `NoopHandler`. Aligns with the roadmap's "massive data import SQL command". |
| 15 | `CREATE INDEX` | `Other` | ❌ | No distributed index management. |
| 16 | `CREATE MACRO` | `Other` | ❌ | No macro support. |
| 17 | `CREATE SCHEMA` | `Other` | ❌ | Coordinator namespace is flat (`schema.tbl` collapsed to bare name). |
| 18 | `CREATE SECRET` | `Other` | ❌ | DuckDB secrets manager. Note VaireDB has its **own** anonymization-secret mechanism via `INSERT INTO vairedb_catalog.anonymization_secret`. |
| 19 | `CREATE SEQUENCE` | `Other` | ❌ | No distributed sequences. |
| 20 | `CREATE VIEW` | `Other` | ❌ | Explicitly labeled unsupported (`unsupported_statement_label`). Common ORM/BI need. |
| 21 | `CREATE TYPE` | `Other` | ❌ | No custom/enum types. |
| 22 | `DESCRIBE` | `Other` | ❌ | Introspection instead flows through emulated `pg_catalog` SELECTs. |
| 23 | `EXPORT` / `IMPORT DATABASE` | `Other` | ❌ | Whole-DB dump/load; not applicable to sharded model. |
| 24 | `INSTALL` / `LOAD` | `Other` | ❌ | Extension management is a per-node concern. Likely `42601`. |
| 25 | `MERGE INTO` | `Other` | ❌ | Upsert/merge unsupported; would need shard-key-aware routing. Likely `42601`. |
| 26 | `PIVOT` | `Other` | ❌ | DuckDB-only syntax; likely `42601`. |
| 27 | Profiling (`PRAGMA` / `EXPLAIN ANALYZE`) | `Other` | ❌ | `EXPLAIN` explicitly labeled unsupported. |
| 28 | `RESET` | `Other`/`Set` | ❌ | Session config not modeled. |
| 29 | `SET` | `Set` | ❌ | Explicitly labeled unsupported. Some drivers issue `SET` on connect — a **compatibility risk**. |
| 30 | `SET VARIABLE` | `Other` | ❌ | DuckDB variables unsupported. |
| 31 | `SHOW` / `SHOW DATABASES` | `ShowVariable` | ❌ | Explicitly labeled unsupported. |
| 32 | `SUMMARIZE` | `Other` | ❌ | DuckDB-only; likely `42601`. |
| 33 | Transaction management (`BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`) | `StartTransaction`/`Commit`/`Rollback`/`Savepoint` | ❌ | Explicitly labeled unsupported. **Major client-compatibility gap** — many drivers/ORMs wrap statements in transactions by default. |
| 34 | `UNPIVOT` | `Other` | ❌ | DuckDB-only; likely `42601`. |
| 35 | `USE` | `Other` | ❌ | No database/schema switching (flat namespace). |
| 36 | `VACUUM` | `Other` | ❌ | Per-shard storage maintenance, not coordinator-exposed. |

> The DuckDB overview page counts 34 entries; the table expands a few combined
> entries (`ATTACH`/`DETACH`, `INSTALL`/`LOAD`, `SET`/`RESET`, `SHOW`/`SHOW
> DATABASES`, transaction management) into their sub-commands, hence 36 rows.

## Prioritized gaps (wire-protocol impact)

Ranked by how often real PG clients/ORMs need them, independent of DuckDB parity:

1. **Transaction control (`BEGIN`/`COMMIT`/`ROLLBACK`)** — most PG drivers open a
   transaction implicitly. Even a single-statement / auto-commit emulation would
   unblock many clients.
2. **`SET` / `RESET` / `SHOW`** — drivers send `SET` (e.g. `client_encoding`,
   `search_path`) at connect time; silently accepting no-op-safe SETs would
   improve compatibility.
3. **`COPY`** — bulk ingest/export; already on the roadmap ("massive data import
   SQL command"). `copy_handler` currently a `NoopHandler`.
4. **`CREATE VIEW` / `ALTER VIEW`** — common for BI/reporting layers.
5. **`EXPLAIN`** — widely used by tooling and humans for query inspection.
6. **`MERGE INTO` / upsert** — needs shard-key-aware routing but is a frequent
   ETL need.

Statements that are **intentionally out of scope** (single-node DuckDB concerns
that don't map to a sharded coordinator): `ATTACH`/`DETACH`, `INSTALL`/`LOAD`,
`CHECKPOINT`, `VACUUM`, `EXPORT`/`IMPORT DATABASE`, `USE`, `CREATE SECRET`
(superseded by VaireDB's own anonymization-secret path).
