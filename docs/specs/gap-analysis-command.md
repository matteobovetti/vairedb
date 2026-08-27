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
| Parse | `SqlSyntaxError` | `42601` | Statement doesn't parse under `PostgreSqlDialect` (most DuckDB-only syntax: `PIVOT`, `SUMMARIZE`, `INSTALL`, …), **plus** `RESET`, `VACUUM`, `CHECKPOINT` and `ALTER VIEW/SCHEMA/SEQUENCE`, which sqlparser omits even though PostgreSQL has them. |
| Classification | `FeatureNotSupported` | `0A000` | Parses fine but isn't one of the 7 routed kinds (`SET`, `SHOW`, `BEGIN`, `CREATE VIEW`, `TRUNCATE`, `EXPLAIN`, `MERGE`, …). See `unsupported_statement_error`, `handler.rs:125`. |

**Which point a statement hits decides the cost of closing it.** A `0A000` row parses
already, so it only needs routing/execution. A `42601` row needs the parser taught
first — either a sqlparser upgrade/dialect change or a pre-parse rewrite — before any
distributed work starts. The `Rejected at` column below records the point **observed
against the 5-node e2e cluster**, not a guess.

## Executable counterpart

Every row of this document is mapped to an end-to-end test under `tests/e2e/tests/`:

| File | Rows | Contents |
|---|---|---|
| `sql_command_select.rs` | 1 | The read path's operator surface. |
| `sql_command_dml.rs` | 2–4, 25 | INSERT/UPDATE/DELETE + MERGE/upsert. |
| `sql_command_ddl.rs` | 5–7 | The 🟡 gap surface of CREATE/ALTER/DROP TABLE. |
| `sql_command_unsupported.rs` | 8–36 | Every ❌ statement. |

Each gap has up to two tests: a **passing** `*_currently_rejected` test pinning that
today's rejection is honest (a real SQLSTATE, never a fake `OK` for work that never
happened), and an `#[ignore = "gap (row N): …"]` test asserting the
PostgreSQL-correct target behavior. The ignored tests fail by construction and are
the definition of done — un-ignore one as its gap closes:

```sh
cd tests/e2e && cargo test --test sql_command_unsupported -- --ignored --test-threads=1
```

`make e2e` runs only the passing set, so the gap map never blocks CI.

## Summary

| Support | Count | Statements |
|---|---:|---|
| ✅ Supported | 5 | SELECT, INSERT, UPDATE, DELETE, CREATE TABLE |
| 🟡 Partial | 2 | ALTER TABLE, DROP |
| ❌ Not supported | 29 | all others (see table) |

Of the 29 ❌ rows, counted by their headline statement: **17 fail at classification**
(`0A000` — the parser is fine, only routing is missing), **10 fail at parse** (`42601` —
the parser must be taught the statement first), and **2 are split across both points**
(`ATTACH`/`DETACH`, `INSTALL`/`LOAD`). Some rows whose `CREATE` form is `0A000` have an
`ALTER` form that is `42601` (schemas, sequences, views).

### Confirmed behaviors that this table used to get wrong

Found while building the executable counterpart, verified against the e2e cluster:

- **`DROP VIEW <table>` silently drops the TABLE.** Every `DROP <kind>` is classified
  `DropTable`, so `DROP VIEW`/`INDEX`/`SEQUENCE`/`SCHEMA` naming an existing table
  reports success and destroys it (PostgreSQL would refuse with `42809` wrong object
  type). Naming something that isn't a table gives `42P01 table "x" does not exist` —
  wrong noun, but harmless. **This is the highest-consequence entry in the whole map:
  a client typo on an object kind is unrecoverable data loss.**
  Guard: `sql_command_ddl.rs::test_drop_view_must_not_drop_a_table`.
- **Upsert is NOT a gap — `INSERT … ON CONFLICT DO UPDATE` works today**, provided the
  arbiter column is declared `PRIMARY KEY` in the `CREATE TABLE`: the declaration
  reaches the per-shard DuckDB tables and supplies the arbiter index. When the arbiter
  is the shard key this is globally correct, because equal keys always hash to the same
  shard. Without a declared PK the shard returns `[VDB-2001] … not referenced by a
  UNIQUE/PRIMARY KEY CONSTRAINT or INDEX`. The constraint must exist at `CREATE` time —
  `ALTER TABLE … ADD CONSTRAINT` (row 6) and `CREATE UNIQUE INDEX` (row 15) are both
  rejected — and an arbiter on a **non**-shard-key column would only be enforced per
  shard, so it must not be relied on for global uniqueness.
- **`CREATE TABLE AS SELECT` doesn't merely skip shard validation — it fails.** CTAS
  classifies as `CreateTable` and *is* broadcast, then dies with `08006 [VDB-3007] DDL
  broadcast to node core-1 failed`; no shard table is created and no rows are
  materialized.
- **`MERGE INTO` parses.** sqlparser 0.58 accepts it, so it is rejected at
  classification (`0A000`), not at parse — closing it is routing work only.

## Full statement gap table

`Rejected at` is the SQLSTATE observed against the e2e cluster; `—` means the
statement is accepted.

| # | DuckDB statement | VaireDB `QueryType` | Status | Rejected at | Notes |
|---|---|---|---|---|---|
| 1 | `SELECT` | `Select` | ✅ | — | Full read path. Planned on DataFusion; distributed via Ballista `session_ctx`, or `local_ctx` for `pg_catalog`/`vairedb_catalog` introspection. |
| 2 | `INSERT` | `Insert` | ✅ | — | Requires an **explicit column list naming the shard key** with a non-NULL value per row. `INSERT … SELECT` and positional inserts are rejected `0A000` (`validate_insert_shard_key`). Multi-row inserts are split per shard. `ON CONFLICT DO UPDATE` works when the arbiter column was declared `PRIMARY KEY` at `CREATE` time — see the summary. |
| 3 | `UPDATE` | `Update` | ✅ | — | Supported **except mutating the shard-key column** (row relocation, rejected `0A000`). |
| 4 | `DELETE` | `Delete` | ✅ | — | Routed/broadcast to owning shards under quorum. |
| 5 | `CREATE TABLE` | `CreateTable` | ✅ | — | VaireDB-extended: `WITH (shards, replication_factor, shard_by, anonymized_columns)`. PG types mapped to DuckDB (`BYTEA`→`BLOB`, `JSONB`→`JSON`). `IF NOT EXISTS` honored. **`CREATE TABLE AS SELECT` fails** `08006` at the shard DDL broadcast (no column list ⇒ shard key falls back to `"id"`). |
| 6 | `ALTER TABLE` | `AlterTable` | 🟡 | `0A000` for other ops | Only column ops: ADD / DROP / RENAME COLUMN, ALTER COLUMN {SET DATA TYPE, SET/DROP NOT NULL, SET/DROP DEFAULT}. Constraints, `RENAME TO`, … → `0A000`. Cannot drop the shard key or touch anonymized columns (both intentional). |
| 7 | `DROP` | `DropTable` | 🟡 | `42P01` if no such table | Only `DROP TABLE` is meaningful — the catalog only knows tables. `DROP VIEW/INDEX/SCHEMA/SEQUENCE` reach the same handler: missing object ⇒ `42P01 table "x" does not exist`; **existing table of that name ⇒ the table is dropped** (see summary). `IF EXISTS` honored for tables. |
| 8 | `ALTER VIEW` | `Other` | ❌ | `42601` | Views unsupported. sqlparser only accepts `ALTER VIEW … AS <query>`, so `RENAME TO` fails at parse. |
| 9 | `ANALYZE` | `Other` | ❌ | `0A000` | No planner statistics surface. |
| 10 | `ATTACH` / `DETACH` | `Other` | ❌ | `0A000` / `42601` | Single-node DuckDB concept; not applicable to the sharded model. `ATTACH` parses, `DETACH` does not. |
| 11 | `CALL` | `Other` | ❌ | `0A000` | No stored/table procedures. |
| 12 | `CHECKPOINT` | `Other` | ❌ | `42601` | Per-shard storage concern, not coordinator-exposed. |
| 13 | `COMMENT ON` | `Other` | ❌ | `0A000` | No catalog comment storage. |
| 14 | `COPY` | `Other` | ❌ | `0A000` | **High-value gap** — bulk import/export. `copy_handler` is a `NoopHandler`. Aligns with the roadmap's "massive data import SQL command". |
| 15 | `CREATE INDEX` | `Other` | ❌ | `0A000` | No distributed index management. A `UNIQUE` index **on the shard key** is the one uniqueness constraint enforceable without cross-shard coordination. |
| 16 | `CREATE MACRO` | `Other` | ❌ | `42601` | No macro support; DuckDB-only syntax. |
| 17 | `CREATE SCHEMA` | `Other` | ❌ | `0A000` (`ALTER SCHEMA`: `42601`) | Coordinator namespace is flat (`schema.tbl` collapsed to bare name), so two same-named tables in different schemas would collide on one catalog key. |
| 18 | `CREATE SECRET` | `Other` | ❌ | `0A000` | DuckDB secrets manager. Note VaireDB has its **own** anonymization-secret mechanism via `INSERT INTO vairedb_catalog.anonymization_secret`. |
| 19 | `CREATE SEQUENCE` | `Other` | ❌ | `0A000` (`ALTER SEQUENCE`: `42601`) | No distributed sequences. Also the blocker behind the `SERIAL` gap in the data-type analysis; `nextval()` in the shard-key position additionally needs routing of a non-literal key. |
| 20 | `CREATE VIEW` | `Other` | ❌ | `0A000` | Explicitly labeled unsupported (`unsupported_statement_label`); `CREATE OR REPLACE VIEW` likewise. Common ORM/BI need. |
| 21 | `CREATE TYPE` | `Other` | ❌ | `0A000` | No custom/enum types. |
| 22 | `DESCRIBE` | `Other` | ❌ | `0A000` | Introspection instead flows through emulated `pg_catalog` SELECTs. Shares the `EXPLAIN` label. |
| 23 | `EXPORT` / `IMPORT DATABASE` | `Other` | ❌ | `42601` | Whole-DB dump/load; not applicable to sharded model. |
| 24 | `INSTALL` / `LOAD` | `Other` | ❌ | `42601` / `0A000` | Extension management is a per-node concern. `INSTALL` fails at parse, `LOAD` parses. |
| 25 | `MERGE INTO` | `Other` | ❌ | `0A000` | Needs shard-key-aware routing of the matched/unmatched branches. **Parses fine** under sqlparser 0.58, so this is routing work only. The `INSERT … ON CONFLICT` spelling of upsert already works (see summary). |
| 26 | `PIVOT` | `Other` | ❌ | `42601` | DuckDB-only syntax. |
| 27 | Profiling (`PRAGMA` / `EXPLAIN ANALYZE`) | `Other` | ❌ | `0A000` | `EXPLAIN` explicitly labeled unsupported, although SELECTs already build a DataFusion `LogicalPlan` that could be rendered. |
| 28 | `RESET` | `Other`/`Set` | ❌ | `42601` | Session config not modeled — **and** sqlparser's `PostgreSqlDialect` has no `RESET`, so this needs a parser fix too, unlike `SET`/`SHOW`. |
| 29 | `SET` | `Set` | ❌ | `0A000` | Explicitly labeled unsupported. Drivers issue `SET` (`client_encoding`, `application_name`, `extra_float_digits`, …) on connect — a **compatibility risk** that can break a client before its first query. |
| 30 | `SET VARIABLE` | `Other` | ❌ | `42601` | DuckDB variables unsupported. |
| 31 | `SHOW` / `SHOW DATABASES` | `ShowVariable` | ❌ | `0A000` | Explicitly labeled unsupported; `SHOW ALL` and `SHOW TABLES` likewise. |
| 32 | `SUMMARIZE` | `Other` | ❌ | `42601` | DuckDB-only. |
| 33 | Transaction management (`BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`) | `StartTransaction`/`Commit`/`Rollback`/`Savepoint` | ❌ | `0A000` | Explicitly labeled unsupported. **Major client-compatibility gap** — many drivers/ORMs wrap statements in transactions by default. Note auto-commit emulation can fake `BEGIN`/`COMMIT` but not `ROLLBACK`. |
| 34 | `UNPIVOT` | `Other` | ❌ | `42601` | DuckDB-only. |
| 35 | `USE` | `Other` | ❌ | `0A000` | No database/schema switching (flat namespace). |
| 36 | `VACUUM` | `Other` | ❌ | `42601` | Per-shard storage maintenance, not coordinator-exposed. sqlparser has no `VACUUM` either. ⚠️ In-scope status unresolved — see *Open question* below. |

`TRUNCATE` is not in DuckDB's overview but is a standard PG statement clients do send:
it is rejected `0A000`, and its rows survive (`sql_command_unsupported.rs` section 12).

> The DuckDB overview page counts 34 entries; the table expands a few combined
> entries (`ATTACH`/`DETACH`, `INSTALL`/`LOAD`, `SET`/`RESET`, `SHOW`/`SHOW
> DATABASES`, transaction management) into their sub-commands, hence 36 rows.

## Prioritized gaps (wire-protocol impact)

Ranked by how often real PG clients/ORMs need them, independent of DuckDB parity.
The parenthetical marks the rejection point, i.e. whether the parser also has to
change: **(routing)** = `0A000`, parses today; **(parser + routing)** = `42601`.

0. **`DROP <non-table>` data loss** — not a feature gap but a correctness bug, and it
   outranks everything below: `DROP VIEW t` on a table drops the table. Fixing it means
   checking the object kind in `handle_drop_table` and returning `42809`. Cheap,
   independent of every item below, and prevents unrecoverable data loss.
1. **Transaction control (`BEGIN`/`COMMIT`/`ROLLBACK`)** *(routing)* — most PG drivers
   open a transaction implicitly. Even a single-statement / auto-commit emulation would
   unblock many clients, though it cannot honor `ROLLBACK`.
2. **`SET` / `SHOW`** *(routing)* / **`RESET`** *(parser + routing)* — drivers send
   `SET` (e.g. `client_encoding`, `search_path`) at connect time; silently accepting
   no-op-safe SETs would improve compatibility. `SET`/`SHOW` are the cheap half.
3. **`COPY`** *(routing)* — bulk ingest/export; already on the roadmap ("massive data
   import SQL command"). `copy_handler` currently a `NoopHandler`. Export must gather
   from every shard; import must route each row by its shard key.
4. **`CREATE VIEW` / `DROP VIEW`** *(routing)* / **`ALTER VIEW`** *(parser + routing)* —
   common for BI/reporting layers. Requires modelling a non-table relation in the
   catalog, which is also what unblocks item 0's object-kind check.
5. **`EXPLAIN` and `DESCRIBE`** *(routing)* — widely used by tooling and humans for
   query inspection and schema exploration. SELECTs already build a DataFusion
   `LogicalPlan`, so `EXPLAIN` is mostly a rendering path.
6. **`MERGE INTO`** *(routing)* — needs shard-key-aware routing of the matched and
   unmatched branches. Scope reduced: the `INSERT … ON CONFLICT` half of this need
   already works on a declared `PRIMARY KEY` (see summary), so this is only about the
   `MERGE` spelling and multi-branch merges.
7. **`INDEX`** *(routing)* — used for optimizing query performance. A `UNIQUE` index on
   the shard key is also the only global uniqueness constraint currently expressible
   at all, and today it can only be declared at `CREATE TABLE` time.
8. **`CREATE SCHEMA` / `DROP SCHEMA`** *(routing)* / **`ALTER SCHEMA`**
   *(parser + routing)* — schema creation and management. Blocked on the flat namespace:
   `canonical_table_name` keeps only the last identifier part.
9. **`CREATE SEQUENCE` / `DROP SEQUENCE`** *(routing)* / **`ALTER SEQUENCE`**
   *(parser + routing)* — distributed sequence management; also the blocker behind
   `SERIAL`.
10. **`VACUUM`** *(parser + routing)* — distributed vacuum management. ⚠️ In-scope status
    unresolved — see *Open question* below.
11. **`PIVOT` / `UNPIVOT`** *(parser + routing)* — reshaping data. DuckDB-only syntax,
    so support starts with teaching the parser the statement.

Statements that are **intentionally out of scope** (single-node DuckDB concerns
that don't map to a sharded coordinator): `ATTACH`/`DETACH`, `INSTALL`/`LOAD`,
`CHECKPOINT`, `EXPORT`/`IMPORT DATABASE`, `USE`, `CREATE SECRET` (superseded by
VaireDB's own anonymization-secret path). These must **stay** rejected — accepting one
would mean it silently ran on one arbitrary node; that is guarded by
`sql_command_unsupported.rs::test_out_of_scope_statements_stay_rejected`.

### Open question: is `VACUUM` in scope?

`VACUUM` appears **both** as prioritized gap #10 ("distributed vacuum management") and,
previously, in the out-of-scope list — the two are mutually exclusive. It has been
removed from the out-of-scope list above so that the ranked list and the test suite
agree, and `sql_command_unsupported.rs` keeps an `#[ignore]`d target-state test for it.
If the decision goes the other way, drop that xfail, delete item 10, and restore
`VACUUM` to the out-of-scope list; the passing rejection test stays either way.
