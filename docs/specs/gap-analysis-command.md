# SQL Command Gap — DuckDB vs. VaireDB (PostgreSQL wire protocol)

Which SQL **statements** VaireDB's coordinator accepts over the PostgreSQL wire
protocol, measured against the statements DuckDB documents.

- **Reference:** DuckDB's [SQL Statements overview](https://duckdb.org/docs/current/sql/statements/overview) — 35 documented entries. The row numbers below are that list's, and the tests cite them, so they stay fixed even where a table groups rows by theme rather than by number. A few combined entries are split into their sub-commands, hence 36 rows.
- **Siblings:** `gap-analysis-data-type.md`, `gap-analysis-operator-literal.md`, `gap-analysis-aggregate-function.md`, `gap-analysis-window-function.md`.
- **Executable counterpart:** every row maps to an end-to-end test — see [Tests](#tests).

VaireDB is a sharded coordinator that speaks the PG wire protocol and executes on
per-shard DuckDB backends; it is not a drop-in DuckDB. It recognizes only the statement
kinds it can shard, route and replicate: `classify_statement`
([`pgwire_handler/query_router.rs`](../../crates/vairedb-coordinator/src/pgwire_handler/query_router.rs))
sorts a statement into one of 18 `QueryType`s, and anything else becomes
`QueryType::Other` and is refused.

## Legend

| Status | Meaning |
|---|---|
| ✅ | Supported. |
| 🟡 | Supported, with restrictions that follow from sharding. Each is listed in the row. |
| 🚫 | **Not planned** — a decided limitation. The deliverable is a rejection that explains itself. |
| ❌ | Refused today. In [Read path and session state](#read-path-and-session-state--next) that means *not yet*; in [Not planned, or out of scope](#not-planned-or-out-of-scope) it means *refused, and not on the roadmap*. |

A refused statement fails at one of two points, and which one decides the cost of closing
it:

| Rejection point | Error | SQLSTATE | When |
|---|---|---|---|
| Parse | `SqlSyntaxError` | `42601` | Does not parse under sqlparser's `PostgreSqlDialect` (most DuckDB-only syntax), **plus** `RESET`, `VACUUM`, `CHECKPOINT`, `ALTER SCHEMA`, `ALTER SEQUENCE` and `ALTER VIEW … RENAME TO`, which sqlparser omits although PostgreSQL has them. Closing such a row needs the parser taught first. |
| Classification | `FeatureNotSupported` | `0A000` | Parses, but is not one of the routed kinds. Only routing/execution is missing. |

The `SQLSTATE` column in the tables records what was **observed against the 5-node e2e
cluster**, not a guess. Every classification refusal names the command it refused, so a
client sending a batch can tell which statement was rejected.

## Summary

| Status | Count | Statements |
|---|---:|---|
| ✅ | 5 | SELECT, INSERT, UPDATE, DELETE, CREATE TABLE |
| 🟡 | 9 | ALTER TABLE, DROP, ALTER VIEW, COPY, CREATE INDEX, CREATE SCHEMA, CREATE VIEW, MERGE INTO, transaction management |
| 🚫 | 2 | CREATE SEQUENCE, CREATE TYPE |
| ❌ | 20 | all others |

Of the 20 ❌ rows: **9 fail at classification**, **9 fail at parse**, and **2 are split
across both points** (`ATTACH`/`DETACH`, `INSTALL`/`LOAD`).

## Rules that apply to every row

These are the invariants the write path is built on; the row notes below only record
where a statement deviates.

- **A statement is either correct on every shard or refused.** No statement is accepted
  and silently under-applied, and no refusal is a fake `OK` for work that never happened.
- **A write must have a determinable shard key before it leaves the coordinator**, because
  the coordinator hashes it to pick the shard. A key that is a computed expression is
  refused rather than guessed. `UPDATE`/`DELETE` predicates broadcast instead, since every
  shard re-evaluates the `WHERE` clause.
- **Uniqueness is only enforceable when it includes the shard key.** Equal shard keys share
  a shard, so a per-shard unique index or constraint then sees every row that could
  collide. Off the shard key it is refused, in every spelling (`UNIQUE` index, `UNIQUE` /
  `PRIMARY KEY` constraint, `ON CONFLICT` arbiter).
- **There is no cross-shard atomicity yet (no 2PC).** A transaction block on one node set
  is all-or-nothing; one spanning node sets is refused at `COMMIT` unless
  `allow_cross_shard_transactions` is set. A multi-shard write that fails part-way reports
  `40003` and how much was written — never a rollback that did not happen. Statements whose
  retry converges (`TRUNCATE`, index and constraint DDL) are shipped with
  `IF (NOT) EXISTS` so re-running finishes the job.
- **DDL is refused inside a transaction block**, because it reaches the catalog and the
  shards immediately and `ROLLBACK` could not undo it.
- **Every object kind resolves through its own namespace**, so no statement can destroy an
  object of another kind: the wrong kind is `42809` naming the statement to use instead, a
  name that resolves to nothing is `42P01`. Tables, views, indexes and index-backed
  constraints share **one relation namespace**, claimed atomically.
- **A schema qualifier is part of the catalog key** (`sales.t`; a relation in the default
  schema keys as the bare `t`) and is folded into the physical per-shard name
  (`sales_t_shard2`). There is no `search_path`: an unqualified name always means the
  default schema.
- **Decorations that only affect performance are stripped**; anything that would change
  results is refused by name rather than accepted and ignored.

## Write path and DDL — closed

The statements that route to shards, plus the DDL that is coordinator-local (views and
schemas reach no shard). This surface is complete; what stays refused in a 🟡 row is
refused by design, not waiting for a turn.

| # | Statement | `QueryType` | Status | SQLSTATE | Notes |
|---|---|---|---|:-:|---|
| 2 | `INSERT` | `Insert` | ✅ | — | Needs the shard key present with a non-NULL, determinable value per row; positional rows are resolved against the declared column order first. Multi-row inserts are split per shard. Any query source (`SELECT`, `UNION`, CTE) is materialized and re-emitted as literal `VALUES`, so it routes like a hand-written insert; `RETURNING` on that form is refused. `ON CONFLICT DO UPDATE` works when the arbiter includes the shard key and is backed by a `PRIMARY KEY`/`UNIQUE` index. |
| 3 | `UPDATE` | `Update` | ✅ | — | Except mutating the shard-key column: row relocation is refused (`0A000`). |
| 4 | `DELETE` | `Delete` | ✅ | — | Routed to the owning shards, or broadcast when the predicate's shard key is not a literal. |
| 5 | `CREATE TABLE` | `CreateTable` | ✅ | — | VaireDB-extended: `WITH (shards, replication_factor, shard_by, anonymized_columns)`. PG types mapped to DuckDB. The name is claimed atomically. Declared constraints are recorded and reach every shard: `CHECK` always, `UNIQUE`/`PRIMARY KEY` only over the shard key, `FOREIGN KEY` never (unenforceable across shards). `AS SELECT` is supported when it states `WITH (shard_by = …)` **before** the query; `LIKE`/`CLONE` supply no column list and are refused. |
| 6 | `ALTER TABLE` | `AlterTable` | 🟡 | `0A000` | Column ops (ADD/DROP/RENAME COLUMN, ALTER COLUMN type/nullability/default), `RENAME TO`, and `ADD`/`DROP CONSTRAINT … UNIQUE (<shard key>)`. `RENAME TO` re-keys the catalog and renames every `{table}_shard{n}`; it stays inside the table's schema and must be the only action (`42601`). Cannot drop the shard key or touch anonymized columns. While the table carries an index or index-backed constraint, only adding a column and changing a default are possible — the engine's dependency is on the table. A column a declared constraint covers cannot be dropped or retyped. Every other `ADD`/`DROP CONSTRAINT` form is refused by name: the shards' engine implements neither `ADD … CHECK` nor `DROP CONSTRAINT`. |
| 7 | `DROP` | `DropTable` / `DropIndex` / `DropView` / `DropSchema` | 🟡 | `0A000` / `42P01` | The four kinds the catalog knows, each through its own namespace. `IF EXISTS` honored. Several names in one statement, and `CASCADE`/`RESTRICT`, are refused rather than partly applied. `DROP SEQUENCE` reaches the table handler and reports as a missing table. |
| 8 | `ALTER VIEW` | `AlterView` | 🟡 | `42601` / `42P01` | `AS <query>` redefines a view in place, validated like a `CREATE`, and refuses a name no view holds. `RENAME TO` fails at parse. |
| 14 | `COPY` | `Copy` | 🟡 | `0A000` | Bulk import/export over a **CSV file on the coordinator**: `TO` gathers from every shard, `FROM` routes each row by its shard key. `FORMAT CSV` must be stated (PG's default is `TEXT`); `HEADER`/`DELIMITER`/`QUOTE` are honored and any other option is refused by name. Refused: `FROM STDIN` / `TO STDOUT` (the streaming sub-protocol), `PROGRAM`, non-CSV formats. ⚠️ No privilege check exists — VaireDB has no role model, so any client reads and writes any path the coordinator process can. |
| 15 | `CREATE INDEX` / `DROP INDEX` | `CreateIndex` / `DropIndex` | 🟡 | `0A000` | One real index per shard, on every replica, recorded on its table. `UNIQUE` over the shard key doubles as an `ON CONFLICT` arbiter, so an existing table can gain upsert. An index is created in its table's schema, so `CREATE INDEX` takes a bare name and `DROP INDEX` the qualified one. Refused: `UNIQUE` off the shard key, a `UNIQUE` index narrowed by `WHERE` or `NULLS NOT DISTINCT`, an unnamed index (nothing for `DROP` to resolve), an index over an expression. |
| 17 | `CREATE SCHEMA` / `DROP SCHEMA` | `CreateSchema` / `DropSchema` | 🟡 | `0A000` | **Coordinator-local**: a namespace in the catalog, nothing broadcast. A taken name is `42P06`, a missing schema `3F000`, a non-empty `DROP SCHEMA` `2BP01` (RESTRICT-only). Because the physical name folds the qualifier in, `sales.t` and `sales_t` cannot both exist — the second is `42P07` naming the owner — and a `.` inside a quoted name still reads as a qualifier. Refused: `CASCADE` (it would drop tables as a side effect of a namespace statement), `AUTHORIZATION` (no role model), dropping `public` or a metadata schema. `ALTER SCHEMA` fails at parse. |
| 20 | `CREATE VIEW` | `CreateView` | 🟡 | `0A000` | **Coordinator-local**: the query text is stored and inlined as a CTE ahead of every query naming the view, so it is planned fresh on each read, never stale, and gets the shard fan-out the query would. The definition is validated at create time. `CREATE OR REPLACE`, `IF NOT EXISTS`, a column list and a view over a view are honored; a client's own CTE of the same name wins. A view is read-only (writes are `42809`). Refused: `MATERIALIZED` (nothing is stored here), the dialect decorations (`TEMPORARY`, `WITH (…)`, `SECURE`, `CLUSTER BY`, `TO`, `COMMENT`, a typed column), a view over a metadata schema or named after a `pg_catalog` table, and a cyclic definition. |
| 25 | `MERGE INTO` | `Merge` | 🟡 | `0A000` | Applied shard by shard, with all four `WHEN` clause kinds, their `AND` conditions and `UPDATE`/`INSERT`/`DELETE` actions honored as written. Accepted when `ON` equates the target's **shard key** with a source column and the source is either a co-located table sharded by that column (same shard count, same nodes) or an inline `VALUES` list, which is split per shard. Refused: any other source, an `ON` that does not pin the shard key (each shard would decide "not matched" on partial information and insert a duplicate), `UPDATE SET <shard key>`, an `INSERT` that omits or contradicts the shard key, `RETURNING`, `NOT MATCHED BY SOURCE` over a `VALUES` source, per-action `WHERE` predicates, a merge into a table with anonymized columns. |
| 33 | Transactions (`BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`) | `TransactionControl` | 🟡 | `0A000` | All of `BEGIN`/`START TRANSACTION`, `COMMIT`/`END`, `ROLLBACK`/`ABORT`, `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `RELEASE`, on both protocols — so a driver that opens a transaction implicitly works. The writes are **buffered in the coordinator** and shipped at `COMMIT` as one atomic batch per node set, which makes `ROLLBACK` exact and savepoints positions in that buffer. Buffering is also the constraint: only statements the coordinator can answer truthfully **without running them** are allowed inside a block, so `UPDATE`/`DELETE` (unknowable row count), DDL, and reads of a table the block has written are refused, `READ ONLY` is honored (`25006`), and a failed block stays failed (`25P02`). Isolation levels are accepted and ignored — the node-local DuckDB transaction's level governs. |
| — | `TRUNCATE` | `TruncateTable` | 🟡 | `0A000` | Not in DuckDB's list, but standard PG that clients send. Every replica of every shard is emptied; the table, its shard layout and its schema survive. `ONLY` and a trailing `*` are accepted (nothing inherits here). Refused: several tables in one statement, `CASCADE`/`RESTRICT`, `RESTART IDENTITY`. |

## Read path and session state — next

Planned on DataFusion, or held in the connection's session state. None of them touch
shard routing, which is why they are a separate body of work.

| # | Statement | `QueryType` | Status | SQLSTATE | Notes |
|---|---|---|---|:-:|---|
| 1 | `SELECT` | `Select` | ✅ | — | Full read path, distributed via Ballista; `pg_catalog`/`vairedb_catalog` introspection is answered from a local context. Its operator, type and function coverage is the subject of the sibling gap docs. |
| 29 | `SET` | `Other` | ❌ | `0A000` | **The compatibility risk on this list.** Drivers issue `SET` (`client_encoding`, `application_name`, `extra_float_digits`, …) on connect, so a refusal can break a client before its first query. Note `search_path` is *not* a no-op-safe parameter now that schemas exist: it has to be honored in name resolution or refused. |
| 31 | `SHOW` / `SHOW DATABASES` | `Other` | ❌ | `0A000` | The read half of the same gap; `SHOW ALL` and `SHOW TABLES` too. |
| 28 | `RESET` | `Other` | ❌ | `42601` | Same gap, but sqlparser has no `RESET`, so the parser has to be taught it first. |
| 27 | `EXPLAIN` / `PRAGMA` / `EXPLAIN ANALYZE` | `Other` | ❌ | `0A000` | A `SELECT` already builds a DataFusion `LogicalPlan`, so `EXPLAIN` is mostly a rendering path. |
| 22 | `DESCRIBE` | `Other` | ❌ | `0A000` | Schema exploration; introspection currently flows through emulated `pg_catalog` SELECTs. |
| 9 | `ANALYZE` | `Other` | ❌ | `0A000` | No planner-statistics surface. |
| 26 / 34 | `PIVOT` / `UNPIVOT` | `Other` | ❌ | `42601` | DuckDB-only syntax, so support starts at the parser. |
| 32 | `SUMMARIZE` | `Other` | ❌ | `42601` | DuckDB-only. |
| 30 | `SET VARIABLE` | `Other` | ❌ | `42601` | DuckDB variables. |

## Not planned, or out of scope

| # | Statement | Status | SQLSTATE | Why |
|---|---|:-:|:-:|---|
| 19 | `CREATE SEQUENCE` / `nextval()` / `SERIAL` | 🚫 | `0A000` (`ALTER SEQUENCE`: `42601`) | See [no sequences](#no-sequences). |
| 21 | `CREATE TYPE` / `CREATE DOMAIN` / `ALTER TYPE` | 🚫 | `0A000` | See [no user-defined types](#no-user-defined-types). |
| 10 | `ATTACH` / `DETACH` | ❌ | `0A000` / `42601` | Single-node DuckDB attachment; meaningless for a sharded cluster. |
| 12 | `CHECKPOINT` | ❌ | `42601` | Per-shard storage concern, not coordinator-exposed. |
| 23 | `EXPORT` / `IMPORT DATABASE` | ❌ | `42601` | Whole-DB dump/load. Use `COPY` (row 14). |
| 24 | `INSTALL` / `LOAD` | ❌ | `42601` / `0A000` | Extension management is a per-node concern. |
| 18 | `CREATE SECRET` | ❌ | `0A000` | DuckDB's secrets manager; VaireDB has its own anonymization-secret path (`INSERT INTO vairedb_catalog.anonymization_secret`). |
| 35 | `USE` | ❌ | `0A000` | One database per cluster, and no `search_path` to switch — a relation elsewhere is named with its qualifier. |
| 11 | `CALL` | ❌ | `0A000` | No stored or table procedures. |
| 13 | `COMMENT ON` | ❌ | `0A000` | No catalog comment storage, and nowhere to read one back — accepting and discarding it would be a fake `OK`. |
| 16 | `CREATE MACRO` | ❌ | `42601` | DuckDB-only. |
| 36 | `VACUUM` | ❌ | `42601` | Per-shard storage maintenance. ⚠️ **Undecided**: either distributed vacuum management is in scope, or this belongs with `CHECKPOINT` above. `sql_command_unsupported.rs` keeps an `#[ignore]`d target-state test pending the decision. |

Rows 10, 12, 18, 23, 24 and 35 are single-node DuckDB concerns: accepting one would mean it
silently ran on one arbitrary node. They have no target-state test because they must **stay**
rejected — `sql_command_unsupported.rs::test_out_of_scope_statements_stay_rejected` is the
guard that none of them is ever quietly accepted.

## Decided limitations

### No sequences

A sequence is one monotonically increasing counter, which is the single thing a
shared-nothing cluster cannot provide cheaply. Broadcasting `CREATE SEQUENCE` gives every
shard its own counter, so the "unique id" collides across shards — and because replication
is statement shipping, a surviving `nextval()` would evaluate differently on each replica.
A coordinator-allocated counter is correct but turns every insert into a round trip through
one serialized allocator and makes the coordinator a hard point of failure for writes,
which is the opposite of what sharding buys.

So `CREATE`/`DROP SEQUENCE`, `nextval()` and `SERIAL`/`BIGSERIAL`/`SMALLSERIAL` are refused
with a message naming the alternative: generate ids in the application (UUID/ULID, or a
client-side snowflake), which needs no coordination and spreads evenly over the shards.
This also settles the `SERIAL` row of `gap-analysis-data-type.md`.

### No user-defined types

A user-defined type is cluster-wide state the coordinator has nowhere to keep. The catalog
models tables and has no replay path for non-table DDL, so a node that joins or is rebuilt
would come back without the type and refuse every write using it. It would not survive the
wire boundary either: the emulated `pg_catalog` has no `pg_type` row to give it an OID. And
what an enum or domain buys is a value check, which is shard-local — a `CHECK` constraint
declared at `CREATE TABLE` is the in-database version of the same guarantee.

So `CREATE TYPE` (enum, composite, range), `CREATE DOMAIN` and `ALTER TYPE` are refused
with the way out. `DROP TYPE` is left alone: it reports that the type does not exist, which
is exactly true. DuckDB's **inline** `ENUM(...)` column type is not a named type and still
reaches the shards — see `gap-analysis-data-type.md`.

## Open gaps, ranked

By how often real PG clients and ORMs need them. **(routing)** = parses today,
**(parser + routing)** = the parser has to be taught the statement first.

1. **`SET` / `SHOW`** *(routing)* and **`RESET`** *(parser + routing)* — the one gap that
   can break a client on connect rather than on a query.
2. **`EXPLAIN` / `DESCRIBE`** *(routing)* — query inspection and schema exploration, both
   widely used by tooling.
3. **The `COPY` streaming sub-protocol** (`FROM STDIN` / `TO STDOUT`, hence `psql`'s
   `\copy`) — protocol work rather than routing.
4. **`ALTER SCHEMA`** *(parser + routing)*, plus the two things a namespace alone does not
   give: `search_path` (needs item 1) and `ALTER TABLE … SET SCHEMA`.
5. **`PIVOT` / `UNPIVOT`** *(parser + routing)* — reshaping, DuckDB-only syntax.

Beyond the statement list, the standing limitation is **cross-shard atomicity**: a
transaction block spanning node sets is refused rather than half-applied, and multi-shard
writes report partial commits honestly. Closing that is 2PC, not a statement gap.

## Tests

| File (`tests/e2e/tests/`) | Rows |
|---|---|
| `sql_command_select.rs` | 1 — the read path's operator surface. |
| `sql_command_dml.rs` | 2–4, 25 — INSERT/UPDATE/DELETE, MERGE, upsert. |
| `sql_command_ddl.rs` | 5–7 — CREATE/ALTER/DROP TABLE, constraints, `TRUNCATE`. |
| `sql_command_transaction.rs` | 33 — what a buffered block honors and refuses. |
| `sql_command_unsupported.rs` | 8–32, 34–36 — every ❌ statement, plus the closed rows (14 `COPY`, 15 indexes, 17 schemas, 20 views), which now pin what stays refused. |
| `shard_key_hazards.rs`, `identifier_rewrite.rs`, `data_types_dialect_gaps.rs`, `concurrency.rs` | Write-path rules that cut across rows: which shard-key values route, how a name becomes one catalog key and one physical name, PG → DuckDB expression rewrites, concurrent DDL/DML. |

Each gap has up to two tests: a **passing** `*_currently_rejected` test pinning that
today's refusal is honest, and an `#[ignore = "gap (row N): …"]` test asserting the
PostgreSQL-correct target behavior. The ignored ones fail by construction and are the
definition of done — un-ignore one as its gap closes:

```sh
cd tests/e2e && cargo test --test sql_command_unsupported -- --ignored --test-threads=1
```

`make e2e` runs only the passing set, so the gap map never blocks CI.
